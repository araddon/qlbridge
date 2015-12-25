package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

const (
	MaxAllowedPacket = 1024 * 1024
)

func (m *JobBuilder) VisitSelect(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error) {

	u.Debugf("VisitSelect %+v", stmt)

	tasks := make(Tasks, 0)

	if len(stmt.From) == 0 {
		//u.Warnf("no from? isSystem?%v", stmt.SystemQry())
		if stmt.SystemQry() {
			return m.VisitSelectSystemInfo(stmt)
		}
		return m.VisitLiteralQuery(stmt)

		return nil, expr.VisitError, fmt.Errorf("No From for %v", stmt.String())

	} else if len(stmt.From) == 1 {

		stmt.From[0].Source = stmt
		srcPlan, err := plan.NewSourcePlan(m.Ctx, stmt.From[0], true)
		if err != nil {
			return nil, expr.VisitError, err
		}
		task, status, err := m.VisitSourceSelect(srcPlan)
		if err != nil {
			return nil, status, err
		}
		if status == expr.VisitFinal {
			//u.Debugf("subselect visit final returning job.Ctx.Projection: %p", m.Ctx.Projection)
			return task, status, nil
		}
		tasks.Add(task.(TaskRunner))

		// Add a Final Projection to choose the columns for results
		projection := NewProjectionFinal(m.Ctx, stmt)
		//u.Infof("adding projection: %#v", projection)
		tasks.Add(projection)

		return NewSequential(m.Ctx, "select", tasks), expr.VisitContinue, nil

	} else {

		var prevTask TaskRunner
		var prevFrom *expr.SqlSource

		for i, from := range stmt.From {

			// Need to rewrite the From statement
			from.Rewrite(stmt)
			srcPlan, err := plan.NewSourcePlan(m.Ctx, from, false)
			if err != nil {
				return nil, expr.VisitError, err
			}
			sourceTask, status, err := m.VisitSourceSelect(srcPlan)
			if err != nil {
				u.Errorf("Could not visitsubselect %v  %s", err, from)
				return nil, status, err
			}

			// now fold into previous task
			curTask := sourceTask.(TaskRunner)
			if i != 0 {
				from.Seekable = true
				twoTasks := []TaskRunner{prevTask, curTask}
				curMergeTask := NewTaskParallel(m.Ctx, "select-sources", nil, twoTasks)
				tasks.Add(curMergeTask)

				// fold this source into previous
				in, err := NewJoinNaiveMerge(m.Ctx, prevTask, curTask, prevFrom, from)
				if err != nil {
					return nil, expr.VisitError, err
				}
				tasks.Add(in)
			}
			prevTask = curTask
			prevFrom = from
			//u.Debugf("got task: %T", prevTask)
		}
	}

	if stmt.Where != nil {
		switch {
		case stmt.Where.Source != nil:
			u.Warnf("Found un-supported subquery: %#v", stmt.Where)
			return nil, expr.VisitError, fmt.Errorf("Unsupported Where Type")
		case stmt.Where.Expr != nil:
			//u.Debugf("adding where: %q", stmt.Where.Expr)
			where := NewWhereFinal(m.Ctx, stmt)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", stmt.Where)
			return nil, expr.VisitError, fmt.Errorf("Unsupported Where Type")
		}

	}

	// Add a Final Projection to choose the columns for results
	projection := NewProjectionFinal(m.Ctx, stmt)
	u.Debugf("exec.projection: %p job.proj: %p added  %s", projection, m.Ctx.Projection, stmt.String())
	//m.Projection = nil
	tasks.Add(projection)

	return NewSequential(m.Ctx, "select", tasks), expr.VisitContinue, nil
}

// Build Column Name to Position index for given *source* (from) used to interpret
// positional []driver.Value args, mutate the *from* itself to hold this map
func buildColIndex(colSchema schema.SchemaColumns, sp *plan.SourcePlan) error {
	if sp.Source == nil {
		u.Errorf("Couldnot build colindex bc no source %#v", sp)
		return nil
	}
	sp.BuildColIndex(colSchema.Columns())
	return nil
}

func (m *JobBuilder) VisitSourceSelect(sp *plan.SourcePlan) (expr.Task, expr.VisitStatus, error) {

	if sp.Source != nil {
		u.Debugf("VisitSubselect from.source = %q", sp.Source)
	} else {
		u.Debugf("VisitSubselect from=%q", sp)
	}

	tasks := make(Tasks, 0)
	needsJoinKey := false
	from := sp.SqlSource

	source, err := sp.DataSource.Open(sp.SourceName())
	if err != nil {
		return nil, expr.VisitError, err
	}
	if sp.Source != nil && len(sp.JoinNodes()) > 0 {
		needsJoinKey = true
	}

	sourcePlan, implementsSourceBuilder := source.(plan.SourceSelectPlanner)
	//u.Debugf("source: tbl:%q  Builder?%v   %T  %#v", from.SourceName(), implementsSourceBuilder, source, source)
	// Must provider either Scanner, SourcePlanner, Seeker interfaces
	if implementsSourceBuilder {

		task, status, err := sourcePlan.VisitSourceSelect(sp)
		if err != nil {
			// PolyFill instead?
			return nil, status, err
		}
		if status == expr.VisitFinal {

			//m.Projection, _ = sourcePlan.Projection()
			tasks.Add(task.(TaskRunner))

			if needsJoinKey {
				if schemaCols, ok := sourcePlan.(schema.SchemaColumns); ok {
					//u.Debugf("schemaCols: %T  ", schemaCols)
					if err := buildColIndex(schemaCols, sp); err != nil {
						return nil, expr.VisitError, err
					}
				} else {
					u.Errorf("Didn't implement schema task: %T source: %T", task, sourcePlan)
				}

				joinKeyTask, err := NewJoinKey(m.Ctx, sp.SqlSource)
				if err != nil {
					return nil, expr.VisitError, err
				}
				tasks.Add(joinKeyTask)
			}
			return NewSequential(m.Ctx, "sub-select", tasks), status, nil
			//return task, status, nil
		}
		if task != nil {
			//u.Infof("found task?")
			//tasks.Add(task)
			//return NewSequential("source-planner", tasks), nil
			return task, expr.VisitContinue, nil
		}
		u.Errorf("Could not source plan for %v  %T %#v", from.SourceName(), source, source)
	}
	scanner, hasScanner := source.(datasource.Scanner)
	if !hasScanner {
		u.Warnf("source %T does not implement datasource.Scanner", source)
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Scanner for %q", source, from.String())
	}

	switch {

	case needsJoinKey: //from.Source != nil && len(from.JoinNodes()) > 0:
		// This is a source that is part of a join expression
		if err := buildColIndex(scanner, sp); err != nil {
			return nil, expr.VisitError, err
		}
		sourceTask := NewSourceJoin(m.Ctx, from, scanner)
		tasks.Add(sourceTask)

	default:
		// If we have table name and no Source(sub-query/join-query) then just read source
		if err := buildColIndex(scanner, sp); err != nil {
			return nil, expr.VisitError, err
		}
		sourceTask := NewSource(m.Ctx, sp.SqlSource, scanner)
		tasks.Add(sourceTask)

	}

	if from.Source != nil && from.Source.Where != nil {
		switch {
		case from.Source.Where.Expr != nil:
			//u.Debugf("adding where: %q", from.Source.Where.Expr)
			where := NewWhereFilter(m.Ctx, from.Source)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", from.Source)
			return nil, expr.VisitError, fmt.Errorf("Unsupported Where clause:  %q", from)
		}
	}

	// Add a Non-Final Projection to choose the columns for results
	if !sp.Final {
		projection := NewProjectionInProcess(m.Ctx, from.Source)
		u.Debugf("source projection: %p added  %s", projection, from.Source.String())
		tasks.Add(projection)
	}

	if needsJoinKey {
		joinKeyTask, err := NewJoinKey(m.Ctx, from)
		if err != nil {
			return nil, expr.VisitError, err
		}
		tasks.Add(joinKeyTask)
	}

	// TODO: projection, groupby, having
	// Plan?   Parallel?  hash?
	return NewSequential(m.Ctx, "sub-select", tasks), expr.VisitContinue, nil
}

// queries for internal schema/variables such as:
//
//    select @@max_allowed_packets
//    select current_user()
//    select connection_id()
//    select timediff(curtime(), utc_time())
//
func (m *JobBuilder) VisitSelectSystemInfo(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error) {

	u.Debugf("VisitSelectSchemaInfo %+v", stmt)

	if stmt.IsSysQuery() {
		return m.VisitSysQuery(stmt)
	} else if len(stmt.From) == 0 && len(stmt.Columns) == 1 && strings.ToLower(stmt.Columns[0].As) == "database" {
		// SELECT database;
		return m.VisitSelectDatabase(stmt)
	}

	tasks := make(Tasks, 0)

	srcPlan, err := plan.NewSourcePlan(m.Ctx, stmt.From[0], true)
	if err != nil {
		return nil, expr.VisitError, err
	}
	task, status, err := m.VisitSourceSelect(srcPlan)
	if err != nil {
		return nil, expr.VisitError, err
	}
	if status == expr.VisitFinal {
		return task, status, nil
	}
	tasks.Add(task.(TaskRunner))

	if stmt.Where != nil {
		switch {
		case stmt.Where.Source != nil:
			u.Warnf("Found un-supported subquery: %#v", stmt.Where)
			return nil, expr.VisitError, fmt.Errorf("Unsupported Where Type")
		case stmt.Where.Expr != nil:
			//u.Debugf("adding where: %q", stmt.Where.Expr)
			where := NewWhereFinal(m.Ctx, stmt)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", stmt.Where)
			return nil, expr.VisitError, fmt.Errorf("Unsupported Where Type")
		}

	}

	// Add a Projection to choose the columns for results
	projection := NewProjectionInProcess(m.Ctx, stmt)
	//u.Infof("adding projection: %#v", projection)
	tasks.Add(projection)

	return NewSequential(m.Ctx, "select-schemainfo", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitLiteralQuery(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error) {
	//u.Debugf("VisitSelectDatabase %+v", stmt)
	tasks := make(Tasks, 0)
	vals := make([]driver.Value, len(stmt.Columns))
	for i, col := range stmt.Columns {

		vv, ok := vm.Eval(nil, col.Expr)
		u.Debugf("%d col %v ok?%v  val= %#v", i, col, ok, vv)
		if ok {
			vals[i] = vv.Value()
		}
	}

	static, p := StaticResults(vals)
	sourceTask := NewSource(m.Ctx, nil, static)
	m.Ctx.Projection = plan.NewProjectionStatic(p)
	tasks.Add(sourceTask)
	return NewSequential(m.Ctx, "literal-query", tasks), expr.VisitFinal, nil
}

func (m *JobBuilder) VisitSelectDatabase(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error) {
	//u.Debugf("VisitSelectDatabase %+v", stmt)

	tasks := make(Tasks, 0)
	val := m.Ctx.Schema.Name
	static := membtree.NewStaticDataValue(val, "database")
	sourceTask := NewSource(m.Ctx, nil, static)
	tasks.Add(sourceTask)
	m.Ctx.Projection = StaticProjection("database", value.StringType)
	return NewSequential(m.Ctx, "database", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitSysQuery(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error) {

	u.Debugf("VisitSysQuery %+v", stmt)
	static := membtree.NewStaticData("schema")

	u.Debugf("Ctx.Projection: %#v", m.Ctx.Projection)
	//u.Debugf("Ctx.Projection.Proj: %#v", m.Ctx.Projection.Proj)
	p := expr.NewProjection()
	cols := make([]string, len(stmt.Columns))
	row := make([]driver.Value, len(cols))
	for i, col := range stmt.Columns {
		if col.Expr == nil {
			return nil, expr.VisitError, fmt.Errorf("no column info? %#v", col.Expr)
		}
		switch n := col.Expr.(type) {
		case *expr.IdentityNode:
			coln := strings.ToLower(n.Text)
			cols[i] = col.As
			if strings.HasPrefix(coln, "@@") {
				//u.Debugf("m.Ctx? %#v", m.Ctx)
				//u.Debugf("m.Ctx.Session? %#v", m.Ctx.Session)
				val, ok := m.Ctx.Session.Get(coln)
				u.Debugf("got session var? %v=%#v", col.As, val)
				if ok {
					p.AddColumnShort(col.As, val.Type())
					row[i] = val.Value()
				} else {
					p.AddColumnShort(col.As, value.NilType)
				}
			} else {
				u.Infof("columns?  as=%q    expr=%q", col.As, coln)
			}
			// SELECT current_user
		case *expr.FuncNode:
			// SELECT current_user()
			// n.String()
		}
	}
	static.SetColumns(cols)
	_, err := static.Put(nil, nil, row)
	if err != nil {
		u.Errorf("Could not put %v", err)
	}

	m.Ctx.Projection = plan.NewProjectionStatic(p)
	//u.Debugf("%p=plan.projection  expr.Projection=%p", m.Projection, p)
	tasks := make(Tasks, 0)
	sourceTask := NewSource(m.Ctx, nil, static)
	tasks.Add(sourceTask)
	return NewSequential(m.Ctx, "sys-var", tasks), expr.VisitContinue, nil
	//u.Errorf("unknown var: %v", sysVar)
	//return nil, expr.VisitError, fmt.Errorf("Unrecognized System Variable: ")

	col1 := "fake"
	switch sysVar := strings.ToLower(col1); sysVar {
	case "@@max_allowed_packet":
		//u.Infof("max allowed")
		m.Ctx.Projection = StaticProjection("@@max_allowed_packet", value.IntType)
		return m.sysVarTasks(sysVar, MaxAllowedPacket)
	case "current_user()", "current_user":
		return m.sysVarTasks(sysVar, "user")
	case "connection_id()":
		return m.sysVarTasks(sysVar, 1)
	case "timediff(curtime(), utc_time())":
		return m.sysVarTasks("timediff", "00:00:00.000000")
		//
	default:
		u.Errorf("unknown var: %v", sysVar)
		return nil, expr.VisitError, fmt.Errorf("Unrecognized System Variable: %v", sysVar)
	}
}

// A very simple tasks/builder for system variables
//
func (m *JobBuilder) sysVarTasks(name string, val interface{}) (expr.Task, expr.VisitStatus, error) {
	tasks := make(Tasks, 0)
	static := membtree.NewStaticDataValue(name, val)
	sourceTask := NewSource(m.Ctx, nil, static)
	tasks.Add(sourceTask)
	switch val.(type) {
	case int, int64:
		m.Ctx.Projection = StaticProjection(name, value.IntType)
	case string:
		m.Ctx.Projection = StaticProjection(name, value.StringType)
	case float32, float64:
		m.Ctx.Projection = StaticProjection(name, value.NumberType)
	case bool:
		m.Ctx.Projection = StaticProjection(name, value.BoolType)
	default:
		u.Errorf("unknown var: %v", val)
		return nil, expr.VisitError, fmt.Errorf("Unrecognized Data Type: %v", val)
	}
	return NewSequential(m.Ctx, "sys-var", tasks), expr.VisitContinue, nil
}

// A very simple projection of name=value, for single row/column
//   select @@max_bytes
//
func StaticProjection(name string, vt value.ValueType) *plan.Projection {
	p := expr.NewProjection()
	p.AddColumnShort(name, vt)
	return plan.NewProjectionStatic(p)
}

func StaticResults(vals []driver.Value) (*membtree.StaticDataSource, *expr.Projection) {
	/*
		mysql> select 1;
		+---+
		| 1 |
		+---+
		| 1 |
		+---+
		1 row in set (0.00 sec)
	*/
	rows := make([][]driver.Value, 1)
	headers := make([]string, len(vals))
	for i, val := range vals {
		headers[i] = fmt.Sprintf("%v", val)
		//vals[i] = driver.Value(headers[i])
	}
	rows[0] = vals

	dataSource := membtree.NewStaticDataSource("literal_vals", 0, rows, headers)
	p := expr.NewProjection()
	for i, val := range vals {
		switch val.(type) {
		case int, int64, int32:
			u.Debugf("add %s val %v", headers[i], val)
			p.AddColumnShort(headers[i], value.IntType)
		case string:
			u.Debugf("add %s val %v", headers[i], val)
			p.AddColumnShort(headers[i], value.StringType)
		}
	}

	//p.AddColumnShort("Variable_name", value.StringType)

	return dataSource, p
}
