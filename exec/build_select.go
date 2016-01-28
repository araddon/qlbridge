package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

func (m *JobBuilder) VisitSelect(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {

	//u.Debugf("VisitSelect %+v", sp.Stmt)

	if len(sp.Stmt.From) == 0 {
		if sp.Stmt.SystemQry() {
			return m.VisitSelectSystemInfo(sp)
		}
		return m.VisitLiteralQuery(sp)
	}

	needsFinalProject := true
	planner := m.TaskMaker.Sequential("select")

	if len(sp.Stmt.From) == 1 {

		sp.Stmt.From[0].Source = sp.Stmt // TODO:   move to a Finalize() in query planner
		srcPlan, err := plan.NewSource(m.Ctx, sp.Stmt.From[0], true)
		if err != nil {
			return nil, rel.VisitError, err
		}

		task, status, err := m.TaskMaker.SourceVisitorMaker(srcPlan).VisitSourceSelect(srcPlan)
		if err != nil {
			return nil, status, err
		}
		if status != rel.VisitContinue {
			//u.Debugf("subselect visit final returning job.Ctx.Projection: %p", m.Ctx.Projection)
			return task, status, nil
		}
		planner.Add(task)

	} else {

		var prevTask TaskRunner
		var prevFrom *rel.SqlSource

		for i, from := range sp.Stmt.From {

			// Need to rewrite the From statement to ensure all fields necessary to support
			//  joins, wheres, etc exist but is standalone query
			from.Rewrite(sp.Stmt)
			srcPlan, err := plan.NewSource(m.Ctx, from, false)
			if err != nil {
				return nil, rel.VisitError, err
			}

			sourceMaker := m.TaskMaker.SourceVisitorMaker(srcPlan)
			sourceTask, status, err := sourceMaker.VisitSourceSelect(srcPlan)
			if err != nil {
				u.Errorf("Could not visitsubselect %v  %s", err, from)
				return nil, status, err
			}

			// now fold into previous task
			curTask := sourceTask.(TaskRunner)
			if i != 0 {
				from.Seekable = true
				curMergeTask := m.TaskMaker.Parallel("select-sources")
				curMergeTask.Add(prevTask)
				curMergeTask.Add(curTask)
				planner.Add(curMergeTask)

				// fold this source into previous
				in, err := NewJoinNaiveMerge(m.Ctx, prevTask, curTask, prevFrom, from)
				if err != nil {
					return nil, rel.VisitError, err
				}
				planner.Add(in)
			}
			prevTask = curTask
			prevFrom = from
			//u.Debugf("got task: %T", prevTask)
		}
	}

	if sp.Stmt.Where != nil {
		switch {
		case sp.Stmt.Where.Source != nil:
			// SELECT id from article WHERE id in (select article_id from comments where comment_ct > 50);
			u.Warnf("Found un-supported subquery: %#v", sp.Stmt.Where)
			return nil, rel.VisitError, ErrNotImplemented
		case sp.Stmt.Where.Expr != nil:
			whereTask, status, err := m.Visitor.VisitWhere(sp)
			if err != nil {
				return nil, status, err
			}
			planner.Add(whereTask)
		default:
			u.Warnf("Found un-supported where type: %#v", sp.Stmt.Where)
			return nil, rel.VisitError, fmt.Errorf("Unsupported Where Type")
		}
	}

	if sp.Stmt.IsAggQuery() {
		u.Debugf("Adding aggregate/group by? %#v", m.Visitor)
		//gb := NewGroupBy(m.Ctx, stmt)
		gbTask, status, err := m.Visitor.VisitGroupBy(sp)
		if err != nil {
			return nil, status, err
		}
		planner.Add(gbTask)
		needsFinalProject = false
	}

	if sp.Stmt.Having != nil {
		havingTask, status, err := m.Visitor.VisitHaving(sp)
		if err != nil {
			return nil, status, err
		}
		planner.Add(havingTask)
	}

	//u.Debugf("needs projection? %v", needsFinalProject)
	if needsFinalProject {
		// Add a Final Projection to choose the columns for results
		//projection := NewProjectionFinal(m.Ctx, stmt)
		projectionTask, status, err := m.Visitor.VisitProjection(sp)
		if err != nil {
			return nil, status, err
		}
		//u.Debugf("exec.projection: %p job.proj: %p added  %s", projection, m.Ctx.Projection, stmt.String())
		planner.Add(projectionTask)
	}

	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitWhere(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	//u.Debugf("VisitWhere %+v", sp.Stmt)
	return NewWhereFinal(m.Ctx, sp), rel.VisitContinue, nil
}

func (m *JobBuilder) VisitHaving(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitHaving %+v", sp)
	return NewHavingFilter(m.Ctx, sp.Stmt.UnAliasedColumns(), sp.Stmt.Having), rel.VisitContinue, nil
}

func (m *JobBuilder) VisitGroupBy(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitGroupBy %+v", sp.Stmt)
	return NewGroupBy(m.Ctx, sp.Stmt), rel.VisitContinue, nil
}

func (m *JobBuilder) VisitProjection(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitProjection %+v", sp.Stmt)
	return NewProjectionFinal(m.Ctx, sp.Stmt), rel.VisitContinue, nil
}

// Build Column Name to Position index for given *source* (from) used to interpret
// positional []driver.Value args, mutate the *from* itself to hold this map
func buildColIndex(colSchema schema.SchemaColumns, sp *plan.Source) error {
	if sp.From.Source == nil {
		u.Errorf("Couldnot build colindex bc no source %#v", sp)
		return nil
	}
	sp.From.BuildColIndex(colSchema.Columns())
	return nil
}

// SourceSelect is a single source select
func (m *SourceBuilder) VisitSourceSelect(sp *plan.Source) (plan.Task, rel.VisitStatus, error) {

	if sp.From.Source != nil {
		u.Debugf("VisitSubselect from.source = %q", sp.From.Source)
	} else {
		u.Debugf("VisitSubselect from=%q", sp)
	}

	planner := m.TaskMaker.Sequential("source-select")

	// All of this is plan info, ie needs JoinKey
	needsJoinKey := false
	from := sp.From

	source, err := sp.DataSource.Open(sp.From.SourceName())
	if err != nil {
		return nil, rel.VisitError, err
	}

	if sp.From.Source != nil && len(sp.From.JoinNodes()) > 0 {
		needsJoinKey = true
	}

	sourcePlan, implementsSourceBuilder := source.(plan.SourceSelectPlanner)
	// u.Debugf("source: tbl:%q  Builder?%v   %T  %#v", from.SourceName(), implementsSourceBuilder, source, source)
	// Must provider either Scanner, SourcePlanner, Seeker interfaces
	if implementsSourceBuilder {

		task, status, err := sourcePlan.VisitSourceSelect(sp)
		if err != nil {
			// PolyFill instead?
			return nil, status, err
		}

		// Source was able to do entirety of query-plan, don't need any polyfilled features
		if status == rel.VisitFinal {

			planner.Add(task.(TaskRunner))

			if needsJoinKey {
				if schemaCols, ok := sourcePlan.(schema.SchemaColumns); ok {
					//u.Debugf("schemaCols: %T  ", schemaCols)
					if err := buildColIndex(schemaCols, sp); err != nil {
						return nil, rel.VisitError, err
					}
				} else {
					u.Errorf("Didn't implement schema task: %T source: %T", task, sourcePlan)
				}

				joinKeyTask, err := NewJoinKey(sp)
				if err != nil {
					return nil, rel.VisitError, err
				}
				planner.Add(joinKeyTask)
			}
			return planner, status, nil
		}
		if task != nil {
			return task, rel.VisitContinue, nil
		}
		u.Errorf("Could not source plan for %v  %T %#v", from.SourceName(), source, source)
	}

	scanner, hasScanner := source.(schema.Scanner)
	if !hasScanner {
		u.Warnf("source %T does not implement datasource.Scanner", source)
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Scanner for %q", source, from.String())
	}

	switch {

	case needsJoinKey:
		// This is a source that is part of a join expression
		if err := buildColIndex(scanner, sp); err != nil {
			return nil, rel.VisitError, err
		}

		sourceTask, status, err := m.SourceVisitor.VisitSourceJoin(scanner)
		if err != nil {
			return nil, status, err
		}
		planner.Add(sourceTask.(plan.Task))

	default:
		// If we have table name and no Source(sub-query/join-query) then just read source
		if err := buildColIndex(scanner, sp); err != nil {
			return nil, rel.VisitError, err
		}
		sourceTask, status, err := m.SourceVisitor.VisitSource(scanner)
		if err != nil {
			return nil, status, err
		}
		planner.Add(sourceTask.(plan.Task))
	}

	if from.Source != nil && from.Source.Where != nil {
		switch {
		case from.Source.Where.Expr != nil:
			whereTask, status, err := m.SourceVisitor.VisitWhere()
			if err != nil {
				return nil, status, err
			}
			planner.Add(whereTask.(plan.Task))
		default:
			u.Warnf("Found un-supported where type: %#v", from.Source)
			return nil, rel.VisitError, fmt.Errorf("Unsupported Where clause:  %q", from)
		}
	}

	// Add a Non-Final Projection to choose the columns for results
	if !sp.Final {
		projection := NewProjectionInProcess(m.Plan.Ctx, from.Source)
		u.Debugf("source projection: %p added  %s", projection, from.Source.String())
		planner.Add(projection)
	}

	if needsJoinKey {
		joinKeyTask, err := NewJoinKey(sp)
		if err != nil {
			return nil, rel.VisitError, err
		}
		planner.Add(joinKeyTask)
	}

	return planner, rel.VisitContinue, nil
}

func (m *SourceBuilder) VisitWhere() (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitWhere %+v", m.Plan.From)
	return NewWhereFilter(m.Plan.Ctx, m.Plan.From.Source), rel.VisitContinue, nil
}

func (m *SourceBuilder) VisitSourceJoin(scanner schema.Scanner) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitSourceJoin %+v", m.Plan.From)
	if ss, ok := scanner.(schema.Scanner); ok {
		return NewSourceJoin(m.Plan, ss), rel.VisitContinue, nil
	}
	return nil, rel.VisitError, fmt.Errorf("Expected schema.Scanner for source but got %T", scanner)
}

func (m *SourceBuilder) VisitSource(scanner schema.Scanner) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitSource %+v", m.Plan.From)
	if ss, ok := scanner.(schema.Scanner); ok {
		return NewSource(m.Plan, ss), rel.VisitContinue, nil
	}
	return nil, rel.VisitError, fmt.Errorf("Expected schema.Scanner for source but got %T", scanner)
}

// queries for internal schema/variables such as:
//
//    select @@max_allowed_packets
//    select current_user()
//    select connection_id()
//    select timediff(curtime(), utc_time())
//
func (m *JobBuilder) VisitSelectSystemInfo(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {

	u.Debugf("VisitSelectSchemaInfo %+v", sp.Stmt)

	if sp.Stmt.IsSysQuery() {
		return m.VisitSysQuery(sp)
	} else if len(sp.Stmt.From) == 0 && len(sp.Stmt.Columns) == 1 && strings.ToLower(sp.Stmt.Columns[0].As) == "database" {
		// SELECT database;
		return m.VisitSelectDatabase(sp)
	}

	planner := m.TaskMaker.Sequential("select-schemainfo")
	stmt := sp.Stmt

	srcPlan, err := plan.NewSource(m.Ctx, sp.Stmt.From[0], true)
	if err != nil {
		return nil, rel.VisitError, err
	}

	sourceMaker := m.TaskMaker.SourceVisitorMaker(srcPlan)
	task, status, err := sourceMaker.VisitSourceSelect(srcPlan)
	if status == rel.VisitFinal {
		return task, status, nil
	}
	planner.Add(task.(TaskRunner))

	if stmt.Where != nil {
		switch {
		case stmt.Where.Source != nil:
			u.Warnf("Found un-supported subquery: %#v", stmt.Where)
			return nil, rel.VisitError, fmt.Errorf("Unsupported Where Type")
		case stmt.Where.Expr != nil:
			//u.Debugf("adding where: %q", stmt.Where.Expr)
			where := NewWhereFinal(m.Ctx, sp)
			planner.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", stmt.Where)
			return nil, rel.VisitError, fmt.Errorf("Unsupported Where Type")
		}

	}

	// Add a Projection to choose the columns for results
	projection := NewProjectionInProcess(m.Ctx, stmt)
	//u.Infof("adding projection: %#v", projection)
	planner.Add(projection)
	// NewSequential(m.Ctx, "select-schemainfo", tasks)
	return planner, rel.VisitContinue, nil
}

// Handle Literal queries such as "SELECT 1, @var;"
func (m *JobBuilder) VisitLiteralQuery(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	//u.Debugf("VisitSelectDatabase %+v", sp.Stmt)
	tasks := m.TaskMaker.Sequential("select-literal")
	vals := make([]driver.Value, len(sp.Stmt.Columns))
	for i, col := range sp.Stmt.Columns {

		vv, ok := vm.Eval(nil, col.Expr)
		u.Debugf("%d col %v ok?%v  val= %#v", i, col, ok, vv)
		if ok {
			vals[i] = vv.Value()
		}
	}

	static, p := StaticResults(vals)
	sourcePlan, err := plan.NewSource(m.Ctx, nil, true)
	if err != nil {
		return nil, rel.VisitError, err
	}
	sourceTask := NewSource(sourcePlan, static)
	m.Ctx.Projection = plan.NewProjectionStatic(p)
	tasks.Add(sourceTask)
	return tasks, rel.VisitFinal, nil
}

func (m *JobBuilder) VisitSelectDatabase(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	//u.Debugf("VisitSelectDatabase %+v", sp.Stmt)

	tasks := m.TaskMaker.Sequential("select-databases")
	val := m.Ctx.Schema.Name
	static := membtree.NewStaticDataValue(val, "database")
	sourcePlan := plan.NewSourceStaticPlan(m.Ctx)
	sourceTask := NewSource(sourcePlan, static)
	tasks.Add(sourceTask)
	m.Ctx.Projection = StaticProjection("database", value.StringType)
	return tasks, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitSysQuery(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {

	u.Debugf("VisitSysQuery %+v", sp.Stmt)

	stmt := sp.Stmt
	static := membtree.NewStaticData("schema")

	//u.Debugf("Ctx.Projection: %#v", m.Ctx.Projection)
	//u.Debugf("Ctx.Projection.Proj: %#v", m.Ctx.Projection.Proj)
	p := rel.NewProjection()
	cols := make([]string, len(stmt.Columns))
	row := make([]driver.Value, len(cols))
	for i, col := range stmt.Columns {
		if col.Expr == nil {
			return nil, rel.VisitError, fmt.Errorf("no column info? %#v", col.Expr)
		}
		switch n := col.Expr.(type) {
		case *expr.IdentityNode:
			coln := strings.ToLower(n.Text)
			cols[i] = col.As
			if strings.HasPrefix(coln, "@@") {
				//u.Debugf("m.Ctx? %#v", m.Ctx)
				//u.Debugf("m.Ctx.Session? %#v", m.Ctx.Session)
				val, ok := m.Ctx.Session.Get(coln)
				//u.Debugf("got session var? %v=%#v", col.As, val)
				if ok {
					p.AddColumnShort(col.As, val.Type())
					row[i] = val.Value()
				} else {
					p.AddColumnShort(col.As, value.NilType)
				}
			} else {
				u.Infof("columns?  as=%q    rel=%q", col.As, coln)
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
	//u.Debugf("%p=plan.projection  rel.Projection=%p", m.Projection, p)
	tasks := m.TaskMaker.Sequential("select-@@sysvars")
	sourcePlan := plan.NewSourceStaticPlan(m.Ctx)
	sourceTask := NewSource(sourcePlan, static)
	tasks.Add(sourceTask)
	return tasks, rel.VisitContinue, nil
}

// A very simple tasks/builder for system variables
//
func (m *JobBuilder) sysVarTasks(name string, val interface{}) (rel.Task, rel.VisitStatus, error) {
	tasks := m.TaskMaker.Sequential("select-@@sysvars")
	static := membtree.NewStaticDataValue(name, val)
	sourcePlan := plan.NewSourceStaticPlan(m.Ctx)
	sourceTask := NewSource(sourcePlan, static)
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
		return nil, rel.VisitError, fmt.Errorf("Unrecognized Data Type: %v", val)
	}
	return tasks, rel.VisitContinue, nil
}

// A very simple projection of name=value, for single row/column
//   select @@max_bytes
//
func StaticProjection(name string, vt value.ValueType) *plan.Projection {
	p := rel.NewProjection()
	p.AddColumnShort(name, vt)
	return plan.NewProjectionStatic(p)
}

func StaticResults(vals []driver.Value) (*membtree.StaticDataSource, *rel.Projection) {
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
	p := rel.NewProjection()
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
