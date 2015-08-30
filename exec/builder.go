package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the sql expr.Visitor interface
	_ expr.Visitor    = (*JobBuilder)(nil)
	_ expr.SubVisitor = (*JobBuilder)(nil)
)

// This is a simple, single source Job Executor
//   we can create smarter ones but this is a basic implementation for
///  running in-process, not distributed
type JobBuilder struct {
	schema   *datasource.RuntimeSchema
	connInfo string
	where    expr.Node
	distinct bool
	children Tasks
}

// JobBuilder
//   @connInfo = connection string info for original connection
//
func NewJobBuilder(rtConf *datasource.RuntimeSchema, connInfo string) *JobBuilder {
	b := JobBuilder{}
	b.schema = rtConf
	b.connInfo = connInfo
	return &b
}

func (m *JobBuilder) VisitPreparedStmt(stmt *expr.PreparedStatement) (expr.Task, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitSelect(stmt *expr.SqlSelect) (expr.Task, error) {
	u.Debugf("VisitSelect %+v", stmt)

	tasks := make(Tasks, 0)
	/*
		General plan to improve/reimplement this

		- Fold:   n number of sources would fold
		- Some datasources can plan for themselves in which case we don't need to poly fill
	*/

	if len(stmt.From) == 1 {
		// One From Source   This entire Source needs to be moved into
		//  a From().Accept(m) or m.visitSubselect()
		from := stmt.From[0]
		if from.Name != "" && from.Source == nil {
			//u.Infof("get SourceConn: %v", from.Name)
			sourceConn := m.schema.Conn(from.Name)
			//u.Debugf("sourceConn: %T  %#v", sourceConn, sourceConn)
			// Must provider either Scanner, and or Seeker interfaces
			if scanner, ok := sourceConn.(datasource.Scanner); !ok {
				return nil, fmt.Errorf("Must Implement Scanner")
			} else {
				in := NewSource(from, scanner)
				tasks.Add(in)
			}
		}
	} else {
		// for now, only support 1 join
		if len(stmt.From) != 2 {
			return nil, fmt.Errorf("3 or more Table/Join not currently implemented")
		}
		// Fold n <- n+1
		stmt.From[0].Rewrite(true, stmt)
		stmt.From[1].Rewrite(false, stmt)
		in, err := NewSourceJoin(m, stmt.From[0], stmt.From[1], m.schema)
		if err != nil {
			return nil, err
		}
		tasks.Add(in)
	}

	//u.Debugf("has where? %v", stmt.Where != nil)
	if stmt.Where != nil {
		switch {
		case stmt.Where.Source != nil:
			u.Warnf("Found un-supported subquery: %#v", stmt.Where)
			return nil, fmt.Errorf("Unsupported Where Type")
		case stmt.Where.Expr != nil:
			where := NewWhere(stmt.Where.Expr, stmt)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", stmt.Where)
			return nil, fmt.Errorf("Unsupported Where Type")
		}

	}

	// Add a Projection
	projection := NewProjection(stmt)
	//u.Infof("adding projection: %#v", projection)
	tasks.Add(projection)

	return NewSequential("select", tasks), nil
}

func (m *JobBuilder) VisitSubselect(stmt *expr.SqlSource) (expr.Task, error) {
	u.Debugf("VisitSubselect %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitJoin(stmt *expr.SqlSource) (expr.Task, error) {
	u.Debugf("VisitJoin %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitInsert(stmt *expr.SqlInsert) (expr.Task, error) {

	u.Debugf("VisitInsert %+v", stmt)
	tasks := make(Tasks, 0)

	//u.Infof("get SourceConn: %v", stmt.Table)
	dataSource := m.schema.Conn(stmt.Table)
	if dataSource == nil {
		return nil, fmt.Errorf("No table '%s' found", stmt.Table)
	}
	//u.Debugf("sourceConn: %T  %#v", dataSource, dataSource)
	// Must provider either Scanner, and or Seeker interfaces
	source, ok := dataSource.(datasource.Upsert)
	if !ok {
		return nil, fmt.Errorf("%T Must Implement Upsert", dataSource)
	}

	insertTask := NewInsertUpsert(stmt, source)
	//u.Infof("adding insert: %#v", insertTask)
	tasks.Add(insertTask)

	return NewSequential("insert", tasks), nil
}

func (m *JobBuilder) VisitUpdate(stmt *expr.SqlUpdate) (expr.Task, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	tasks := make(Tasks, 0)

	//u.Infof("get SourceConn: %v", stmt.Table)
	dataSource := m.schema.Conn(stmt.Table)
	if dataSource == nil {
		return nil, fmt.Errorf("No table '%s' found", stmt.Table)
	}
	//u.Debugf("sourceConn: %T  %#v", dataSource, dataSource)
	// Must provider either Scanner, and or Seeker interfaces
	source, ok := dataSource.(datasource.Upsert)
	if !ok {
		return nil, fmt.Errorf("%T Must Implement Upsert", dataSource)
	}

	updateTask := NewUpdateUpsert(stmt, source)
	//u.Infof("adding update: %#v", updateTask)
	tasks.Add(updateTask)

	return NewSequential("update", tasks), nil
}

func (m *JobBuilder) VisitUpsert(stmt *expr.SqlUpsert) (expr.Task, error) {

	u.Debugf("VisitUpsert %+v", stmt)
	tasks := make(Tasks, 0)

	//u.Infof("get SourceConn: %v", stmt.Table)
	dataSource := m.schema.Conn(stmt.Table)
	if dataSource == nil {
		return nil, fmt.Errorf("No table '%s' found", stmt.Table)
	}
	//u.Debugf("sourceConn: %T  %#v", dataSource, dataSource)
	// Must provider either Scanner, and or Seeker interfaces
	source, ok := dataSource.(datasource.Upsert)
	if !ok {
		return nil, fmt.Errorf("%T Must Implement Upsert", dataSource)
	}

	upsertTask := NewUpsertUpsert(stmt, source)
	//u.Infof("adding upsert: %#v", upsertTask)
	tasks.Add(upsertTask)

	return NewSequential("upsert", tasks), nil
}

func (m *JobBuilder) VisitDelete(stmt *expr.SqlDelete) (expr.Task, error) {
	u.Debugf("VisitDelete %+v", stmt)
	tasks := make(Tasks, 0)

	//u.Infof("get SourceConn: %q", stmt.Table)
	dataSource := m.schema.Conn(stmt.Table)
	if dataSource == nil {
		return nil, fmt.Errorf("No table '%s' found", stmt.Table)
	}
	//u.Debugf("sourceConn: %T  %#v", dataSource, dataSource)
	// Must provider either Scanner, and or Seeker interfaces
	source, ok := dataSource.(datasource.Deletion)
	if !ok {
		// If this datasource doesn't implement delete, do a scan + delete?
		return nil, fmt.Errorf("%T Must Implement Delete", dataSource)
	}

	deleteTask := NewDelete(stmt, source)
	//u.Infof("adding delete task: %#v", deleteTask)
	tasks.Add(deleteTask)

	return NewSequential("delete", tasks), nil
}

func (m *JobBuilder) VisitShow(stmt *expr.SqlShow) (expr.Task, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitDescribe(stmt *expr.SqlDescribe) (expr.Task, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitCommand(stmt *expr.SqlCommand) (expr.Task, error) {
	u.Debugf("VisitCommand %+v", stmt)
	return nil, expr.ErrNotImplemented
}
