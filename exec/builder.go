package exec

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Exec Visitor interface
	_ Visitor = (*JobBuilder)(nil)
)

// This is a simple, single source Job Executor
//   we can create smarter ones but this is a basic implementation
type JobBuilder struct {
	conf     *RuntimeConfig
	connInfo string
	where    expr.Node
	distinct bool
	children Tasks
}

func NewJobBuilder(rtConf *RuntimeConfig, connInfo string) *JobBuilder {
	b := JobBuilder{}
	b.conf = rtConf
	b.connInfo = connInfo
	return &b
}

func (m *JobBuilder) VisitSelect(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSelect %+v", stmt)

	tasks := make(Tasks, 0)

	// Create our Datasource Reader
	var source datasource.DataSource
	if len(stmt.From) == 1 {
		from := stmt.From[0]
		if from.Name != "" && from.Source == nil {
			source = m.conf.DataSource(m.connInfo, from.Name)
			//u.Debugf("source: %T", source)
			in := NewSourceScanner(from.Name, source)
			tasks.Add(in)
		}

	} else {
		// if we have a join?
	}

	u.Debugf("has where? %v", stmt.Where != nil)
	if stmt.Where != nil {
		switch {
		case stmt.Where.Source != nil:
			u.Warnf("Found un-supported subquery: %#v", stmt.Where)
		case stmt.Where.Expr != nil:
			where := NewWhere(stmt.Where.Expr)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", stmt.Where)
		}

	}

	// Add a Projection
	projection := NewProjection(stmt)
	tasks.Add(projection)

	return tasks, nil
}

func (m *JobBuilder) VisitInsert(stmt *expr.SqlInsert) (interface{}, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *JobBuilder) VisitDelete(stmt *expr.SqlDelete) (interface{}, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *JobBuilder) VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *JobBuilder) VisitShow(stmt *expr.SqlShow) (interface{}, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *JobBuilder) VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, ErrNotImplemented
}
func (m *JobBuilder) VisitPreparedStmt(stmt *expr.PreparedStatement) (interface{}, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, ErrNotImplemented
}
