package exec

import (
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
	Conf       *datasource.RuntimeSchema
	Projection *expr.Projection
	connInfo   string
	where      expr.Node
	distinct   bool
	children   Tasks
}

// JobBuilder
//   @conf   = the config/runtime schema info
//   @connInfo = connection string info for original connection
//
func NewJobBuilder(conf *datasource.RuntimeSchema, connInfo string) *JobBuilder {
	b := JobBuilder{}
	b.Conf = conf
	b.connInfo = connInfo
	return &b
}

func (m *JobBuilder) VisitPreparedStmt(stmt *expr.PreparedStatement) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, expr.VisitFinal, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitCommand(stmt *expr.SqlCommand) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitCommand %+v", stmt)
	return nil, expr.VisitFinal, expr.ErrNotImplemented
}
