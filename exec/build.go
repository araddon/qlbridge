package exec

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the expr.Visitor interface
	_ expr.Visitor       = (*JobBuilder)(nil)
	_ plan.SourceVisitor = (*JobBuilder)(nil)
)

// This is a simple, single source Job Executor
//   hopefully we create smarter ones but this is a basic implementation for
///  running in-process, not distributed
type JobBuilder struct {
	Conf       *datasource.RuntimeSchema
	Projection *plan.Projection
	Ctx        *expr.Context
	Schema     *datasource.Schema
	connInfo   string
	where      expr.Node
	distinct   bool
	children   Tasks
}

// JobBuilder
//   @conf   = the config/runtime schema info
//   @connInfo = connection string info for original connection
//
func NewJobBuilder(conf *datasource.RuntimeSchema, reqCtx *expr.Context) *JobBuilder {
	b := JobBuilder{}
	b.Conf = conf
	b.Ctx = reqCtx
	b.connInfo = reqCtx.ConnInfo
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
