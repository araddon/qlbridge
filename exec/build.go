package exec

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the expr.Visitor interface
	_ plan.Visitor       = (*JobBuilder)(nil)
	_ plan.SourceVisitor = (*JobBuilder)(nil)
)

// This is a simple, single source Job Builder
//   hopefully we create smarter ones but this is a basic implementation for
///  running in-process, not distributed
type JobBuilder struct {
	rel.Visitor
	Ctx      *plan.Context
	planner  plan.ExecutionPlanner
	where    expr.Node
	distinct bool
	children []plan.Task
}

func NewJobBuilder(reqCtx *plan.Context) *JobBuilder {
	b := JobBuilder{}
	b.Ctx = reqCtx
	b.planner = TaskRunnersMaker
	return &b
}

func (m *JobBuilder) Wrap(visitor rel.Visitor) rel.Visitor {
	u.Debugf("wrap %T", visitor)
	m.Visitor = visitor
	return m
}
func (m *JobBuilder) VisitPreparedStmt(stmt *rel.PreparedStatement) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, rel.VisitFinal, expr.ErrNotImplemented
}

func (m *JobBuilder) VisitCommand(stmt *rel.SqlCommand) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitCommand %+v", stmt)
	return nil, rel.VisitFinal, expr.ErrNotImplemented
}
