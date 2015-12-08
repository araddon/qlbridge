package plan

import (
	"github.com/araddon/qlbridge/expr"
)

// Task is the interface for execution/plan
type Task interface {
	Run(ctx *expr.Context) error
	Close() error
}

// Visitor defines the Visit Pattern, so our expr package can
//   expect implementations from downstream packages
//   in our case: planner(s), job builder, execution engine
//
type Visitor interface {
	VisitPreparedStmt(stmt *expr.PreparedStatement) (expr.Task, expr.VisitStatus, error)
	VisitSelect(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error)
	VisitInsert(stmt *expr.SqlInsert) (expr.Task, expr.VisitStatus, error)
	VisitUpsert(stmt *expr.SqlUpsert) (expr.Task, expr.VisitStatus, error)
	VisitUpdate(stmt *expr.SqlUpdate) (expr.Task, expr.VisitStatus, error)
	VisitDelete(stmt *expr.SqlDelete) (expr.Task, expr.VisitStatus, error)
	VisitShow(stmt *expr.SqlShow) (expr.Task, expr.VisitStatus, error)
	VisitDescribe(stmt *expr.SqlDescribe) (expr.Task, expr.VisitStatus, error)
	VisitCommand(stmt *expr.SqlCommand) (expr.Task, expr.VisitStatus, error)
}

// Interface for sub-select Tasks of the Select Statement, sub-selects
type SourceVisitor interface {
	VisitSourceSelect(plan *SourcePlan) (expr.Task, expr.VisitStatus, error)
}
