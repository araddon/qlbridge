package plan

import (
	"github.com/araddon/qlbridge/rel"
)

// Visitor defines the Visit Pattern, so our rel package can
//   expect implementations from downstream packages
//   in our case: planner(s), job builder, execution engine
//
type Visitor interface {
	VisitPreparedStmt(stmt *rel.PreparedStatement) (rel.Task, rel.VisitStatus, error)
	VisitSelect(stmt *rel.SqlSelect) (rel.Task, rel.VisitStatus, error)
	VisitInsert(stmt *rel.SqlInsert) (rel.Task, rel.VisitStatus, error)
	VisitUpsert(stmt *rel.SqlUpsert) (rel.Task, rel.VisitStatus, error)
	VisitUpdate(stmt *rel.SqlUpdate) (rel.Task, rel.VisitStatus, error)
	VisitDelete(stmt *rel.SqlDelete) (rel.Task, rel.VisitStatus, error)
	VisitShow(stmt *rel.SqlShow) (rel.Task, rel.VisitStatus, error)
	VisitDescribe(stmt *rel.SqlDescribe) (rel.Task, rel.VisitStatus, error)
	VisitCommand(stmt *rel.SqlCommand) (rel.Task, rel.VisitStatus, error)
	SourceVisitor
}

// Interface for sub-select Tasks of the Select Statement, sub-selects
type SourceVisitor interface {
	VisitSourceSelect(plan *SourcePlan) (rel.Task, rel.VisitStatus, error)
}
