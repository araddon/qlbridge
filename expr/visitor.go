package expr

import (
	u "github.com/araddon/gou"
)

var _ = u.EMPTY

// VisitStatus surfaces status to visit builders
// if visit was completed, successful or needs to be polyfilled
type VisitStatus int

const (
	VisitUnknown  VisitStatus = 0 // not used
	VisitError    VisitStatus = 1 // error
	VisitFinal    VisitStatus = 2 // final, no more building needed
	VisitContinue VisitStatus = 3 // continue visit
)

// Task is the interface for execution/plan
type Task interface {
	Run() error
	Close() error
}

// Visitor defines the Visit Pattern, so our expr package can
//   expect implementations from downstream packages
//   in our case: planner(s), job builder, execution engine
//
type Visitor interface {
	Wrap(Visitor) Visitor
	VisitPreparedStmt(stmt *PreparedStatement) (Task, VisitStatus, error)
	VisitSelect(stmt *SqlSelect) (Task, VisitStatus, error)
	VisitInsert(stmt *SqlInsert) (Task, VisitStatus, error)
	VisitUpsert(stmt *SqlUpsert) (Task, VisitStatus, error)
	VisitUpdate(stmt *SqlUpdate) (Task, VisitStatus, error)
	VisitDelete(stmt *SqlDelete) (Task, VisitStatus, error)
	VisitShow(stmt *SqlShow) (Task, VisitStatus, error)
	VisitDescribe(stmt *SqlDescribe) (Task, VisitStatus, error)
	VisitCommand(stmt *SqlCommand) (Task, VisitStatus, error)
	VisitWhere(stmt *SqlWhere) (Task, VisitStatus, error)
	VisitInto(stmt *SqlInto) (Task, VisitStatus, error)
}

// Interface for sub-select Tasks of the Select Statement
type SourceVisitor interface {
	VisitSourceSelect(stmt *SqlSource) (Task, VisitStatus, error)
}
