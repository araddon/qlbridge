package expr

import (
	u "github.com/araddon/gou"
	"golang.org/x/net/context"
)

// Context for Plan/Execution
type Context struct {
	context.Context
	DisableRecover bool
	Errors         []error
	errRecover     interface{}
	id             string
	prefix         string
}

func (m *Context) Recover() {
	if m.DisableRecover {
		return
	}
	if r := recover(); r != nil {
		u.Errorf("context recover: %v", r)
		m.errRecover = r
	}
}

func NewContext() *Context {
	return &Context{}
}

// Task is the interface for execution/plan
type Task interface {
	Run(ctx *Context) error
	Close() error
}

// Visitor defines the Visit Pattern, so our expr package can
//   expect implementations from downstream packages
//   in our case: planner(s), job builder, execution engine
//
type Visitor interface {
	VisitPreparedStmt(stmt *PreparedStatement) (Task, error)
	VisitSelect(stmt *SqlSelect) (Task, error)
	VisitInsert(stmt *SqlInsert) (Task, error)
	VisitUpsert(stmt *SqlUpsert) (Task, error)
	VisitUpdate(stmt *SqlUpdate) (Task, error)
	VisitDelete(stmt *SqlDelete) (Task, error)
	VisitShow(stmt *SqlShow) (Task, error)
	VisitDescribe(stmt *SqlDescribe) (Task, error)
	VisitCommand(stmt *SqlCommand) (Task, error)
}

// Interface for sub-select Tasks of the Select Statement, joins, sub-selects
type SubVisitor interface {
	VisitSubselect(stmt *SqlSource) (Task, error)
	VisitJoin(stmt *SqlSource) (Task, error)
}
