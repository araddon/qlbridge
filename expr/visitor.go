package expr

import (
	u "github.com/araddon/gou"
	"golang.org/x/net/context"
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

// Context for Plan/Execution
type Context struct {
	context.Context
	Raw            string
	ConnInfo       string
	Stmt           SqlStatement
	Session        ContextReader
	DisableRecover bool
	Errors         []error
	errRecover     interface{}
	id             string
	prefix         string
}

func (m *Context) Recover() {
	if m == nil {
		return
	}
	if m.DisableRecover {
		return
	}
	if r := recover(); r != nil {
		u.Errorf("context recover: %v", r)
		m.errRecover = r
	}
}

func NewContext(query string) *Context {
	return &Context{Raw: query}
}
func NewContextConn(conn, query string) *Context {
	return &Context{ConnInfo: conn, Raw: query}
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
	VisitPreparedStmt(stmt *PreparedStatement) (Task, VisitStatus, error)
	VisitSelect(stmt *SqlSelect) (Task, VisitStatus, error)
	VisitInsert(stmt *SqlInsert) (Task, VisitStatus, error)
	VisitUpsert(stmt *SqlUpsert) (Task, VisitStatus, error)
	VisitUpdate(stmt *SqlUpdate) (Task, VisitStatus, error)
	VisitDelete(stmt *SqlDelete) (Task, VisitStatus, error)
	VisitShow(stmt *SqlShow) (Task, VisitStatus, error)
	VisitDescribe(stmt *SqlDescribe) (Task, VisitStatus, error)
	VisitCommand(stmt *SqlCommand) (Task, VisitStatus, error)
}

// Interface for sub-select Tasks of the Select Statement
type SourceVisitor interface {
	VisitSourceSelect(stmt *SqlSource) (Task, VisitStatus, error)
}
