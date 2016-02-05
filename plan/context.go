package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

// Context for Plan/Execution of a Relational task
// - may be transported across network boundaries to particpate in dag of tasks
// - holds references to in-mem data structures for schema
// - holds references to original statement
// - holds task specific state for errors, ids, etc (net.context)
// - manages Recover() - to persist/transport state
type Context struct {

	// Stateful Fields that are transported to participate across network/nodes
	context.Context                  // Cross-boundry net context
	Raw             string           // Raw sql statement
	Stmt            rel.SqlStatement // Original Statement
	Projection      *Projection      // Projection for this context optional

	// Local in-memory helpers not transported across network
	Session expr.ContextReader // Session for this connection
	Schema  *schema.Schema     // this schema for this connection

	// From configuration
	DisableRecover bool

	// Local State
	Errors     []error
	errRecover interface{}
}

// New plan context
func NewContext(query string) *Context {
	return &Context{Raw: query}
}

// called by go routines/tasks to ensure any recovery panics are captured
func (m *Context) Recover() {
	if m == nil {
		return
	}
	//return
	if m.DisableRecover {
		return
	}
	if r := recover(); r != nil {
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "close of closed") {
			u.Errorf("context recover: %v", r)
		}
		m.errRecover = r
	}
}

var _ = u.EMPTY
