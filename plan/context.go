package plan

import (
	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
)

// Context for Plan/Execution
type Context struct {
	context.Context                    // Cross-boundry net context
	Raw             string             // Raw statement
	Stmt            expr.SqlStatement  // Original Statement
	Projection      *Projection        // Final Projection
	Session         expr.ContextReader // Session for this connection
	Schema          *schema.Schema     // this schema for this connection

	// Connection specific erros, handling
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

// Get a New Context and its Schema from connection
func NewContext(query string) *Context {
	return &Context{Raw: query}
}
