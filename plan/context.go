package plan

import (
	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

// Context for Plan/Execution
type Context struct {
	context.Context
	Raw            string
	Stmt           expr.SqlStatement
	Projection     *Projection
	Session        expr.ContextReader
	Schema         *datasource.Schema
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
