package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*Create)(nil)
)

// Create is executeable task for SET SQL Create, Alter
type Create struct {
	*TaskBase
	p *plan.Create
}

// NewCommand creates new create exec task
func NewCreate(ctx *plan.Context, p *plan.Create) *Create {
	m := &Create{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

// Close Create
func (m *Create) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

// Run Create
func (m *Create) Run() error {
	//defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	if m.Ctx.Session == nil {
		u.Warnf("no Context.Session?")
		return fmt.Errorf("no Context.Session?")
	}

	switch kw := m.p.Stmt.Keyword(); kw {
	case lex.TokenCreate:
		u.Warnf("not implemented CREATE")
		return plan.ErrNotImplemented
	default:
		u.Warnf("unrecognized create/alter: kw=%v   stmt:%s", kw, m.p.Stmt)
	}
	return ErrNotImplemented

}
