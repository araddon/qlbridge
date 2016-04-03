package exec

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*Command)(nil)
)

// Command
//
type Command struct {
	*TaskBase
	p *plan.Command
}

func NewCommand(ctx *plan.Context, p *plan.Command) *Command {
	m := &Command{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

func (m *Command) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Command) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	return nil
}
