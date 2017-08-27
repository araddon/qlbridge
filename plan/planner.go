package plan

import (
	u "github.com/araddon/gou"
)

var (
	// Ensure our default planner meets interface Planner
	_ Planner = (*PlannerDefault)(nil)
)

// PlannerDefault is implementation of Planner that creates a dag of plan.Tasks
// that will be turned into execution plan by executor.  This is a simple
// planner but can be over-ridden by providing a Planner that will
// supercede any single or more visit methods.
// - stateful, specific to a single request
type PlannerDefault struct {
	Planner  Planner
	Ctx      *Context
	distinct bool
	children []Task
}

func NewPlanner(ctx *Context) *PlannerDefault {
	p := &PlannerDefault{
		Ctx:      ctx,
		children: make([]Task, 0),
	}
	p.Planner = p
	return p
}

func (m *PlannerDefault) WalkCommand(p *Command) error {
	u.Debugf("VisitCommand %+v", p.Stmt)
	return nil
}

func (m *PlannerDefault) WalkCreate(p *Create) error {
	u.Debugf("WalkCreate %+v", p.Stmt)
	return nil
}
