package plan

import (
	"fmt"

	u "github.com/araddon/gou"
)

var (
	// Ensure our default planner meets Planner interface.
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

// NewPlanner creates a new default planner with context.
func NewPlanner(ctx *Context) *PlannerDefault {
	p := &PlannerDefault{
		Ctx:      ctx,
		children: make([]Task, 0),
	}
	p.Planner = p
	return p
}

// WalkCommand walks the command statement
func (m *PlannerDefault) WalkCommand(p *Command) error {
	u.Debugf("VisitCommand %+v", p.Stmt)
	return nil
}

// WalkCreate walk a Create Plan to create the dag of tasks for Create.
func (m *PlannerDefault) WalkCreate(p *Create) error {
	u.Debugf("WalkCreate %#v", p)
	if len(p.Stmt.With) == 0 {
		return fmt.Errorf("CREATE {SCHEMA|SOURCE|DATABASE}")
	}
	return nil
}
