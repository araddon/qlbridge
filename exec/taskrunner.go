package exec

import (
	"github.com/araddon/qlbridge/plan"
)

var (
	// ensure we implement interface
	_ plan.TaskPlanner = (*TaskRunners)(nil)
)

type TaskRunners struct {
	ctx     *plan.Context
	tasks   []plan.Task
	runners []TaskRunner
}

func TaskRunnersMaker(ctx *plan.Context) plan.TaskPlanner {
	return &TaskRunners{
		ctx:     ctx,
		tasks:   make([]plan.Task, 0),
		runners: make([]TaskRunner, 0),
	}
}
func (m *TaskRunners) SourceVisitorMaker(sp *plan.Source) plan.SourceVisitor {
	sb := NewSourceBuilder(sp, m)
	sb.SourceVisitor = sb
	return sb
}
func (m *TaskRunners) Sequential(name string) plan.Task {
	return NewSequential(m.ctx, name)
}
func (m *TaskRunners) Parallel(name string) plan.Task {
	return NewTaskParallel(m.ctx, name)
}
