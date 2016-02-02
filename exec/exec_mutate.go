package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

func (m *JobExecutor) mustBeExecTask(task plan.Task, err error) (Task, error) {

	if err != nil {
		return nil, err
	}
	execTask, ok := task.(Task)
	if !ok {
		return nil, fmt.Errorf("%T must implement exec.Task", task)
	}
	return execTask, nil

}

func (m *JobExecutor) VisitInto(p *plan.Into) (Task, error) {
	u.Debugf("VisitInto %+v", p.Stmt)
	return nil, ErrNotImplemented
}
func (m *JobExecutor) VisitInsert(p *plan.Insert) (Task, error) {
	u.Warnf("VisitInsert %+v", p.Stmt)
	return m.mustBeExecTask(p, m.Planner.WalkInsert(p))
}
func (m *JobExecutor) VisitUpdate(p *plan.Update) (Task, error) {
	return m.mustBeExecTask(p, m.Planner.WalkUpdate(p))
}
func (m *JobExecutor) VisitUpsert(p *plan.Upsert) (Task, error) {
	return m.mustBeExecTask(p, m.Planner.WalkUpsert(p))
}
func (m *JobExecutor) VisitDelete(p *plan.Delete) (Task, error) {
	return m.mustBeExecTask(p, m.Planner.WalkDelete(p))
}
