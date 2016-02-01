package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

func (m *JobExecutor) mustBeExecTask(task plan.Task, status plan.WalkStatus, err error) (Task, plan.WalkStatus, error) {

	if err != nil {
		return nil, status, err
	}
	execTask, ok := task.(Task)
	if !ok {
		return nil, plan.WalkError, fmt.Errorf("%T must implement exec.Task", task)
	}
	return execTask, status, nil

}

func (m *JobExecutor) VisitInto(p *plan.Into) (Task, plan.WalkStatus, error) {
	u.Debugf("VisitInto %+v", p.Stmt)
	return nil, plan.WalkError, ErrNotImplemented
}
func (m *JobExecutor) VisitInsert(p *plan.Insert) (Task, plan.WalkStatus, error) {
	u.Warnf("VisitInsert %+v", p.Stmt)
	return m.mustBeExecTask(m.Planner.VisitInsert(p))
}
func (m *JobExecutor) VisitUpdate(p *plan.Update) (Task, plan.WalkStatus, error) {
	return m.mustBeExecTask(m.Planner.VisitUpdate(p))
}
func (m *JobExecutor) VisitUpsert(p *plan.Upsert) (Task, plan.WalkStatus, error) {
	return m.mustBeExecTask(m.Planner.VisitUpsert(p))
}
func (m *JobExecutor) VisitDelete(p *plan.Delete) (Task, plan.WalkStatus, error) {
	return m.mustBeExecTask(m.Planner.VisitDelete(p))
}
