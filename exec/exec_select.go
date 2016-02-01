package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

func (m *JobExecutor) VisitPreparedStatement(p *plan.PreparedStatement) (Task, error) {
	u.Debugf("VisitPreparedStatement %+v", p.Stmt)
	return nil, ErrNotImplemented
}

func (m *JobExecutor) WalkSelect(p *plan.Select) (Task, error) {

	planTask, _, err := m.Planner.VisitSelect(p)
	if err != nil {
		return nil, err
	}
	execTask, ok := planTask.(Task)
	if !ok {
		return nil, fmt.Errorf("%T must implement exec.Task", planTask)
	}
	return execTask, nil
}
