package exec

import (
	//"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

func (m *JobExecutor) VisitPreparedStatement(p *plan.PreparedStatement) (Task, error) {
	u.Debugf("VisitPreparedStatement %+v", p.Stmt)
	return nil, ErrNotImplemented
}

func (m *JobExecutor) WalkSelect(p *plan.Select) (Task, error) {
	// execTask := NewTaskSequential(m.Ctx)
	// err := execTask.AddPlan(p)
	// if err != nil {
	// 	return nil, err
	// }
	// return execTask, nil
	return m.WalkPlan(p)
}
