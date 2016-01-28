package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY
)

func (m *JobBuilder) VisitInsert(sp *plan.Insert) (plan.Task, rel.VisitStatus, error) {

	u.Debugf("VisitInsert %s", sp.Stmt)
	stmt := sp.Stmt
	planner := m.TaskMaker.Sequential("insert")

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("%p schema %v", m.Ctx.Schema, err)
		return nil, rel.VisitError, schema.ErrNotFound
	}

	mutatorSource, hasMutator := conn.(schema.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewInsertUpsert(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}

	if upsertDs, isUpsert := conn.(schema.Upsert); isUpsert {
		insertTask := NewInsertUpsert(m.Ctx, stmt, upsertDs)
		//u.Debugf("adding insert: %#v", insertTask)
		planner.Add(insertTask)
	} else {
		u.Warnf("doesn't implement upsert? %T", conn)
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Upsert or SourceMutation", conn)
	}

	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitUpdate(sp *plan.Update) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", sp.Stmt)

	planner := m.TaskMaker.Sequential("update")

	conn, err := m.Ctx.Schema.Open(sp.Stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", sp.Stmt.Table)
		return nil, rel.VisitError, schema.ErrNotFound
	}

	mutatorSource, hasMutator := conn.(schema.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpdateUpsert(m.Ctx, sp, mutator)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := conn.(schema.Upsert)
	if !hasUpdate {
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", conn)
	}
	task := NewUpdateUpsert(m.Ctx, sp, updateSource)
	planner.Add(task)
	//u.Debugf("adding update conn %#v", conn)
	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitUpsert(sp *plan.Upsert) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitUpsert %+v", sp.Stmt)
	//u.Debugf("VisitUpsert %T  %s\n%#v", stmt, stmt.String(), stmt)
	planner := m.TaskMaker.Sequential("upsert")

	conn, err := m.Ctx.Schema.Open(sp.Stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", sp.Stmt.Table)
		return nil, rel.VisitError, schema.ErrNotFound
	}

	mutatorConn, hasMutator := conn.(schema.SourceMutation)
	if hasMutator {
		mutator, err := mutatorConn.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpsertUpsert(m.Ctx, sp, mutator)
			//u.Debugf("adding delete conn %#v", conn)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := conn.(schema.Upsert)
	if !hasUpdate {
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", conn)
	}
	task := NewUpsertUpsert(m.Ctx, sp, updateSource)
	planner.Add(task)
	//u.Debugf("adding update conn %#v", conn)
	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitDelete(sp *plan.Delete) (plan.Task, rel.VisitStatus, error) {
	u.Debugf("VisitDelete %+v", sp.Stmt)
	planner := m.TaskMaker.Sequential("delete")

	conn, err := m.Ctx.Schema.Open(sp.Stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", sp.Stmt.Table)
		return nil, rel.VisitError, schema.ErrNotFound
	}

	mutatorConn, hasMutator := conn.(schema.SourceMutation)
	if hasMutator {
		mutator, err := mutatorConn.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewDelete(m.Ctx, sp, mutator)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}
	deletionSource, hasDeletion := conn.(schema.Deletion)
	if !hasDeletion {
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Deletion or SourceMutation", conn)
	}
	task := NewDelete(m.Ctx, sp, deletionSource)
	//u.Infof("adding delete task: %#v", task)
	planner.Add(task)
	return planner, rel.VisitContinue, nil
}
