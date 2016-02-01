package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY
)

func (m *JobBuilder) WalkInto(p *plan.Into) error {
	u.Debugf("VisitInto %+v", p.Stmt)
	return ErrNotImplemented
}

func upsertSource(ctx *plan.Context, table string) (schema.Upsert, error) {

	conn, err := ctx.Schema.Open(table)
	if err != nil {
		u.Warnf("%p no schema for %q err=%v", ctx.Schema, table, err)
		return nil, err
	}

	mutatorSource, hasMutator := conn.(schema.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(ctx)
		if err != nil {
			u.Warnf("%p could not create mutator for %q err=%v", ctx.Schema, table, err)
			//return nil, err
		} else {
			return mutator, nil
		}
	}

	upsertDs, isUpsert := conn.(schema.Upsert)
	if !isUpsert {
		return nil, fmt.Errorf("%T does not implement required schema.Upsert for upserts", conn)
	}
	return upsertDs, nil
}

func (m *JobBuilder) WalkInsert(p *plan.Insert) error {

	u.Debugf("VisitInsert %s", p.Stmt)
	mutator, err := upsertSource(m.Ctx, p.Stmt.Table)
	if err != nil {
		return err
	}

	//rootTask := m.TaskMaker.Sequential("update")
	mutateTask := NewInsertUpsert(m.Ctx, p.Stmt, mutator)
	p.Add(mutateTask)
	return nil
}

func (m *JobBuilder) WalkUpdate(p *plan.Update) error {
	u.Debugf("VisitUpdate %+v", p.Stmt)

	mutator, err := upsertSource(m.Ctx, p.Stmt.Table)
	if err != nil {
		return err
	}

	//rootTask := m.TaskMaker.Sequential("update")
	mutateTask := NewUpdateUpsert(m.Ctx, p, mutator)
	p.Add(mutateTask)
	return nil
}

func (m *JobBuilder) WalkUpsert(p *plan.Upsert) error {
	u.Debugf("VisitUpsert %+v", p.Stmt)

	mutator, err := upsertSource(m.Ctx, p.Stmt.Table)
	if err != nil {
		return err
	}

	//rootTask := m.TaskMaker.Sequential("update")
	mutateTask := NewUpsertUpsert(m.Ctx, p, mutator)
	p.Add(mutateTask)
	return nil
}

func (m *JobBuilder) WalkDelete(p *plan.Delete) error {
	u.Debugf("VisitDelete %+v", p.Stmt)
	//planner := m.TaskMaker.Sequential("delete")

	conn, err := m.Ctx.Schema.Open(p.Stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", p.Stmt.Table)
		return schema.ErrNotFound
	}

	mutatorConn, hasMutator := conn.(schema.SourceMutation)
	if hasMutator {
		mutator, err := mutatorConn.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewDelete(m.Ctx, p, mutator)
			//u.Infof("adding delete: %#v", task)
			p.Add(task)
			return nil
		}
	}
	deletionSource, hasDeletion := conn.(schema.Deletion)
	if !hasDeletion {
		return fmt.Errorf("%T Must Implement Deletion or SourceMutation", conn)
	}
	task := NewDelete(m.Ctx, p, deletionSource)
	//u.Infof("adding delete task: %#v", task)
	p.Add(task)
	return nil
}
