package plan

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY
)

func (m *PlannerDefault) WalkInto(p *Into) error {
	u.Debugf("VisitInto %+v", p.Stmt)
	return ErrNotImplemented
}

func upsertSource(ctx *Context, table string) (schema.ConnUpsert, error) {

	conn, err := ctx.Schema.OpenConn(table)
	if err != nil {
		u.Warnf("%p no schema for %q err=%v", ctx.Schema, table, err)
		return nil, err
	}

	mutatorSource, hasMutator := conn.(schema.ConnMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(ctx)
		if err != nil {
			u.Warnf("%p could not create mutator for %q err=%v", ctx.Schema, table, err)
			//return nil, err
		} else {
			return mutator, nil
		}
	}

	upsertDs, isUpsert := conn.(schema.ConnUpsert)
	if !isUpsert {
		return nil, fmt.Errorf("%T does not implement required schema.Upsert for upserts", conn)
	}
	return upsertDs, nil
}

func (m *PlannerDefault) WalkInsert(p *Insert) error {
	u.Debugf("VisitInsert %s", p.Stmt)
	src, err := upsertSource(m.Ctx, p.Stmt.Table)
	if err != nil {
		return err
	}
	p.Source = src
	return nil
}

func (m *PlannerDefault) WalkUpdate(p *Update) error {
	u.Debugf("VisitUpdate %+v", p.Stmt)
	src, err := upsertSource(m.Ctx, p.Stmt.Table)
	if err != nil {
		return err
	}
	p.Source = src
	return nil
}

func (m *PlannerDefault) WalkUpsert(p *Upsert) error {
	u.Debugf("VisitUpsert %+v", p.Stmt)
	src, err := upsertSource(m.Ctx, p.Stmt.Table)
	if err != nil {
		return err
	}
	p.Source = src
	return nil
}

func (m *PlannerDefault) WalkDelete(p *Delete) error {
	u.Debugf("VisitDelete %+v", p.Stmt)
	conn, err := m.Ctx.Schema.OpenConn(p.Stmt.Table)
	if err != nil {
		u.Warnf("%p no schema for %q err=%v", m.Ctx.Schema, p.Stmt.Table, err)
		return err
	}

	mutatorSource, hasMutator := conn.(schema.ConnMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Warnf("%p could not create mutator for %q err=%v", m.Ctx.Schema, p.Stmt.Table, err)
			//return nil, err
		} else {
			p.Source = mutator
			return nil
		}
	}

	deleteDs, isDelete := conn.(schema.ConnDeletion)
	if !isDelete {
		return fmt.Errorf("%T does not implement required schema.Deletion for deletions", conn)
	}
	p.Source = deleteDs
	return nil
}
