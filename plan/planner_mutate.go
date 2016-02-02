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

func upsertSource(ctx *Context, table string) (schema.Upsert, error) {

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

func (m *PlannerDefault) WalkInsert(p *Insert) error {
	u.Debugf("VisitInsert %s", p.Stmt)
	return nil
}

func (m *PlannerDefault) WalkUpdate(p *Update) error {
	u.Debugf("VisitUpdate %+v", p.Stmt)
	return nil
}

func (m *PlannerDefault) WalkUpsert(p *Upsert) error {
	u.Debugf("VisitUpsert %+v", p.Stmt)
	return nil
}

func (m *PlannerDefault) WalkDelete(p *Delete) error {
	u.Debugf("VisitDelete %+v", p.Stmt)
	//planner := m.TaskMaker.Sequential("delete")
	return nil
}
