package exec

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	_ TaskRunner = (*Upsert)(nil)
	_ TaskRunner = (*DeletionTask)(nil)
	_ TaskRunner = (*DeletionScanner)(nil)
)

type (
	// Upsert task for insert, update, upsert
	Upsert struct {
		*TaskBase
		closed  bool
		insert  *rel.SqlInsert
		update  *rel.SqlUpdate
		upsert  *rel.SqlUpsert
		db      schema.ConnUpsert
		dbpatch schema.ConnPatchWhere
	}
	// Delete task for sources that natively support delete
	DeletionTask struct {
		*TaskBase
		closed  bool
		p       *plan.Delete
		sql     *rel.SqlDelete
		db      schema.ConnDeletion
		deleted int
	}
	// Delete scanner if we don't have a seek operation on this source
	DeletionScanner struct {
		*DeletionTask
	}
)

// An insert to write to data source
func NewInsert(ctx *plan.Context, p *plan.Insert) *Upsert {
	m := &Upsert{
		TaskBase: NewTaskBase(ctx),
		db:       p.Source,
		insert:   p.Stmt,
	}
	return m
}
func NewUpdate(ctx *plan.Context, p *plan.Update) *Upsert {
	m := &Upsert{
		TaskBase: NewTaskBase(ctx),
		db:       p.Source,
		update:   p.Stmt,
	}
	return m
}
func NewUpsert(ctx *plan.Context, p *plan.Upsert) *Upsert {
	m := &Upsert{
		TaskBase: NewTaskBase(ctx),
		db:       p.Source,
		upsert:   p.Stmt,
	}
	return m
}

// An inserter to write to data source
func NewDelete(ctx *plan.Context, p *plan.Delete) *DeletionTask {
	m := &DeletionTask{
		TaskBase: NewTaskBase(ctx),
		db:       p.Source,
		sql:      p.Stmt,
		p:        p,
	}
	return m
}

func (m *Upsert) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	if closer, ok := m.db.(schema.Source); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return m.TaskBase.Close()
}

func (m *Upsert) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	var err error
	var affectedCt int64
	switch {
	case m.insert != nil:
		affectedCt, err = m.insertRows(m.insert.Rows)
	case m.upsert != nil && len(m.upsert.Rows) > 0:
		affectedCt, err = m.insertRows(m.upsert.Rows)
	case m.update != nil:
		affectedCt, err = m.updateValues()
	default:
		u.Warnf("unknown mutation op?  %v", m)
	}

	vals := make([]driver.Value, 2)
	if err != nil {
		u.Warnf("errored, should not complete %v", err)
		vals[0] = err.Error()
		vals[1] = -1
		m.msgOutCh <- &datasource.SqlDriverMessage{Vals: vals, IdVal: 1}
		return err
	}
	vals[0] = int64(0) // status?
	vals[1] = affectedCt
	u.Infof("affected? %v", affectedCt)
	m.msgOutCh <- &datasource.SqlDriverMessage{Vals: vals, IdVal: 1}
	return nil
}

func (m *Upsert) updateValues() (int64, error) {

	select {
	case <-m.SigChan():
		return 0, nil
	default:
		// fall through
	}

	valmap := make(map[string]driver.Value, len(m.update.Values))
	for key, valcol := range m.update.Values {

		// TODO: qlbridge#13  Need a way of expressing which layer (here, db) this expr should run in?
		//  - ie, run in backend datasource?   or here?  translate the expr to native language
		if valcol.Expr != nil {
			exprVal, ok := vm.Eval(nil, valcol.Expr)
			if !ok {
				u.Errorf("Could not evaluate: %s", valcol.Expr)
				return 0, fmt.Errorf("Could not evaluate expression: %v", valcol.Expr)
			}
			valmap[key] = exprVal.Value()
		} else {
			u.Debugf("%T  %v", valcol.Value.Value(), valcol.Value.Value())
			valmap[key] = valcol.Value.Value()
		}
		//u.Debugf("key:%v col: %v   vals:%v", key, valcol, valmap[key])
	}

	// if our backend source supports Where-Patches, ie update multiple
	dbpatch, ok := m.db.(schema.ConnPatchWhere)
	if ok {
		updated, err := dbpatch.PatchWhere(m.Ctx, m.update.Where.Expr, valmap)
		u.Infof("patch: %v %v", updated, err)
		if err != nil {
			return updated, err
		}
		return updated, nil
	}

	// TODO:   If it does not implement Where Patch then we need to do a poly fill
	//      Do we have to recognize if the Where is on a primary key?
	// - for sources/queries that can't do partial updates we need to do a read first

	// Create a key from Where
	key := datasource.KeyFromWhere(m.update.Where)
	if _, err := m.db.Put(m.Ctx, key, valmap); err != nil {
		u.Errorf("Could not put values: %v", err)
		return 0, err
	}
	return 1, nil
}

func (m *Upsert) insertRows(rows [][]*rel.ValueColumn) (int64, error) {
	for i, row := range rows {
		select {
		case <-m.SigChan():
			if i == 0 {
				return 0, nil
			}
			return int64(i) - 1, nil
		default:
			vals := make([]driver.Value, len(row))
			for x, val := range row {
				if val.Expr != nil {
					exprVal, ok := vm.Eval(nil, val.Expr)
					if !ok {
						u.Errorf("Could not evaluate: %v", val.Expr)
						return 0, fmt.Errorf("Could not evaluate expression: %v", val.Expr)
					}
					vals[x] = exprVal.Value()
				} else {
					vals[x] = val.Value.Value()
				}
			}

			if _, err := m.db.Put(m.Ctx.Context, nil, vals); err != nil {
				u.Errorf("Could not put values: fordb T:%T  %v", m.db, err)
				return 0, err
			}
		}
	}
	return int64(len(rows)), nil
}

func (m *DeletionTask) Close() error {
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()
	if closer, ok := m.db.(schema.Source); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return m.TaskBase.Close()
}

func (m *DeletionTask) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	vals := make([]driver.Value, 2)
	deletedCt, err := m.db.DeleteExpression(m.p, m.sql.Where.Expr)
	if err != nil {
		u.Errorf("Could not delete values: %v", err)
		vals[0] = err.Error()
		vals[1] = int64(0)
		m.msgOutCh <- &datasource.SqlDriverMessage{Vals: vals, IdVal: 1}
		return err
	}
	m.deleted = deletedCt

	vals[0] = int64(0)
	vals[1] = int64(deletedCt)
	m.msgOutCh <- &datasource.SqlDriverMessage{Vals: vals, IdVal: 1}

	return nil
}

func (m *DeletionScanner) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	select {
	case <-m.SigChan():
		return nil
	default:
		if m.sql.Where != nil {

			vals := make([]driver.Value, 2)
			deletedCt, err := m.db.DeleteExpression(m.p, m.sql.Where.Expr)
			if err != nil {
				u.Errorf("Could not delete values: %v", err)

				vals[0] = err.Error()
				vals[1] = int64(0)
				m.msgOutCh <- &datasource.SqlDriverMessage{Vals: vals, IdVal: 1}
				return err
			}
			m.deleted = deletedCt
			vals[0] = int64(0)
			vals[1] = int64(deletedCt)
			m.msgOutCh <- &datasource.SqlDriverMessage{Vals: vals, IdVal: 1}
		}
	}
	return nil
}
