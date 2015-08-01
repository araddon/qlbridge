package exec

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	_ TaskRunner = (*Upsert)(nil)
)

// Upsert data task
//
type Upsert struct {
	*TaskBase
	insert *expr.SqlInsert
	update *expr.SqlUpdate
	upsert *expr.SqlUpsert
	db     datasource.Upsert
}

// An insert to write to data source
func NewInsertUpsert(sql *expr.SqlInsert, db datasource.Upsert) *Upsert {
	m := &Upsert{
		TaskBase: NewTaskBase("Upsert"),
		db:       db,
		insert:   sql,
	}
	m.TaskBase.TaskType = m.Type()
	return m
}
func NewUpdateUpsert(sql *expr.SqlUpdate, db datasource.Upsert) *Upsert {
	m := &Upsert{
		TaskBase: NewTaskBase("Upsert"),
		db:       db,
		update:   sql,
	}
	m.TaskBase.TaskType = m.Type()
	return m
}

func NewUpsertUpsert(sql *expr.SqlUpsert, db datasource.Upsert) *Upsert {
	m := &Upsert{
		TaskBase: NewTaskBase("Upsert"),
		db:       db,
		upsert:   sql,
	}
	m.TaskBase.TaskType = m.Type()
	return m
}

func (m *Upsert) Copy() *Upsert { return &Upsert{} }

func (m *Upsert) Close() error {
	if closer, ok := m.db.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Upsert) Run(ctx *Context) error {
	defer ctx.Recover()
	defer close(m.msgOutCh)

	switch {
	case m.insert != nil && len(m.insert.Rows) > 0:
		u.Debugf("Insert.Run():  %v   %#v", len(m.insert.Rows), m.insert)
		return m.insertRows(ctx, m.insert.Rows)
	case m.upsert != nil && len(m.upsert.Rows) > 0:
		u.Debugf("Upsert.Run():  %v   %#v", len(m.upsert.Rows), m.upsert)
		return m.insertRows(ctx, m.upsert.Rows)
	case m.update != nil:
		u.Debugf("update implemented? %v", m.update.String())
		return m.updateValues(ctx)
	default:
		u.Warnf("unknown mutation op?  %v", m)
	}
	return nil
}

func (m *Upsert) updateValues(ctx *Context) error {

	for key, val := range m.update.Values {
		u.Infof("In Update iter %s:%#v", key, val)
		select {
		case <-m.SigChan():
			u.Warnf("got signal quit")
			return nil
		default:
			// vals := make([]driver.Value, len(row))
			// for x, val := range row {
			// 	if val.Expr != nil {
			// 		exprVal, ok := vm.Eval(nil, val.Expr)
			// 		if !ok {
			// 			u.Errorf("Could not evaluate: %v", val.Expr)
			// 			return fmt.Errorf("Could not evaluate expression: %v", val.Expr)
			// 		}
			// 		vals[x] = exprVal.Value()
			// 	} else {
			// 		vals[x] = val.Value.Value()
			// 	}
			// 	//u.Debugf("%d col: %v   vals:%v", x, val, vals[x])
			// }
			// //m.msgOutCh <- &datasource.SqlDriverMessage{vals, uint64(i)}
			// if _, err := m.db.Put(ctx, nil, vals); err != nil {
			// 	u.Errorf("Could not put values: %v", err)
			// 	return err
			// }
			// continue
		}
	}
	return nil
}

func (m *Upsert) insertRows(ctx *Context, rows [][]*expr.ValueColumn) error {

	for _, row := range rows {
		//u.Infof("In Insert Scanner iter %#v", row)
		select {
		case <-m.SigChan():
			u.Warnf("got signal quit")
			return nil
		default:
			vals := make([]driver.Value, len(row))
			for x, val := range row {
				if val.Expr != nil {
					exprVal, ok := vm.Eval(nil, val.Expr)
					if !ok {
						u.Errorf("Could not evaluate: %v", val.Expr)
						return fmt.Errorf("Could not evaluate expression: %v", val.Expr)
					}
					vals[x] = exprVal.Value()
				} else {
					vals[x] = val.Value.Value()
				}
				//u.Debugf("%d col: %v   vals:%v", x, val, vals[x])
			}
			//m.msgOutCh <- &datasource.SqlDriverMessage{vals, uint64(i)}
			if _, err := m.db.Put(ctx, nil, vals); err != nil {
				u.Errorf("Could not put values: %v", err)
				return err
			}
			// continue
		}
	}
	return nil
}

// Delete task
//
type DeletionTask struct {
	*TaskBase
	sql     *expr.SqlDelete
	db      datasource.Deletion
	deleted int
}

// An inserter to write to data source
func NewDelete(sql *expr.SqlDelete, db datasource.Deletion) *DeletionTask {
	m := &DeletionTask{
		TaskBase: NewTaskBase("Delete"),
		db:       db,
		sql:      sql,
	}
	m.TaskBase.TaskType = m.Type()
	return m
}

func (m *DeletionTask) Copy() *DeletionTask { return &DeletionTask{} }

func (m *DeletionTask) Close() error {
	if closer, ok := m.db.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *DeletionTask) Run(context *Context) error {
	defer context.Recover()
	defer close(m.msgOutCh)

	//u.Warnf("DeletionTask.Run():  %v   %#v", len(m.sql.Rows), m.sql)

	u.Infof("In Delete Scanner expr %#v", m.sql.Where)
	select {
	case <-m.SigChan():
		return nil
	default:
		if m.sql.Where != nil {
			// Hm, how do i evaluate here?  Do i need a special Vm?
			//return fmt.Errorf("Not implemented delete vm")
			deletedCt, err := m.db.DeleteExpression(m.sql.Where)
			if err != nil {
				u.Errorf("Could not put values: %v", err)
				return err
			}
			m.deleted = deletedCt
			vals := make([]driver.Value, 2)
			vals[0] = int64(0)
			vals[1] = int64(deletedCt)
			m.msgOutCh <- &datasource.SqlDriverMessage{vals, 1}
			//return &qlbResult{affected: deletedCt}
		}
		// continue
	}
	return nil
}
