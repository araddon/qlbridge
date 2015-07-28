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

	_ TaskRunner = (*Insert)(nil)
)

// Insert data task
//
type Insert struct {
	*TaskBase
	sql *expr.SqlInsert
	db  datasource.Upsert
}

// An inserter to write to data source
func NewInsert(sql *expr.SqlInsert, db datasource.Upsert) *Insert {
	m := &Insert{
		TaskBase: NewTaskBase("Insert"),
		db:       db,
		sql:      sql,
	}
	m.TaskBase.TaskType = m.Type()
	return m
}

func (m *Insert) Copy() *Insert { return &Insert{} }

func (m *Insert) Close() error {
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

func (m *Insert) Run(context *Context) error {
	defer context.Recover() // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing input channels is the signal to stop

	u.Warnf("Insert.Run():  %v   %#v", len(m.sql.Rows), m.sql)

	// We need another SourceInsert for
	//    SELECT a,b,c FROM users
	//        INTO archived_users ....
	for _, row := range m.sql.Rows {
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
			if err := m.db.Put(vals); err != nil {
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
