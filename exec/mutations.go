package exec

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	// to ensure this can run in exec engine
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

	upsert, ok := m.db.(datasource.Upsert)
	if !ok {
		return fmt.Errorf("Does not implement Scanner: %T", m.db)
	}
	//u.Debugf("iter in sql insert: %T  %#v", iter, iter)

	// Need to be able parse/convert sql into []driver.value

	for _, row := range m.sql.Rows {
		u.Infof("In Insert Scanner iter %#v", row)
		select {
		case <-m.SigChan():
			u.Warnf("got signal quit")
			return nil
		default:
			vals := make([]driver.Value, len(row))
			for x, val := range row {
				vals[x] = val.Value()
			}
			//m.msgOutCh <- &datasource.SqlDriverMessage{vals, uint64(i)}
			upsert.Put(vals)
			// continue
		}
	}
	//u.Debugf("leaving insert ")
	return nil
}
