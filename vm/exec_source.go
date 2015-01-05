package vm

import (
	_ "fmt"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/value"
)

// Scan a data source for rows, feed into pipeline
//
//  1) table      -- FROM table
//  2) channels   -- FROM stream
//  3) join       -- SELECT t1.name, t2.salary
//                        FROM employee AS t1 INNER JOIN info AS t2 ON t1.name = t2.name;
//  4) sub-select -- SELECT * FROM (SELECT 1, 2, 3) AS t1;
type SourceKey struct {
	runBase
	source datasource.DataSource
}

// plan *plan.KeyScan
func NewKeyScan() *SourceKey {
	s := &SourceKey{
		runBase: newRunBase(),
	}

	return s
}

func (m *SourceKey) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitSourceKeyScan(m)
}

func (m *SourceKey) Copy() PartialRunner {
	return &SourceKey{}
}

func (m *SourceKey) Run(context *Context, parent value.Value) {
	defer context.Recover() // Our context can recover panics, save error msg
	//defer close(m.itemCh)   // closing input channels is the signal to stop

	//source := datasource.New(context)
	iter := m.source.CreateIterator(nil)
	//defer notifySourceStop(source)

	for item := iter.Next(); item != nil; item = iter.Next() {

		if item == nil {
			return
		}
		select {
		case <-m.quitCh:
			return
		default:
		}

		// if filtered out?
		//m.sendItem(item)
	}
}
