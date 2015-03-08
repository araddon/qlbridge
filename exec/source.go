package exec

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	// to ensure this can run in exec engine
	_ TaskRunner = (*SourceScanner)(nil)
)

// Scan a data source for rows, feed into runner
//
//  1) table      -- FROM table
//  2) channels   -- FROM stream
//  3) join       -- SELECT t1.name, t2.salary
//                        FROM employee AS t1 INNER JOIN info AS t2 ON t1.name = t2.name;
//  4) sub-select -- SELECT * FROM (SELECT 1, 2, 3) AS t1;
//
type SourceScanner struct {
	*TaskBase
	source datasource.DataSource
}

// A scanner to read from data source
func NewSourceScanner(from string, source datasource.DataSource) *SourceScanner {
	s := &SourceScanner{
		TaskBase: NewTaskBase("SourceScanner"),
		source:   source,
	}
	s.TaskBase.TaskType = s.Type()

	return s
}

// func (m *SourceScanner) Accept(visitor PlanVisitor) (interface{}, error) {
// 	return visitor.VisitSourceScan(m)
// }

func (m *SourceScanner) Copy() *SourceScanner {
	return &SourceScanner{}
}
func (m *SourceScanner) Close() error {
	if err := m.source.Close(); err != nil {
		return err
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *SourceScanner) Run(context *Context) error {
	defer context.Recover() // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing input channels is the signal to stop

	//u.Infof("in source scanner")
	iter := m.source.CreateIterator(nil)

	for item := iter.Next(); item != nil; item = iter.Next() {
		//switch ctxReader := item.Body().(type) {
		switch item.Body().(type) {
		case *datasource.ContextUrlValues:
			//u.Debugf("found url.Values: %v", ctxReader)
			select {
			case <-m.SigChan():
				u.Warnf("quit channel?")
				return nil
			case m.msgOutCh <- item:
				// continue
			}
		default:
			u.Debug(item.Body())
			select {
			case <-m.SigChan():
				return nil
			case m.msgOutCh <- item:
				// continue
			}
		}

	}
	//u.Warnf("leaving source scanner")
	return nil
}
