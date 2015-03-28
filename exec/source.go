package exec

import (
	"fmt"
	//"sync"
	//"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	//"github.com/mdmarek/topo"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	// to ensure this can run in exec engine
	_ TaskRunner = (*Source)(nil)
)

// Scan a data source for rows, feed into runner.  The source scanner being
//   a source is iter.Next() messages instead of sending them on input channel
//
//  1) table      -- FROM table
//  2) channels   -- FROM stream
//  3) join       -- SELECT t1.name, t2.salary
//                       FROM employee AS t1
//                       INNER JOIN info AS t2
//                       ON t1.name = t2.name;
//  4) sub-select -- SELECT * FROM (SELECT 1, 2, 3) AS t1;
//
type Source struct {
	*TaskBase
	source datasource.Scanner
}

// A scanner to read from data source
func NewSource(from string, source datasource.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase("Source"),
		source:   source,
	}
	s.TaskBase.TaskType = s.Type()

	return s
}

func (m *Source) Copy() *Source { return &Source{} }

func (m *Source) Close() error {
	if closer, ok := m.source.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Source) Run(context *Context) error {
	defer context.Recover() // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing input channels is the signal to stop

	// TODO:  Allow an alternate interface that allows Source to provide
	//        an output channel?
	scanner, ok := m.source.(datasource.Scanner)
	if !ok {
		return fmt.Errorf("Does not implement Scanner: %T", m.source)
	}
	iter := scanner.CreateIterator(nil)

	for item := iter.Next(); item != nil; item = iter.Next() {

		//u.Infof("In source Scanner iter %#v", item)
		select {
		case <-m.SigChan():
			u.Warnf("got signal quit")
			return nil
		case m.msgOutCh <- item:
			// continue
		}

	}
	//u.Debugf("leaving source scanner")
	return nil
}

// Scan a data source for rows, feed into runner for join sources
//
//  1) join  SELECT t1.name, t2.salary
//               FROM employee AS t1
//               INNER JOIN info AS t2
//               ON t1.name = t2.name;
//
type SourceJoin struct {
	*TaskBase
	conf        *RuntimeConfig
	leftStmt    *expr.SqlSource
	rightStmt   *expr.SqlSource
	leftSource  datasource.Scanner
	rightSource datasource.Scanner
}

// A scanner to read from data source
func NewSourceJoin(leftFrom, rightFrom *expr.SqlSource, conf *RuntimeConfig) (*SourceJoin, error) {
	m := &SourceJoin{
		TaskBase: NewTaskBase("SourceJoin"),
	}
	m.TaskBase.TaskType = m.Type()

	m.leftStmt = leftFrom
	m.rightStmt = rightFrom

	source := conf.Conn(leftFrom.Name)
	u.Debugf("source: %T", source)
	// Must provider either Scanner, and or Seeker interfaces
	if scanner, ok := source.(datasource.Scanner); !ok {
		u.Errorf("Could not create scanner for %v  %T %#v", leftFrom.Name, source, source)
		return nil, fmt.Errorf("Must Implement Scanner")
	} else {
		m.leftSource = scanner
	}

	source2 := conf.Conn(leftFrom.Name)
	u.Debugf("source right: %T", source2)
	// Must provider either Scanner, and or Seeker interfaces
	if scanner, ok := source2.(datasource.Scanner); !ok {
		u.Errorf("Could not create scanner for %v  %T %#v", leftFrom.Name, source2, source2)
		return nil, fmt.Errorf("Must Implement Scanner")
	} else {
		m.rightSource = scanner
	}

	return m, nil
}

func (m *SourceJoin) Copy() *Source { return &Source{} }

func (m *SourceJoin) Close() error {
	if closer, ok := m.leftSource.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if closer, ok := m.rightSource.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *SourceJoin) Run(context *Context) error {
	defer context.Recover() // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing input channels is the signal to stop

	leftIn := m.leftSource.MesgChan(nil)
	rightIn := m.rightSource.MesgChan(nil)

	/*
		JOIN = INNER JOIN = Equal Join

		1)   Do we need to Filter?
		     - A way of getting that in scanner
		     - If so we need Rewritten Where
		2)
	*/
	for {

		//u.Infof("In source Scanner iter %#v", item)
		select {
		case <-m.SigChan():
			u.Warnf("got signal quit")
			return nil
		case m := <-leftIn:
			u.Debugf("%v", m)
		case m := <-rightIn:
			u.Debugf("%v", m)
		}

	}

	//u.Debugf("leaving source scanner")
	return nil
}
