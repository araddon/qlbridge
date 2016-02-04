package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
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
	p          *plan.Source
	Scanner    schema.Scanner
	ExecSource ExecutorSource
	JoinKey    KeyEvaluator
}

// A scanner to read from data source
func NewSource(p *plan.Source) (*Source, error) {

	if p.Stmt == nil {
		return nil, fmt.Errorf("must have from for Source")
	}

	source, err := p.DataSource.Open(p.Stmt.SourceName())
	if err != nil {
		return nil, err
	}

	scanner, hasScanner := source.(schema.Scanner)
	if !hasScanner {
		e, hasSourceExec := source.(ExecutorSource)
		if hasSourceExec {
			s := &Source{
				TaskBase:   NewTaskBase(p.Ctx),
				ExecSource: e,
				p:          p,
			}
			return s, nil
		}
		u.Warnf("source %T does not implement datasource.Scanner", source)
		return nil, fmt.Errorf("%T Must Implement Scanner for %q", source, p.Stmt.String())
	}

	s := &Source{
		TaskBase: NewTaskBase(p.Ctx),
		Scanner:  scanner,
		p:        p,
	}
	return s, nil
}

// A scanner to read from sub-query data source (join, sub-query, static)
func NewSourceScanner(p *plan.Source, scanner schema.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase(p.Ctx),
		Scanner:  scanner,
		p:        p,
	}
	return s
}

func (m *Source) Copy() *Source { return &Source{} }

func (m *Source) Close() error {
	if m.Scanner != nil {
		if closer, ok := m.Scanner.(schema.SourceConn); ok {
			if err := closer.Close(); err != nil {
				return err
			}
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Source) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	//u.Infof("Run() ")

	//u.Debugf("scanner: %T %#v", scanner, scanner)
	iter := m.Scanner.CreateIterator(nil)
	//u.Debugf("iter in source: %T  %#v", iter, iter)
	sigChan := m.SigChan()

	for item := iter.Next(); item != nil; item = iter.Next() {

		//u.Infof("In source Scanner iter %#v", item)
		select {
		case <-sigChan:
			u.Warnf("got shutdown")
			return nil
		case m.msgOutCh <- item:
			// continue
		}

	}
	u.Debugf("leaving source scanner")
	return nil
}
