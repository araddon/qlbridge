package exec

import (
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
	sp      *plan.SourcePlan
	scanner schema.Scanner
	JoinKey KeyEvaluator
}

// A scanner to read from data source
func NewSource(sp *plan.SourcePlan, scanner schema.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase(sp.Ctx, "Source"),
		scanner:  scanner,
		sp:       sp,
	}
	return s
}

// A scanner to read from sub-query data source (join, sub-query)
func NewSourceJoin(sp *plan.SourcePlan, scanner schema.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase(sp.Ctx, "SourceJoin"),
		scanner:  scanner,
		sp:       sp,
	}
	return s
}

func (m *Source) Copy() *Source { return &Source{} }

func (m *Source) Close() error {
	if closer, ok := m.scanner.(schema.SourceConn); ok {
		if err := closer.Close(); err != nil {
			return err
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
	iter := m.scanner.CreateIterator(nil)
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
	//u.Debugf("leaving source scanner")
	return nil
}
