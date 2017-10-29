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

// RequiresContext defines a Source which requires context.
type RequiresContext interface {
	SetContext(ctx *plan.Context)
}

// Source defines a datasource execution task.  It will Scan a data source for
// rows to feed into exec dag of tasks.  The source scanner uses iter.Next()
// messages.  The source may optionally allow Predicate PushDown, that is
// use the SQL select/where to filter rows so its not a real table scan. This
// interface is called ExecutorSource.
//
// Examples of Sources:
// 1) table      -- FROM table
// 2) channels   -- FROM stream
// 3) join       -- SELECT t1.name, t2.salary
//                       FROM employee AS t1
//                       INNER JOIN info AS t2
//                       ON t1.name = t2.name;
// 4) sub-select -- SELECT * FROM (SELECT 1, 2, 3) AS t1;
type Source struct {
	*TaskBase
	p          *plan.Source
	Scanner    schema.ConnScanner
	ExecSource ExecutorSource
	JoinKey    KeyEvaluator
	closed     bool
}

// NewSource create a scanner to read from data source
func NewSource(ctx *plan.Context, p *plan.Source) (*Source, error) {

	if p.Stmt == nil {
		return nil, fmt.Errorf("must have from for Source")
	}
	if p.Conn == nil {
		return nil, fmt.Errorf("Must have existing connection on Plan")
	}

	scanner, hasScanner := p.Conn.(schema.ConnScanner)

	// Some sources require context so we seed it here
	if sourceContext, needsContext := p.Conn.(RequiresContext); needsContext {
		sourceContext.SetContext(ctx)
	}

	if !hasScanner {
		e, hasSourceExec := p.Conn.(ExecutorSource)
		if hasSourceExec {
			s := &Source{
				TaskBase:   NewTaskBase(ctx),
				ExecSource: e,
				p:          p,
			}
			return s, nil
		}
		u.Warnf("source %T does not implement datasource.Scanner", p.Conn)
		return nil, fmt.Errorf("%T Must Implement Scanner for %q", p.Conn, p.Stmt.String())
	}
	s := &Source{
		TaskBase: NewTaskBase(ctx),
		Scanner:  scanner,
		p:        p,
	}
	return s, nil
}

// NewSourceScanner A scanner to read from sub-query data source (join, sub-query, static)
func NewSourceScanner(ctx *plan.Context, p *plan.Source, scanner schema.ConnScanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase(ctx),
		Scanner:  scanner,
		p:        p,
	}
	return s
}

func (m *Source) Copy() *Source { return &Source{} }

func (m *Source) closeSource() error {
	m.Lock()
	defer m.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.Scanner != nil {
		if closer, ok := m.Scanner.(schema.Conn); ok {
			if err := closer.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Source) Close() error {
	if err := m.closeSource(); err != nil {
		// Still need to close base right?
		return err
	}
	return m.TaskBase.Close()
}

func (m *Source) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	if m.Scanner == nil {
		u.Warnf("no datasource configured?")
		return fmt.Errorf("No datasource found")
	}

	sigChan := m.SigChan()

	for item := m.Scanner.Next(); item != nil; item = m.Scanner.Next() {

		select {
		case <-sigChan:
			return nil
		case m.msgOutCh <- item:
			// continue
		}

	}
	return nil
}
