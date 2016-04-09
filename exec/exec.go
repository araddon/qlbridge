// Exececution tasks and executor for DAG of plan tasks
// can be embedded and used, or extended using Executor interface.
package exec

import (
	"fmt"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	// Standard errors
	ErrShuttingDown     = fmt.Errorf("Received Shutdown Signal")
	ErrNotSupported     = fmt.Errorf("QLBridge: Not supported")
	ErrNotImplemented   = fmt.Errorf("QLBridge: Not implemented")
	ErrUnknownCommand   = fmt.Errorf("QLBridge: Unknown Command")
	ErrInternalError    = fmt.Errorf("QLBridge: Internal Error")
	ErrNoSchemaSelected = fmt.Errorf("No Schema Selected")
)

type (
	//Task channel types
	SigChan     chan bool
	ErrChan     chan error
	MessageChan chan schema.Message
	// Handle/Forward a message for this Task
	MessageHandler func(ctx *plan.Context, msg schema.Message) bool
)

type (
	// Job Factory
	JobMaker func(ctx *plan.Context) (Executor, error)

	// Job Runner is the main RunTime interface for running a SQL Job of tasks
	JobRunner interface {
		Setup() error
		Run() error
		Close() error
	}

	// exec Tasks are inherently DAG's of task's implementing Run(), Close() etc
	//  to allow them to be executeable
	Task interface {
		Run() error
		Close() error
		CloseFinal() error
		Children() []Task // children sub-tasks
		Add(Task) error   // Add a child to this dag
	}

	// TaskRunner is an interface for a single task in Dag of Tasks necessary to execute a Job
	// - it may have children tasks
	// - it may be parallel, distributed, etc
	TaskRunner interface {
		Task
		Setup(depth int) error
		MessageIn() MessageChan
		MessageOut() MessageChan
		MessageInSet(MessageChan)
		MessageOutSet(MessageChan)
		ErrChan() ErrChan
		SigChan() SigChan
		Quit()
	}
	TaskPrinter interface {
		PrintDag(depth int)
	}

	// Executor defines standard Walk() pattern to create a executeable task dag from a plan dag
	//
	// An implementation of WalkPlan() will be be able to execute/run a Statement
	//  - inproc:   ie, in process.  qlbridge/exec package implements a non-distributed query-planner
	//  - distributed:  ie, run this job across multiple servers
	//         dataux/planner implements a distributed query-planner
	//         that distributes/runs tasks across multiple nodes
	//
	Executor interface {
		NewTask(p plan.Task) Task
		WalkPlan(p plan.Task) (Task, error)
		WalkSelect(p *plan.Select) (Task, error)
		WalkInsert(p *plan.Insert) (Task, error)
		WalkUpsert(p *plan.Upsert) (Task, error)
		WalkUpdate(p *plan.Update) (Task, error)
		WalkDelete(p *plan.Delete) (Task, error)
		WalkCommand(p *plan.Command) (Task, error)
		WalkPreparedStatement(p *plan.PreparedStatement) (Task, error)

		// Child Tasks
		WalkSource(p *plan.Source) (Task, error)
		WalkJoin(p *plan.JoinMerge) (Task, error)
		WalkJoinKey(p *plan.JoinKey) (Task, error)
		WalkWhere(p *plan.Where) (Task, error)
		WalkHaving(p *plan.Having) (Task, error)
		WalkGroupBy(p *plan.GroupBy) (Task, error)
		WalkProjection(p *plan.Projection) (Task, error)
	}

	// Sources can often do their own execution-plan for sub-select statements
	//  ie mysql can do its own (select, projection) mongo, es can as well
	// - provide interface to allow passing down select planning to source
	ExecutorSource interface {
		// given our plan, turn that into a Task.
		WalkExecSource(p *plan.Source) (Task, error)
	}
)
