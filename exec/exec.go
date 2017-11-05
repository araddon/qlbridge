// Package exec contains execution tasks to run each of the separate tasks
// (Source, Project, Where, Having, etc) of a SQL data of tasks.  It does
// by defining interface, and base tasks, and a single-machine channel
// oriented DAG runner (Executor).  The Executor interface allows
// other downstreams to implement a multi-node message passing oriented
// version while re-using most Tasks.
package exec

import (
	"fmt"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ErrShuttingDown already shutting down error
	ErrShuttingDown = fmt.Errorf("Received Shutdown Signal")
	// ErrNotSupported statement not supported
	ErrNotSupported = fmt.Errorf("QLBridge: Not supported")
	// ErrNotImplemented exec not impelemented for statement
	ErrNotImplemented = fmt.Errorf("QLBridge: Not implemented")
	// ErrUnknownCommand unknown command error.
	ErrUnknownCommand = fmt.Errorf("QLBridge: Unknown Command")
	// ErrInternalError internal error
	ErrInternalError = fmt.Errorf("QLBridge: Internal Error")
	// ErrNoSchemaSelected no schema was selected when performing statement.
	ErrNoSchemaSelected = fmt.Errorf("No Schema Selected")
)

type (
	// SigChan is signal channel for shutdown etc
	SigChan chan bool
	// ErrChan error channel
	ErrChan chan error
	// MessageChan message channel
	MessageChan chan schema.Message
	// MessageHandler Handle/Forward a message for this Task
	MessageHandler func(ctx *plan.Context, msg schema.Message) bool
)

type (
	// JobMaker Job Factory
	JobMaker func(ctx *plan.Context) (Executor, error)

	// JobRunner is the main RunTime interface for running a SQL Job of tasks
	JobRunner interface {
		Setup() error
		Run() error
		Close() error
	}

	// Task exec Tasks are inherently DAG's of task's implementing Run(), Close() etc
	// to allow them to be executeable
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
	// TaskPrinter a debug printer for dag-shape.
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

		// DML Statements
		WalkSelect(p *plan.Select) (Task, error)
		WalkInsert(p *plan.Insert) (Task, error)
		WalkUpsert(p *plan.Upsert) (Task, error)
		WalkUpdate(p *plan.Update) (Task, error)
		WalkDelete(p *plan.Delete) (Task, error)
		// DML Child Tasks
		WalkSource(p *plan.Source) (Task, error)
		WalkJoin(p *plan.JoinMerge) (Task, error)
		WalkJoinKey(p *plan.JoinKey) (Task, error)
		WalkWhere(p *plan.Where) (Task, error)
		WalkHaving(p *plan.Having) (Task, error)
		WalkGroupBy(p *plan.GroupBy) (Task, error)
		WalkOrder(p *plan.Order) (Task, error)
		WalkProjection(p *plan.Projection) (Task, error)
		// Other Statements
		WalkCommand(p *plan.Command) (Task, error)
		WalkPreparedStatement(p *plan.PreparedStatement) (Task, error)
		// DDL Tasks
		WalkCreate(p *plan.Create) (Task, error)
		WalkDrop(p *plan.Drop) (Task, error)
		WalkAlter(p *plan.Alter) (Task, error)
	}

	// ExecutorSource Sources can often do their own execution-plan for sub-select statements
	// ie mysql can do its own (select, projection) mongo, es can as well
	// - provide interface to allow passing down select planning to source
	ExecutorSource interface {
		// WalkExecSource given our plan, turn that into a Task.
		WalkExecSource(p *plan.Source) (Task, error)
	}
)
