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
	}

	// Executor defines standard Sql Walk() pattern to create a runnable task from a plan
	//
	// An implementation of WalkPlan() will be be able to execute/run a Statement
	//  - inproc:   ie, in process
	//  - distributed:  ie, run this job across multiple servers
	//
	//         qlbridge/exec package implements a non-distributed query-planner
	//         dataux/planner implements a distributed query-planner
	//
	Executor interface {
		WalkPlan(p plan.Task) (Task, error)
	}
)
