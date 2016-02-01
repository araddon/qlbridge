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
		plan.Task
		Type() string
		Setup(depth int) error
		MessageIn() MessageChan
		MessageOut() MessageChan
		MessageInSet(MessageChan)
		MessageOutSet(MessageChan)
		ErrChan() ErrChan
		SigChan() SigChan
	}

	// exec.Visitor defines standard Sql Visit() pattern to create
	//   a job executor from a plan
	//
	// An implementation of Visitor() will be be able to execute/run a Statement
	//  - inproc:   ie, in process
	//  - distributed:  ie, run this job across multiple servers
	//
	//         qlbridge/exec package implements a non-distributed query-planner
	//         dataux/planner implements a distributed query-planner
	//
	Executor interface {
		// DML Statements
		//VisitPreparedStatement(p *plan.PreparedStatement) (Task, plan.WalkStatus, error)
		WalkSelect(sp *plan.Select) (Task, error)
		// VisitInsert(p *plan.Insert) (Task, plan.WalkStatus, error)
		// VisitUpsert(p *plan.Upsert) (Task, plan.WalkStatus, error)
		// VisitUpdate(p *plan.Update) (Task, plan.WalkStatus, error)
		// VisitDelete(p *plan.Delete) (Task, plan.WalkStatus, error)
		// VisitInto(p *plan.Into) (Task, plan.WalkStatus, error)

		// DDL & server operations
		// VisitShow(p *plan.Show) (Task, plan.WalkStatus, error)
		// VisitDescribe(p *plan.Describe) (Task, plan.WalkStatus, error)
		// VisitCommand(p *plan.Command) (Task, plan.WalkStatus, error)

		// Select Sub Tasks
		// VisitWhere(p *Select) (Task, plan.WalkStatus, error)
		// VisitHaving(p *Select) (Task, plan.WalkStatus, error)
		// VisitGroupBy(p *Select) (Task, plan.WalkStatus, error)
		// VisitProjection(p *Select) (Task, plan.WalkStatus, error)
		// VisitMutateWhere(p *Where) (Task, plan.WalkStatus, error)
	}

	// Interface for sub-select Tasks of the Select Statement
	// SourceVisitor interface {
	// 	VisitSourceSelect(p *Source) (Task, plan.WalkStatus, error)
	// 	VisitSource(scanner schema.Scanner) (Task, plan.WalkStatus, error)
	// 	VisitSourceJoin(scanner schema.Scanner) (Task, plan.WalkStatus, error)
	// 	VisitWhere() (Task, plan.WalkStatus, error)
	// }
)
