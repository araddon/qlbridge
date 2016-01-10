package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY

	_ plan.ExecutionPlan = (*TaskRunners)(nil)
)

const (
	ItemDefaultChannelSize = 50
)

type SigChan chan bool
type ErrChan chan error
type MessageChan chan schema.Message

// Handle/Forward a message for this Task
//  TODO:  this bool is either wrong, or not-used?   error?
type MessageHandler func(ctx *plan.Context, msg schema.Message) bool

// TaskRunner is an interface for single dependent task in Dag of
//  Tasks necessary to execute a Job
// - it may have children tasks
// - it may be parallel, distributed, etc
type TaskRunner interface {
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

type TaskRunners struct {
	ctx     *plan.Context
	tasks   []plan.Task
	runners []TaskRunner
}

func TaskRunnersMaker(ctx *plan.Context) plan.ExecutionPlan {
	return &TaskRunners{
		ctx:     ctx,
		tasks:   make([]plan.Task, 0),
		runners: make([]TaskRunner, 0),
	}
}

func (m *TaskRunners) Close() error          { return nil }
func (m *TaskRunners) Run() error            { return nil }
func (m *TaskRunners) Children() []plan.Task { return m.tasks }
func (m *TaskRunners) Add(task plan.Task) error {
	tr, ok := task.(TaskRunner)
	if !ok {
		panic(fmt.Sprintf("must be taskrunner %T", task))
	}
	m.tasks = append(m.tasks, task)
	m.runners = append(m.runners, tr)
	return nil
}
func (m *TaskRunners) Sequential(name string) plan.Task {
	return NewSequential(m.ctx, name, m)
}
func (m *TaskRunners) Parallel(name string) plan.Task {
	return NewTaskParallel(m.ctx, name, m.tasks)
}

type TaskBase struct {
	depth    int
	setup    bool
	TaskType string
	Ctx      *plan.Context
	Handler  MessageHandler
	msgInCh  MessageChan
	msgOutCh MessageChan
	errCh    ErrChan
	sigCh    SigChan // notify of quit/stop
	errors   []error
}

func NewTaskBase(ctx *plan.Context, taskType string) *TaskBase {
	return &TaskBase{
		// All Tasks Get output channels by default, but NOT input
		msgOutCh: make(MessageChan, ItemDefaultChannelSize),
		sigCh:    make(SigChan, 1),
		errCh:    make(ErrChan, 10),
		TaskType: taskType,
		errors:   make([]error, 0),
		Ctx:      ctx,
	}
}

func (m *TaskBase) Children() []plan.Task { return nil }
func (m *TaskBase) Setup(depth int) error {
	m.depth = depth
	m.setup = true
	//u.Debugf("setup() %s %T in:%p  out:%p", m.TaskType, m, m.msgInCh, m.msgOutCh)
	return nil
}
func (m *TaskBase) Add(task plan.Task) error     { return fmt.Errorf("This is not a list-type task %T", m) }
func (m *TaskBase) MessageIn() MessageChan       { return m.msgInCh }
func (m *TaskBase) MessageOut() MessageChan      { return m.msgOutCh }
func (m *TaskBase) MessageInSet(ch MessageChan)  { m.msgInCh = ch }
func (m *TaskBase) MessageOutSet(ch MessageChan) { m.msgOutCh = ch }
func (m *TaskBase) ErrChan() ErrChan             { return m.errCh }
func (m *TaskBase) SigChan() SigChan             { return m.sigCh }
func (m *TaskBase) Type() string                 { return m.TaskType }
func (m *TaskBase) Close() error                 { return nil }

func MakeHandler(task TaskRunner) MessageHandler {
	out := task.MessageOut()
	return func(ctx *plan.Context, msg schema.Message) bool {
		select {
		case out <- msg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}

func (m *TaskBase) Run() error {
	defer m.Ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		close(m.msgOutCh) // closing output channels is the signal to stop
		//u.Debugf("close taskbase: ch:%p    %v", m.msgOutCh, m.Type())
	}()

	//u.Debugf("TaskBase: %T inchan", m)
	if m.Handler == nil {
		u.Warnf("returning, no handler %T", m)
		return fmt.Errorf("Must have a handler to run base runner")
	}
	ok := true
	var err error
	var msg datasource.Message
msgLoop:
	for ok {

		// Either of the SigQuit, or error channel will
		//  cause breaking out of message channels below
		select {
		case err = <-m.errCh:
			//m.errors = append(m.errors, err)
			break msgLoop
		case <-m.sigCh: // Signal, ie quit etc
			u.Debugf("got taskbase sig")
			break msgLoop
		default:
		}

		//
		select {
		case msg, ok = <-m.msgInCh:
			if ok {
				//u.Debugf("sending to handler: %v %T  %+v", m.Type(), msg, msg)
				m.Handler(m.Ctx, msg)
			} else {
				//u.Debugf("msg in closed shutting down: %s", m.TaskType)
				break msgLoop
			}
		case <-m.sigCh:
			break msgLoop
		}
	}

	return err
}

// On Task stepper we don't Run it, rather use a
//   Next() explicit call from end user
type TaskStepper struct {
	*TaskBase
}

func NewTaskStepper(ctx *plan.Context, taskType string) *TaskStepper {
	t := NewTaskBase(ctx, taskType)
	return &TaskStepper{t}
}

func (m *TaskStepper) Run() error {
	defer m.Ctx.Recover()   // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing output channels is the signal to stop

	//u.Infof("runner: %T inchan", m)
	for {
		select {
		case <-m.sigCh:
			break
		}
	}
	//u.Warnf("end of Runner")
	return nil
}
