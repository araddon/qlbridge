package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY
)

const (
	ItemDefaultChannelSize = 50
)

type TaskBase struct {
	Ctx      *plan.Context
	Handler  MessageHandler
	depth    int
	setup    bool
	msgInCh  MessageChan
	msgOutCh MessageChan
	errCh    ErrChan
	sigCh    SigChan // notify of quit/stop
	errors   []error

	// Temporary, making plan.Task,exec.Task compatible, remove me please
	parallel bool
}

func NewTaskBase(ctx *plan.Context) *TaskBase {
	return &TaskBase{
		// All Tasks Get output channels by default, but NOT input
		msgOutCh: make(MessageChan, ItemDefaultChannelSize),
		sigCh:    make(SigChan, 1),
		errCh:    make(ErrChan, 10),
		errors:   make([]error, 0),
		Ctx:      ctx,
	}
}

/// TEMP----------------------------------------------------------
func (m *TaskBase) IsParallel() bool                                 { return m.parallel }
func (m *TaskBase) IsSequential() bool                               { return !m.parallel }
func (m *TaskBase) SetParallel()                                     { m.parallel = true }
func (m *TaskBase) SetSequential()                                   { m.parallel = false }
func (m *TaskBase) Walk(plan.Planner) error                          { panic("not implemented") }
func (m *TaskBase) WalkStatus(plan.Planner) (plan.WalkStatus, error) { panic("not implemented") }

//  //------- TEMP

func (m *TaskBase) Children() []Task { return nil }
func (m *TaskBase) Setup(depth int) error {
	m.depth = depth
	m.setup = true
	//u.Debugf("setup() %s %T in:%p  out:%p", m.TaskType, m, m.msgInCh, m.msgOutCh)
	return nil
}
func (m *TaskBase) Add(task Task) error { return fmt.Errorf("This is not a list-type task %T", m) }
func (m *TaskBase) AddPlan(task plan.Task) error {
	return fmt.Errorf("This is not a list-type task %T", m)
}
func (m *TaskBase) MessageIn() MessageChan       { return m.msgInCh }
func (m *TaskBase) MessageOut() MessageChan      { return m.msgOutCh }
func (m *TaskBase) MessageInSet(ch MessageChan)  { m.msgInCh = ch }
func (m *TaskBase) MessageOutSet(ch MessageChan) { m.msgOutCh = ch }
func (m *TaskBase) ErrChan() ErrChan             { return m.errCh }
func (m *TaskBase) SigChan() SigChan             { return m.sigCh }
func (m *TaskBase) Close() error {
	if m == nil {
		u.LogTraceDf(u.WARN, 12, "")
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("panic in close %v", r)
		}
	}()
	//u.Debugf("got close? %#v", m)
	close(m.sigCh)
	return nil
}

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
	var msg schema.Message
msgLoop:
	for ok {

		// Either of the SigQuit, or error channel will
		//  cause breaking out of message channels below
		select {
		case err = <-m.errCh:
			//m.errors = append(m.errors, err)
			break msgLoop
		case <-m.sigCh: // Signal, ie quit etc
			u.Debugf("got taskbase signal")
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

func NewTaskStepper(ctx *plan.Context) *TaskStepper {
	t := NewTaskBase(ctx)
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
