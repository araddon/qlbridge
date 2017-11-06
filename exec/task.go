package exec

import (
	"context"
	"fmt"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

const (
	// ItemDefaultChannelSize default channel buffer for task's
	ItemDefaultChannelSize = 50
)

// TaskBase Base executeable task that implements Task interface, embedded
// into other channel based task runners
type TaskBase struct {
	sync.Mutex
	Ctx      *plan.Context
	Name     string
	Handler  MessageHandler
	depth    int
	setup    bool
	closed   bool
	hasquit  bool
	msgInCh  MessageChan
	msgOutCh MessageChan
	errCh    ErrChan
	sigCh    SigChan // notify of quit/stop
	errors   []error
}

func NewTaskBase(ctx *plan.Context) *TaskBase {
	if ctx.Context == nil {
		ctx.Context = context.Background()
	}
	return &TaskBase{
		// All Tasks Get output channels by default, but NOT input
		msgOutCh: make(MessageChan, ItemDefaultChannelSize),
		sigCh:    make(SigChan, 1),
		errCh:    make(ErrChan, 10),
		errors:   make([]error, 0),
		Ctx:      ctx,
	}
}

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
func (m *TaskBase) Quit() {
	if m.hasquit {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("Error on closing sigchannel %v", r)
		}
	}()
	m.hasquit = true
	close(m.sigCh)
}
func (m *TaskBase) Close() error {
	//u.Debugf("%p start Close()", m)
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("panic in close %v", r)
		}
	}()
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()
	//u.Debugf("%p finished Close()", m)
	close(m.sigCh)
	return nil
}
func (m *TaskBase) CloseFinal() error { return nil }

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
			//u.Debugf("got taskbase signal")
			break msgLoop
		default:
		}

		//
		select {
		case msg, ok = <-m.msgInCh:
			if ok {
				//u.Debugf("sending to handler: %T  %+v", msg, msg)
				m.Handler(m.Ctx, msg)
			} else {
				//u.Debugf("msg in closed shutting down")
				break msgLoop
			}
		case <-m.sigCh:
			break msgLoop
		}
	}

	//u.Warnf("exiting")
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

	for {
		select {
		case <-m.sigCh:
			return nil
		}
	}
}
