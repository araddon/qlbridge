package exec

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

var _ = u.EMPTY

const (
	ItemDefaultChannelSize = 50
)

type SigChan chan bool
type ErrChan chan error
type MessageChan chan datasource.Message
type MessageHandler func(ctx *Context, msg datasource.Message) bool
type Tasks []TaskRunner

// TaskRunner is an interface for single dependent task in Dag of
//  Tasks necessary to execute a Job
// - it may have children tasks
// - it may be parallel, distributed, etc
type TaskRunner interface {
	Children() Tasks
	Type() string
	MessageIn() MessageChan
	MessageOut() MessageChan
	MessageInSet(MessageChan)
	MessageOutSet(MessageChan)
	ErrChan() ErrChan
	SigChan() SigChan
	Run(ctx *Context) error
	Close() error
}

// Add a child Task
func (m *Tasks) Add(task TaskRunner) {
	//u.Debugf("add task: %T", task)
	*m = append(*m, task)
}

type TaskBase struct {
	TaskType string
	Handler  MessageHandler
	msgInCh  MessageChan
	msgOutCh MessageChan
	errCh    ErrChan
	sigCh    SigChan // notify of quit/stop
	input    TaskRunner
	output   TaskRunner
	errors   []error
}

func NewTaskBase(taskType string) *TaskBase {
	return &TaskBase{
		msgOutCh: make(MessageChan, ItemDefaultChannelSize),
		sigCh:    make(SigChan, 1),
		errCh:    make(ErrChan, 10),
		TaskType: taskType,
		errors:   make([]error, 0),
	}
}

func (m *TaskBase) Children() Tasks              { return nil }
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
	return func(ctx *Context, msg datasource.Message) bool {
		select {
		case out <- msg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}

func (m *TaskBase) Run(ctx *Context) error {
	defer ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		close(m.msgOutCh) // closing output channels is the signal to stop
		//u.Warnf("close taskbase: %v", m.Type())
	}()

	//u.Debugf("TaskBase: %T inchan", m)
	if m.Handler == nil {
		u.Warnf("returning, no handler")
		return fmt.Errorf("Must have a handler to run base runner")
	}
	ok := true
	var err error
	var msg datasource.Message
msgLoop:
	for ok {
		select {
		case err = <-m.errCh:
			//m.errors = append(m.errors, err)
			break msgLoop
		case <-m.sigCh:
			break msgLoop
		default:
		}

		select {
		case msg, ok = <-m.msgInCh:
			if ok {
				//u.Debugf("sending to handler: %v %T  %+v", m.Type(), msg, msg)
				ok = m.Handler(ctx, msg)
			} else {
				//u.Warnf("Not ok?   shutting down")
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

func NewTaskStepper(taskType string) *TaskStepper {
	t := NewTaskBase(taskType)
	return &TaskStepper{t}
}

func (m *TaskStepper) Run(ctx *Context) error {
	defer ctx.Recover()     // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing output channels is the signal to stop

	u.Infof("runner: %T inchan", m)
	for {
		select {
		case <-m.sigCh:
			break
		}
	}
	u.Warnf("end of Runner")
	return nil
}
