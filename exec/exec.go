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
type MessageChan chan datasource.Message
type MessageHandler func(ctx *Context, msg datasource.Message) bool
type Tasks []TaskRunner

// TaskRunner is an interface for single dependent task in Dag of
//  Tasks necessary to exec a query
type TaskRunner interface {
	Children() Tasks
	Type() string
	MessageIn() MessageChan
	MessageOut() MessageChan
	MessageInSet(MessageChan)
	MessageOutSet(MessageChan)
	SigChan() SigChan
	Run(ctx *Context) error
	Close() error
}

func (m *Tasks) Add(task TaskRunner) {
	u.Debugf("add task: %T", task)
	*m = append(*m, task)
}

type TaskBase struct {
	TaskType string
	Handler  MessageHandler
	msgInCh  MessageChan
	msgOutCh MessageChan
	sigCh    SigChan // notify of quit/stop
	input    TaskRunner
	output   TaskRunner
}

func NewTaskBase(taskType string) *TaskBase {
	return &TaskBase{
		msgOutCh: make(MessageChan, ItemDefaultChannelSize),
		sigCh:    make(SigChan, 1),
		TaskType: taskType,
	}
}

func (m *TaskBase) Children() Tasks              { return nil }
func (m *TaskBase) MessageIn() MessageChan       { return m.msgInCh }
func (m *TaskBase) MessageOut() MessageChan      { return m.msgOutCh }
func (m *TaskBase) MessageInSet(ch MessageChan)  { m.msgInCh = ch }
func (m *TaskBase) MessageOutSet(ch MessageChan) { m.msgOutCh = ch }
func (m *TaskBase) SigChan() SigChan             { return m.sigCh }
func (m *TaskBase) Type() string                 { return m.TaskType }
func (m *TaskBase) Close() error                 { return nil }

//func (m *TaskBase) New() TaskRunner         { return NewTaskBase() }

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

	//u.Infof("runner: %T inchan", m)
	if m.Handler == nil {
		return fmt.Errorf("Must have a handler to run base runner")
	}
	ok := true
	var msg datasource.Message
msgLoop:
	for ok {
		select {
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

	return nil
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
