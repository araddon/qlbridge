package vm

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
type TaskRunner interface {
	Children() Tasks
	MessageIn() MessageChan
	MessageOut() MessageChan
	MessageInSet(MessageChan)
	MessageOutSet(MessageChan)
	SigChan() SigChan
	Run(ctx *Context) error
}

// type JobRunner interface {
// 	Run(ctx *Context) error
// }

func (m *Tasks) Add(task TaskRunner) {
	*m = append(*m, task)
}

type TaskBase struct {
	Handler  MessageHandler
	msgInCh  MessageChan
	msgOutCh MessageChan
	sigCh    SigChan // notify of quit/stop
	input    TaskRunner
	output   TaskRunner
}

func NewTaskBase() *TaskBase {
	return &TaskBase{
		msgOutCh: make(MessageChan, ItemDefaultChannelSize),
		sigCh:    make(SigChan, 1),
	}
}

func (m *TaskBase) Children() Tasks         { return nil }
func (m *TaskBase) MessageIn() MessageChan  { return m.msgInCh }
func (m *TaskBase) MessageOut() MessageChan { return m.msgOutCh }
func (m *TaskBase) MessageInSet(ch MessageChan) {
	m.msgInCh = ch
	u.Infof("setting in chan: %p", m.msgInCh)
}
func (m *TaskBase) MessageOutSet(ch MessageChan) { m.msgOutCh = ch }
func (m *TaskBase) SigChan() SigChan             { return m.sigCh }

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
	defer ctx.Recover()     // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing output channels is the signal to stop

	u.Infof("runner: %p inchan", m.msgInCh)
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
				u.Debugf("sending to handler: %T  %+v", msg, msg)
				ok = m.Handler(ctx, msg)
			}
		case <-m.sigCh:
			break msgLoop
		}
	}

	return nil
}
