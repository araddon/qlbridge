package exec

import (
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
)

var _ = u.EMPTY

type TaskSequential struct {
	*TaskBase
	tasks Tasks
}

func NewSequential(taskType string, tasks Tasks) *TaskSequential {
	baseTask := NewTaskBase(taskType)
	task := &TaskSequential{
		TaskBase: baseTask,
		tasks:    tasks,
	}
	return task
}

func (m *TaskSequential) Close() error {
	errs := make(errList, 0)
	for _, task := range m.tasks {
		if err := task.Close(); err != nil {
			errs.append(err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (m *TaskSequential) Setup() error {
	// We don't need to setup the First(source) Input channel
	for i := 1; i < len(m.tasks); i++ {
		m.tasks[i].MessageInSet(m.tasks[i-1].MessageOut())
		//u.Infof("set msg in: %s  %p", m.tasks[i].Type(), m.tasks[i].MessageIn())
	}
	return nil
}

func (m *TaskSequential) Add(task TaskRunner) error {
	m.tasks = append(m.tasks, task)
	if len(m.tasks) > 1 {
		i := len(m.tasks) - 1
		m.tasks[i].MessageInSet(m.tasks[i-1].MessageOut())
	}
	u.Infof("new task? %v  %T", len(m.tasks), task)
	return nil
}
func (m *TaskSequential) Children() Tasks { return m.tasks }

func (m *TaskSequential) Run(ctx *expr.Context) error {
	defer ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		close(m.msgOutCh) // closing output channels is the signal to stop
		//u.Warnf("close TaskSequential: %v", m.Type())
	}()

	// Either of the SigQuit, or error channel will
	//  cause breaking out of message channels below
	select {
	case err := <-m.errCh:
		u.Errorf("%v", err)
	case <-m.sigCh:
		u.Warnf("got quit channel?")
	default:
	}

	var wg sync.WaitGroup

	// start tasks in reverse order, so that by time
	// source starts up all downstreams have started
	//lastTaskId := len(m.tasks) - 1
	for i := len(m.tasks) - 1; i >= 0; i-- {
		//if i != lastTaskId {
		//u.Infof("wg.Add")
		wg.Add(1)
		//}
		go func(taskId int) {
			//u.Infof("starting task %v   %T", taskId, m.tasks[taskId])
			if err := m.tasks[taskId].Run(ctx); err != nil {
				u.Errorf("%T.Run() errored %v", m.tasks[taskId], err)
				// TODO:  what do we do with this error?   send to error channel?
			}
			//u.Warnf("exiting taskId: %v %T", taskId, m.tasks[taskId])
			//if taskId != lastTaskId {
			//u.Infof("wg done")
			wg.Done()
			//}
		}(i)
	}

	wg.Wait() // block until all tasks have finished
	//u.Warnf("nice, after wg.Wait()")
	return nil
}
