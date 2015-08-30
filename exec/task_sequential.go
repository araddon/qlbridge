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
	return &TaskSequential{
		TaskBase: baseTask,
		tasks:    tasks,
	}
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
func (m *TaskSequential) Add(task TaskRunner) error {
	m.tasks = append(m.tasks, task)
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

	// We don't need to setup the First(source) Input channel
	for i := 1; i < len(m.tasks); i++ {
		m.tasks[i].MessageInSet(m.tasks[i-1].MessageOut())
	}

	var wg sync.WaitGroup

	// start tasks in reverse order, so that by time
	// source starts up all downstreams have started
	for i := len(m.tasks) - 1; i >= 0; i-- {

		wg.Add(1)
		go func(taskId int) {
			u.Infof("starting task %v   %T", taskId, m.tasks[taskId])
			if err := m.tasks[taskId].Run(ctx); err != nil {
				u.Errorf("%T.Run() errored %v", m.tasks[taskId], err)
				// TODO:  what do we do with this error?   send to error channel?
			}
			u.Warnf("exiting taskId: %v %T", taskId, m.tasks[taskId])
			wg.Done()
		}(i)
	}

	wg.Wait()
	return nil
}
