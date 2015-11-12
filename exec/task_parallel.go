package exec

import (
	"fmt"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
)

var _ = u.EMPTY

// A parallel set of tasks, this starts each child task and offers up
//   an output channel that is a merger of each child
//
//  --> \
//  --> - ->
//  --> /
type TaskParallel struct {
	*TaskBase
	in    TaskRunner
	tasks Tasks
}

func NewTaskParallel(taskType string, input TaskRunner, tasks Tasks) *TaskParallel {
	baseTask := NewTaskBase(taskType)
	return &TaskParallel{
		TaskBase: baseTask,
		tasks:    tasks,
		in:       input,
	}
}

func (m *TaskParallel) Close() error {
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

func (m *TaskParallel) Setup(depth int) error {
	m.setup = true
	if m.in != nil {
		for _, task := range m.tasks {
			task.MessageInSet(m.in.MessageOut())
			//u.Infof("parallel task in: #%d task p:%p %s  %p", i, task, task.Type(), task.MessageIn())
		}
	}
	for _, task := range m.tasks {
		task.MessageOutSet(m.msgOutCh)
	}
	for i := 0; i < len(m.tasks); i++ {
		//u.Debugf("%d  Setup: %T", depth, m.tasks[i])
		if err := m.tasks[i].Setup(depth + 1); err != nil {
			return err
		}
	}
	return nil
}

func (m *TaskParallel) Add(task TaskRunner) error {
	if m.setup {
		return fmt.Errorf("Cannot add task after Setup() called")
	}
	m.tasks = append(m.tasks, task)
	return nil
}

func (m *TaskParallel) Children() Tasks { return m.tasks }

func (m *TaskParallel) Run(ctx *expr.Context) error {
	defer ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		close(m.msgOutCh) // closing output channels is the signal to stop
		u.Debugf("close TaskParallel: %v", m.Type())
	}()

	// Either of the SigQuit, or error channel will
	//  cause breaking out of message channels below
	select {
	case err := <-m.errCh:
		//m.errors = append(m.errors, err)
		u.Errorf("%v", err)
	case <-m.sigCh:

	default:
	}

	var wg sync.WaitGroup

	// start tasks in reverse order, so that by time
	// source starts up all downstreams have started
	for i := len(m.tasks) - 1; i >= 0; i-- {
		wg.Add(1)
		go func(taskId int) {
			if err := m.tasks[taskId].Run(ctx); err != nil {
				u.Errorf("%T.Run() errored %v", m.tasks[taskId], err)
				// TODO:  what do we do with this error?   send to error channel?
			}
			//u.Debugf("exiting taskId: %v %T", taskId, m.tasks[taskId])
			wg.Done()
		}(i)
	}

	wg.Wait()

	return nil
}
