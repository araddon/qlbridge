package exec

import (
	"fmt"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the plan.Tasks
	_ plan.Task = (*TaskSequential)(nil)
)

type TaskSequential struct {
	*TaskBase
	tasks   []plan.Task
	runners []TaskRunner
}

func NewSequential(ctx *plan.Context, taskType string) *TaskSequential {
	baseTask := NewTaskBase(ctx, taskType)
	st := &TaskSequential{
		TaskBase: baseTask,
		tasks:    make([]plan.Task, 0),
		runners:  make([]TaskRunner, 0),
	}
	return st
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

func (m *TaskSequential) Setup(depth int) error {
	// We don't need to setup the First(source) Input channel
	m.depth = depth
	m.setup = true
	for i := 0; i < len(m.runners); i++ {
		//u.Debugf("%d i:%d  Setup: %T p:%p", depth, i, m.tasks[i], m.tasks[i])
		if err := m.runners[i].Setup(depth + 1); err != nil {
			return err
		}
	}
	//u.Infof("%d  TaskSequential Setup  tasks len=%d", depth, len(m.tasks))
	for i := 1; i < len(m.runners); i++ {
		m.runners[i].MessageInSet(m.runners[i-1].MessageOut())
		//u.Infof("%d-%d setup msgin: %s  %p", depth, i, m.tasks[i].Type(), m.tasks[i].MessageIn())
	}
	if depth > 0 {
		m.TaskBase.MessageOutSet(m.runners[len(m.tasks)-1].MessageOut())
		m.runners[0].MessageInSet(m.TaskBase.MessageIn())
	}
	//u.Debugf("setup() %s %T in:%p  out:%p", m.TaskType, m, m.msgInCh, m.msgOutCh)
	return nil
}

func (m *TaskSequential) Add(task plan.Task) error {
	if m.setup {
		return fmt.Errorf("Cannot add task after Setup() called")
	}
	tr, ok := task.(TaskRunner)
	if !ok {
		panic(fmt.Sprintf("must be taskrunner %T", task))
	}
	m.tasks = append(m.tasks, task)
	m.runners = append(m.runners, tr)
	return nil
}

func (m *TaskSequential) Children() []plan.Task { return m.tasks }

func (m *TaskSequential) Run() error {
	defer m.Ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		//close(m.msgOutCh) // closing output channels is the signal to stop
		//u.Debugf("close TaskSequential: %v", m.Type())
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
	for i := len(m.tasks) - 1; i >= 0; i-- {
		wg.Add(1)
		go func(taskId int) {
			task := m.tasks[taskId]
			//u.Infof("starting task %d-%d %T in:%p  out:%p", m.depth, taskId, task, task.MessageIn(), task.MessageOut())
			if err := task.Run(); err != nil {
				u.Errorf("%T.Run() errored %v", task, err)
				// TODO:  what do we do with this error?   send to error channel?
			}
			//u.Warnf("exiting taskId: %v %T", taskId, m.tasks[taskId])
			wg.Done()
		}(i)
	}

	wg.Wait() // block until all tasks have finished
	//u.Debugf("exit TaskSequential Run()")
	return nil
}
