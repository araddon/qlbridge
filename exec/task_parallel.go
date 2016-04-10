package exec

import (
	"fmt"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Tasks
	_ Task = (*TaskParallel)(nil)
)

// A parallel set of tasks, this starts each child task and offers up
//   an output channel that is a merger of each child
//
//  --> \
//  --> - ->
//  --> /
type TaskParallel struct {
	*TaskBase
	in      TaskRunner
	runners []TaskRunner
	tasks   []Task
}

func NewTaskParallel(ctx *plan.Context) *TaskParallel {
	return &TaskParallel{
		TaskBase: NewTaskBase(ctx),
		runners:  make([]TaskRunner, 0),
		tasks:    make([]Task, 0),
	}
}

func (m *TaskParallel) PrintDag(depth int) {

	prefix := ""
	for i := 0; i < depth; i++ {
		prefix += "\t"
	}
	for i := 0; i < len(m.runners); i++ {
		t := m.runners[i]
		switch tt := t.(type) {
		case TaskPrinter:
			u.Warnf("%s%d %p task i:%v %T", prefix, depth, m, i, t)
			tt.PrintDag(depth + 1)
		default:
			u.Warnf("%s%d %p task i:%v %T", prefix, depth, m, i, t)
		}
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
	return m.TaskBase.Close()
}

func (m *TaskParallel) Setup(depth int) error {
	m.setup = true
	if m.in != nil {
		for _, task := range m.runners {
			task.MessageInSet(m.in.MessageOut())
			//u.Infof("parallel task in: #%d task p:%p %T  %p", i, task, task, task.MessageIn())
		}
	}
	for _, task := range m.runners {
		task.MessageOutSet(m.msgOutCh)
	}
	for i := 0; i < len(m.runners); i++ {
		//u.Debugf("%d  Setup: %T", depth, m.runners[i])
		if err := m.runners[i].Setup(depth + 1); err != nil {
			return err
		}
	}
	return nil
}

func (m *TaskParallel) Add(task Task) error {
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

func (m *TaskParallel) Children() []Task { return m.tasks }

func (m *TaskParallel) Run() error {
	defer m.Ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		// TODO:  find the culprit
		defer func() {
			if r := recover(); r != nil {
				//u.Errorf("panic on:  %v", r)
			}
		}()
		//u.WarnT(8)
		close(m.msgOutCh) // closing output channels is the signal to stop
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
	for i := len(m.runners) - 1; i >= 0; i-- {
		wg.Add(1)
		go func(taskId int) {
			task := m.runners[taskId]
			//u.Infof("starting task %d-%d %T in:%p  out:%p", m.depth, taskId, task, task.MessageIn(), task.MessageOut())
			if err := task.Run(); err != nil {
				u.Errorf("%T.Run() errored %v", task, err)
				// TODO:  what do we do with this error?   send to error channel?
			}
			//u.Debugf("exiting taskId: %v %T", taskId, task)
			wg.Done()
		}(i)
	}

	wg.Wait()

	return nil
}
