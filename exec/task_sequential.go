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
	_ Task = (*TaskSequential)(nil)
)

type TaskSequential struct {
	*TaskBase
	closed  bool
	tasks   []Task
	runners []TaskRunner
}

func NewTaskSequential(ctx *plan.Context) *TaskSequential {
	st := &TaskSequential{
		TaskBase: NewTaskBase(ctx),
		tasks:    make([]Task, 0),
		runners:  make([]TaskRunner, 0),
	}
	return st
}

func (m *TaskSequential) PrintDag(depth int) {
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

func (m *TaskSequential) Close() error {
	//u.Debugf("%p start Close() closed?%v", m, m.closed)
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()

	errs := make(errList, 0)
	for _, task := range m.tasks {
		//u.Debugf("%p task.Close()  %T", task, task)
		if err := task.Close(); err != nil {
			errs.append(err)
		}
	}
	for _, task := range m.tasks {
		if err := task.CloseFinal(); err != nil {
			errs.append(err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return m.TaskBase.Close()
}

func (m *TaskSequential) Setup(depth int) error {
	// We don't need to setup the First(source) Input channel
	m.depth = depth
	m.setup = true
	for i := 0; i < len(m.runners); i++ {
		//u.Debugf("%d i:%d  Setup: %T p:%p", depth, i, m.runners[i], m.runners[i])
		if err := m.runners[i].Setup(depth + 1); err != nil {
			return err
		}
	}
	//u.Infof("%d  TaskSequential Setup  tasks len=%d", depth, len(m.tasks))
	for i := 1; i < len(m.runners); i++ {
		m.runners[i].MessageInSet(m.runners[i-1].MessageOut())
		//u.Infof("%d-%d setup msgin: %T  %p", depth, i, m.runners[i], m.runners[i].MessageIn())
	}
	if depth > 0 {
		m.TaskBase.MessageOutSet(m.runners[len(m.tasks)-1].MessageOut())
		m.runners[0].MessageInSet(m.TaskBase.MessageIn())
	}
	//u.Debugf("setup() %T in:%p  out:%p", m, m.msgInCh, m.msgOutCh)
	return nil
}

func (m *TaskSequential) Add(task Task) error {
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

func (m *TaskSequential) Children() []Task { return m.tasks }

func (m *TaskSequential) Run() (err error) {
	defer m.Ctx.Recover() // Our context can recover panics, save error msg
	defer func() {
		//close(m.msgOutCh) // closing output channels is the signal to stop
		//u.Debugf("close TaskSequential: %v", m.Type())
	}()

	var wg sync.WaitGroup

	// Either of the SigQuit, or error channel will
	//  cause breaking out of task execution below
	// go func() {
	// 	select {
	// 	case err := <-m.errCh:
	// 		u.Errorf("error on run %v", err)
	// 	case <-m.sigCh:
	// 		u.Warnf("%p %q got quit channel?", m, m.Name)
	// 		// If we close here, we close without draining not giving messaging time
	// 		// so we should????
	// 		//err = m.Close()
	// 		// for _, task := range m.runners {
	// 		// 	task.Quit()
	// 		// }
	// 	}
	// }()

	// start tasks in reverse order, so that by time
	// source starts up all downstreams have started
	for i := len(m.runners) - 1; i >= 0; i-- {
		wg.Add(1)
		go func(taskId int) {
			task := m.runners[taskId]
			//u.Infof("starting task %d-%d %T in:%p  out:%p", m.depth, taskId, task, task.MessageIn(), task.MessageOut())
			if taskErr := task.Run(); taskErr != nil {
				u.Errorf("%T.Run() errored %v", task, taskErr)
				// TODO:  what do we do with this error?   send to error channel?
				err = taskErr
				m.errors = append(m.errors, taskErr)
			}
			//u.Debugf("%p %q exiting taskId: %p %v %T", m, m.Name, task, taskId, task)
			wg.Done()
			// Lets look for the last task to shutdown, the result-writer or projection
			// will finish first on limit so we need to shutdown sources
			if len(m.runners)-1 == taskId {
				//u.Warnf("%p got shutdown on last one, lets shutdown them all", m)
				for i := len(m.runners) - 2; i >= 0; i-- {
					//u.Debugf("%p sending close??: %v %T", m, i, m.runners[i])
					m.runners[i].Close()
					//u.Debugf("%p after close??: %v %T", m, i, m.runners[i])
				}
			}
		}(i)
	}

	wg.Wait() // block until all tasks have finished
	//u.Debugf("%p exit TaskSequential Run():  %q", m, m.Name)
	return
}
