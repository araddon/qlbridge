package exec

import (
	"fmt"
	"strings"
	"sync"

	"database/sql/driver"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
)

var (
	ShuttingDownError = fmt.Errorf("Received Shutdown Signal")

	// SqlJob implements JobRunner
	_ JobRunner = (*SqlJob)(nil)
)

// Job Runner is the main RunTime interface for running a SQL Job
type JobRunner interface {
	Setup() error
	Run() error
	Close() error
}

type Context struct {
	errRecover interface{}
	id         string
	prefix     string
}

func (m *Context) Recover() {
	if r := recover(); r != nil {
		u.Errorf("context recover: %v", r)
		m.errRecover = r
	}
}

type SqlJob struct {
	Tasks Tasks
	Stmt  expr.SqlStatement
}

func (m *SqlJob) Setup() error {
	return SetupTasks(m.Tasks)
}

func (m *SqlJob) Run() error {
	return RunJob(m.Tasks)
}

func (m *SqlJob) Close() error {
	errs := make(errList, 0)
	for _, task := range m.Tasks {
		if err := task.Close(); err != nil {
			errs.append(err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// The drain is the last out channel, on last task
func (m *SqlJob) DrainChan() MessageChan {
	return m.Tasks[len(m.Tasks)-1].MessageOut()
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlJob(rtConf *RuntimeConfig, connInfo, sqlText string) (*SqlJob, error) {

	stmt, err := expr.ParseSqlVm(sqlText)
	if err != nil {
		return nil, err
	}

	builder := NewJobBuilder(rtConf, connInfo)
	ex, err := stmt.Accept(builder)

	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, fmt.Errorf("No job runner? %v", sqlText)
	}
	tasks, ok := ex.(Tasks)
	if !ok {
		return nil, fmt.Errorf("expected tasks but got: %T", ex)
	}
	return &SqlJob{tasks, stmt}, nil
}

func SetupTasks(tasks Tasks) error {

	// We don't need to setup the First(source) Input channel
	for i := 1; i < len(tasks); i++ {
		tasks[i].MessageInSet(tasks[i-1].MessageOut())
	}

	// for i, task := range tasks {
	// 	u.Infof("set message in: %v %T  in:%p out:%p", i, task, task.MessageIn(), task.MessageOut())
	// }

	return nil
}

func RunJob(tasks Tasks) error {

	//u.Debugf("in RunJob exec %v", len(tasks))
	ctx := new(Context)

	var wg sync.WaitGroup

	// start tasks in reverse order, so that by time
	// source starts up all downstreams have started
	for i := len(tasks) - 1; i >= 0; i-- {
		wg.Add(1)
		go func(taskId int) {
			tasks[taskId].Run(ctx)
			//u.Warnf("exiting taskId: %v %T", taskId, tasks[taskId])
			wg.Done()
		}(i)
	}

	wg.Wait()
	u.Infof("RunJob(tasks) is completing")

	return nil
}

// Create a multiple error type
type errList []error

func (e *errList) append(err error) {
	if err != nil {
		*e = append(*e, err)
	}
}

func (e errList) error() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

func (e errList) Error() string {
	a := make([]string, len(e))
	for i, v := range e {
		a[i] = v.Error()
	}
	return strings.Join(a, "\n")
}

func params(args []driver.Value) []interface{} {
	r := make([]interface{}, len(args))
	for i, v := range args {
		r[i] = interface{}(v)
	}
	return r
}
