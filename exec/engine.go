package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	// Standard errors
	ErrShuttingDown     = fmt.Errorf("Received Shutdown Signal")
	ErrNotSupported     = fmt.Errorf("QLBridge: Not supported")
	ErrNotImplemented   = fmt.Errorf("QLBridge: Not implemented")
	ErrUnknownCommand   = fmt.Errorf("QLBridge: Unknown Command")
	ErrInternalError    = fmt.Errorf("QLBridge: Internal Error")
	ErrNoSchemaSelected = fmt.Errorf("No Schema Selected")
	// SqlJob implements JobRunner
	_ JobRunner = (*SqlJob)(nil)

	_ = u.EMPTY
)

// Job Runner is the main RunTime interface for running a SQL Job
type JobRunner interface {
	Setup() error
	Run() error
	Close() error
}

// SqlJob is dag of tasks for sql execution
type SqlJob struct {
	RootTask   TaskRunner
	Stmt       expr.SqlStatement
	Schema     *datasource.Schema
	Projection *expr.Projection
	Conf       *datasource.RuntimeSchema
}

func (m *SqlJob) Setup() error {
	return m.RootTask.Setup(0)
}

func (m *SqlJob) Run() error {
	ctx := expr.NewContext()
	ctx.DisableRecover = m.Conf.DisableRecover
	return m.RootTask.Run(ctx)
}

func (m *SqlJob) Close() error {
	return m.RootTask.Close()
}

// The drain is the last out channel, on last task
func (m *SqlJob) DrainChan() MessageChan {
	tasks := m.RootTask.Children()
	return tasks[len(tasks)-1].MessageOut()
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlProjectedJob(conf *datasource.RuntimeSchema, connInfo, sqlText string) (*SqlJob, error) {

	job, err := BuildSqlJob(conf, connInfo, sqlText)
	if err != nil {
		return job, err
	}
	if job.Projection != nil {
		//u.Warnf("return job.proj: %p", job.Projection)
		return job, nil
	}
	if sqlSelect, ok := job.Stmt.(*expr.SqlSelect); ok {
		job.Projection, err = NewExprProjection(conf, sqlSelect)
		if err != nil {
			return nil, err
		}
		// if err = createProjection(job, sqlSelect); err != nil {
		// 	return nil, err
		// }
	}
	return job, nil
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlJob(conf *datasource.RuntimeSchema, connInfo, sqlText string) (*SqlJob, error) {

	stmt, err := expr.ParseSqlVm(sqlText)
	if err != nil {
		return nil, err
	}

	builder := NewJobBuilder(conf, connInfo)
	task, _, err := stmt.Accept(builder)

	//u.Debugf("build sqljob.proj: %p", builder.Projection)

	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("No job runner? %v", sqlText)
	}
	taskRunner, ok := task.(TaskRunner)
	if !ok {
		return nil, fmt.Errorf("Must be taskrunner but was %T", task)
	}
	return &SqlJob{
		RootTask:   taskRunner,
		Projection: builder.Projection,
		Stmt:       stmt,
		Conf:       conf,
	}, nil
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
