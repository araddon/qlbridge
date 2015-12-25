package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
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
	RootTask TaskRunner
	Ctx      *plan.Context
}

func (m *SqlJob) Setup() error {
	if m == nil {
		return fmt.Errorf("No job")
	}
	if m.RootTask == nil {
		return fmt.Errorf("No task exists for this job")
	}
	return m.RootTask.Setup(0)
}

func (m *SqlJob) Run() error {
	if m.Ctx != nil {
		m.Ctx.DisableRecover = m.Ctx.DisableRecover
	}
	return m.RootTask.Run()
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
//  plan for execution of this query/job, include the projection
func BuildSqlProjectedJob(ctx *plan.Context) (*SqlJob, error) {
	return BuildSqlProjectedWrapper(nil, ctx)
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job, include the projection
func BuildSqlProjectedWrapper(wrapper expr.Visitor, ctx *plan.Context) (*SqlJob, error) {

	job, err := BuildSqlJobWrapped(wrapper, ctx)
	if err != nil {
		//u.Warnf("could not build %v", err)
		return job, err
	}
	if job.Ctx.Projection != nil {
		//u.Warnf("already has projection?")
		return job, nil
	}
	if sqlSelect, ok := ctx.Stmt.(*expr.SqlSelect); ok {
		job.Ctx.Projection, err = plan.NewProjectionFinal(ctx, sqlSelect)
		//u.Debugf("load projection final job.Projection: %p", job.Projection)
		if err != nil {
			return nil, err
		}
	}
	return job, nil
}
func BuildSqlJob(ctx *plan.Context) (*SqlJob, error) {
	return BuildSqlJobWrapped(nil, ctx)
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlJobWrapped(wrapper expr.Visitor, ctx *plan.Context) (*SqlJob, error) {

	stmt, err := expr.ParseSql(ctx.Raw)
	if err != nil {
		//u.Warnf("could not parse %v", err)
		return nil, err
	}
	if stmt == nil {
		return nil, fmt.Errorf("Not statement for parse? %v", ctx.Raw)
	}
	ctx.Stmt = stmt

	if ctx.Schema == nil {
		u.LogTracef(u.WARN, "no schema? %s", ctx.Raw)
	}
	builder := NewJobBuilder(ctx)
	var visitor expr.Visitor = builder
	if wrapper != nil {
		visitor = wrapper.Wrap(builder)
	}
	task, _, err := stmt.Accept(visitor)
	//u.Infof("build sqljob.proj: %p", builder.Projection)

	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("No job runner? %v", ctx.Raw)
	}
	taskRunner, ok := task.(TaskRunner)
	if !ok {
		return nil, fmt.Errorf("Must be taskrunner but was %T", task)
	}
	return &SqlJob{
		RootTask: taskRunner,
		Ctx:      ctx,
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
