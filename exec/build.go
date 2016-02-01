package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = u.EMPTY

	// JobBuilder implements JobRunner
	_ JobRunner = (*JobExecutor)(nil)

	// Ensure that we implement the plan.Planner interface for our job
	_ plan.Planner       = (*JobBuilder)(nil)
	_ Executor           = (*JobExecutor)(nil)
	_ plan.SourcePlanner = (*SourceBuilder)(nil)
)

// Job Runner is the main RunTime interface for running a SQL Job of tasks
type JobRunner interface {
	Setup() error
	Run() error
	Close() error
}

// JobBuilder is implementation of plan.Planner that creates a dag of plan.Tasks
// that will be turned into execution plan by executor.  This is a simple
// planner but can be over-ridden by providing a Planner that will
// supercede any single or more visit methods.
type JobBuilder struct {
	Planner   plan.Planner
	Ctx       *plan.Context
	TaskMaker plan.TaskPlanner
	distinct  bool
	children  []plan.Task
}

type JobExecutor struct {
	Planner   plan.Planner
	Executor  Executor
	RootTask  TaskRunner
	Ctx       *plan.Context
	TaskMaker plan.TaskPlanner
	distinct  bool
	children  []Task
}

// Build dag of tasks for single source of statement
type SourceBuilder struct {
	SourcePlanner plan.SourcePlanner
	Plan          *plan.Source
	TaskMaker     plan.TaskPlanner
}

func NewJobBuilder(ctx *plan.Context, planner plan.Planner) *JobExecutor {
	b := &JobBuilder{}
	b.Ctx = ctx
	if planner == nil {
		b.Planner = b
	} else {
		b.Planner = planner
	}
	b.TaskMaker = TaskRunnersMaker(ctx)
	e := &JobExecutor{}
	e.Executor = e
	e.Planner = b.Planner
	e.TaskMaker = b.TaskMaker
	e.Ctx = ctx
	return e
}
func BuildSqlJob(ctx *plan.Context) (*JobExecutor, error) {
	job := NewJobBuilder(ctx, nil)
	task, err := BuildSqlJobVisitor(job.Planner, job.Executor, ctx)
	if err != nil {
		return nil, err
	}
	taskRunner, ok := task.(TaskRunner)
	if !ok {
		return nil, fmt.Errorf("Expected TaskRunner but was %T", task)
	}
	job.RootTask = taskRunner
	return job, err
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlJobVisitor(planner plan.Planner, executor Executor, ctx *plan.Context) (plan.Task, error) {

	stmt, err := rel.ParseSql(ctx.Raw)
	if err != nil {
		u.Debugf("could not parse %v", err)
		return nil, err
	}
	if stmt == nil {
		return nil, fmt.Errorf("Not statement for parse? %v", ctx.Raw)
	}
	ctx.Stmt = stmt

	if ctx.Schema == nil {
		u.LogTraceDf(u.WARN, 12, "no schema? %s", ctx.Raw)
	}

	u.Debugf("build sqljob.Planner: %T   %#v", planner, planner)
	pln, err := plan.WalkStmt(stmt, planner)
	//u.Debugf("build sqljob.proj: %p", builder.Projection)

	if err != nil {
		return nil, err
	}
	if pln == nil {
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	execRoot, err := WalkExecutor(pln, executor)
	//u.Debugf("build sqljob.proj: %p", builder.Projection)

	if err != nil {
		return nil, err
	}
	if execRoot == nil {
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	execTask, ok := execRoot.(plan.Task)
	if !ok {
		return nil, fmt.Errorf("Expected plan.Task but was %T", pln)
	}
	return execTask, err
}

func WalkExecutor(p plan.Task, executor Executor) (Task, error) {
	switch pt := p.(type) {
	case *plan.Select:
		return executor.WalkSelect(pt)
		// case *plan.PreparedStatement:
		// case *plan.Insert:
		// case *plan.Upsert:
		// case *rel.Update:
		// case *plan.Delete:
		// case *plan.Show:
		// case *plan.Describe:
		// case *plan.Command:
	}
	panic(fmt.Sprintf("Not implemented for %T", p))
}
func NewSourceBuilder(sp *plan.Source, taskMaker plan.TaskPlanner) *SourceBuilder {
	return &SourceBuilder{Plan: sp, TaskMaker: taskMaker}
}

func (m *JobExecutor) Setup() error {
	if m == nil {
		return fmt.Errorf("No job")
	}
	if m.RootTask == nil {
		return fmt.Errorf("No task exists for this job")
	}
	return m.RootTask.Setup(0)
}

func (m *JobExecutor) Run() error {
	if m.Ctx != nil {
		m.Ctx.DisableRecover = m.Ctx.DisableRecover
	}
	return m.RootTask.Run()
}

func (m *JobExecutor) Close() error {
	return m.RootTask.Close()
}

// The drain is the last out channel, on last task
func (m *JobExecutor) DrainChan() MessageChan {
	tasks := m.RootTask.Children()
	return tasks[len(tasks)-1].(TaskRunner).MessageOut()
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
