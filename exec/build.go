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
	_ Executor = (*JobExecutor)(nil)
	//_ plan.SourcePlanner = (*SourceBuilder)(nil)
)

// Job Runner is the main RunTime interface for running a SQL Job of tasks
type JobRunner interface {
	Setup() error
	Run() error
	Close() error
}

type JobExecutor struct {
	Planner  plan.Planner
	Executor Executor
	RootTask TaskRunner
	Ctx      *plan.Context
	distinct bool
	children []Task
}

// Build dag of tasks for single source of statement
// type SourceBuilder struct {
// 	SourcePlanner plan.SourcePlanner
// 	Plan          *plan.Source
// 	TaskMaker     plan.TaskPlanner
// }

func NewExecutor(ctx *plan.Context, planner plan.Planner) *JobExecutor {
	e := &JobExecutor{}
	e.Executor = e
	e.Planner = planner
	e.Ctx = ctx
	return e
}
func BuildSqlJob(ctx *plan.Context) (*JobExecutor, error) {
	job := NewExecutor(ctx, plan.NewPlanner(ctx))
	task, err := BuildSqlJobPlanned(job.Planner, job.Executor, ctx)
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
func BuildSqlJobPlanned(planner plan.Planner, executor Executor, ctx *plan.Context) (Task, error) {

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
		u.Errorf("wat?  %v", err)
		return nil, err
	}
	if pln == nil {
		u.Errorf("wat?  %v", err)
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	execRoot, err := executor.WalkPlan(pln)
	//u.Debugf("build sqljob.proj: %p", builder.Projection)

	if err != nil {
		u.Errorf("wat?  %v", err)
		return nil, err
	}
	if execRoot == nil {
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	return execRoot, err
}

func (m *JobExecutor) WalkPlan(p plan.Task) (Task, error) {
	var root Task
	if p.IsParallel() {
		root = NewTaskParallel(m.Ctx)
	} else {
		root = NewTaskSequential(m.Ctx)
	}
	switch pt := p.(type) {
	case *plan.Select:
		return root, m.WalkChildren(pt, root)
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
func (m *JobExecutor) WalkPlanTask(p plan.Task) (Task, error) {
	switch pt := p.(type) {
	case *plan.Source:
		return NewSource(pt)
	case *plan.Where:
		return NewWhere(m.Ctx, pt), nil
	case *plan.Projection:
		return NewProjection(m.Ctx, pt), nil
	}
	panic(fmt.Sprintf("Not implemented for %T", p))
}
func (m *JobExecutor) WalkChildren(p plan.Task, root Task) error {
	for _, t := range p.Children() {
		u.Debugf("t %T", t)
		et, err := m.WalkPlanTask(t)
		if err != nil {
			u.Errorf("could not create task %#v err=%v", t, err)
		}
		if len(t.Children()) > 0 {
			u.Warnf("has children %#v", t)
			// var childRoot Task
			// if p.IsParallel() {
			// 	childRoot = NewTaskParallel(m.Ctx)
			// } else {
			// 	childRoot = NewTaskSequential(m.Ctx)
			// }
		}
		err = root.Add(et)
		if err != nil {
			return err
		}
	}
	return nil
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
