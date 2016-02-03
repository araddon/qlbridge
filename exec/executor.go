package exec

import (
	"fmt"

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

type JobExecutor struct {
	Planner  plan.Planner
	Executor Executor
	RootTask TaskRunner
	Ctx      *plan.Context
	distinct bool
	children []Task
}

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
	case *plan.Upsert:
		return root, root.Add(NewUpsert(m.Ctx, pt))
	case *plan.Insert:
		return root, root.Add(NewInsert(m.Ctx, pt))
	case *plan.Update:
		return root, root.Add(NewUpdate(m.Ctx, pt))
	case *plan.Delete:
		return root, root.Add(NewDelete(m.Ctx, pt))
		// case *plan.Show:
		// case *plan.Describe:
		// case *plan.Command:
	}
	panic(fmt.Sprintf("Not implemented for %T", p))
}
func (m *JobExecutor) WalkPlanAll(p plan.Task) (Task, error) {
	root, err := m.WalkPlanTask(p)
	if err != nil {
		u.Errorf("all damn %v err=%v", p, err)
		return nil, err
	}
	if len(p.Children()) > 0 {
		var dagRoot Task
		u.Debugf("sequential?%v  parallel?%v", p.IsSequential(), p.IsParallel())
		if p.IsParallel() {
			dagRoot = NewTaskParallel(m.Ctx)
		} else {
			dagRoot = NewTaskSequential(m.Ctx)
		}
		err = dagRoot.Add(root)
		if err != nil {
			u.Errorf("Could not add root: %v", err)
			return nil, err
		}
		return dagRoot, m.WalkChildren(p, dagRoot)
	}
	u.Debugf("got root? %T for %T", root, p)
	u.Debugf("len=%d  for children:%v", len(p.Children()), p.Children())
	return root, m.WalkChildren(p, root)
}
func (m *JobExecutor) WalkPlanTask(p plan.Task) (Task, error) {
	switch pt := p.(type) {
	case *plan.Source:
		return NewSource(pt)
	case *plan.Where:
		return NewWhere(m.Ctx, pt), nil
	case *plan.Having:
		return NewHaving(m.Ctx, pt), nil
	case *plan.GroupBy:
		return NewGroupBy(m.Ctx, pt), nil
	case *plan.Projection:
		return NewProjection(m.Ctx, pt), nil
	case *plan.JoinMerge:
		return m.WalkJoin(pt)
	case *plan.JoinKey:
		return NewJoinKey(m.Ctx, pt), nil
	}
	panic(fmt.Sprintf("Task plan-exec Not implemented for %T", p))
}
func (m *JobExecutor) WalkChildren(p plan.Task, root Task) error {
	for _, t := range p.Children() {
		u.Debugf("parent: %T  walk child %T", p, t)
		et, err := m.WalkPlanTask(t)
		if err != nil {
			u.Errorf("could not create task %#v err=%v", t, err)
		}
		if len(t.Children()) > 0 {
			u.Warnf("has children but not handled %#v", t)
		}
		err = root.Add(et)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *JobExecutor) WalkJoin(p *plan.JoinMerge) (Task, error) {
	execTask := NewTaskParallel(m.Ctx)
	u.Debugf("join.Left: %#v    \nright:%#v", p.Left, p.Right)
	l, err := m.WalkPlanAll(p.Left)
	if err != nil {
		u.Errorf("whoops %T  %v", l, err)
		return nil, err
	}
	err = execTask.Add(l)
	if err != nil {
		u.Errorf("whoops %T  %v", l, err)
		return nil, err
	}
	r, err := m.WalkPlanAll(p.Right)
	if err != nil {
		return nil, err
	}
	err = execTask.Add(r)
	if err != nil {
		return nil, err
	}

	jm := NewJoinNaiveMerge(m.Ctx, l.(TaskRunner), r.(TaskRunner), p)
	err = execTask.Add(jm)
	if err != nil {
		return nil, err
	}
	return execTask, nil
}

func (m *JobExecutor) Setup() error {
	if m == nil {
		return fmt.Errorf("JobExecutor is nil?")
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
