package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/lex"
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

// JobExecutor translates a Sql Statement into a Execution DAG of tasks
// using the Planner, Executor supplied.  This package implements default
// executor and uses the default Planner from plan.  This will create a single
// node dag of Tasks.
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
		u.Debugf("could not parse sql : %v", err)
		return nil, err
	}
	if stmt == nil {
		return nil, fmt.Errorf("Not statement for parse? %v", ctx.Raw)
	}
	ctx.Stmt = stmt

	if ctx.Schema == nil {
		u.LogTraceDf(u.WARN, 12, "no schema? %s", ctx.Raw)
	}

	//u.WarnT(8)
	//u.Debugf("P:%p  E:%p  build sqljob.Planner: %T   %#v", planner, executor, planner, planner)
	pln, err := plan.WalkStmt(ctx, stmt, planner)
	//u.Debugf("build sqljob.proj: %#v", pln)

	if err != nil {
		u.Errorf("error on plan walk statement?  %v", err)
		return nil, err
	}
	if pln == nil {
		u.Errorf("error, no plan task, should not be possible?  %v", err)
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	//u.Warnf("executor? %T  ", executor)
	execRoot, err := executor.WalkPlan(pln)
	//u.Debugf("finished exec task plan: %#v", execRoot)

	if err != nil {
		u.Errorf("error on plan?  %v", err)
		return nil, err
	}
	if execRoot == nil {
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	return execRoot, err
}

func (m *JobExecutor) NewTask(p plan.Task) Task {
	if p.IsParallel() {
		return NewTaskParallel(m.Ctx)
	}
	return NewTaskSequential(m.Ctx)
}

// Main Entry point to take a Plan, and convert into Execution DAG
func (m *JobExecutor) WalkPlan(p plan.Task) (Task, error) {
	switch p := p.(type) {
	case *plan.PreparedStatement:
		return m.Executor.WalkPreparedStatement(p)
	case *plan.Select:
		if len(p.From) > 0 {
			//u.Debugf("walk select p:%p m.Executor: %p ChildDag?%v %v", p, m.Executor, p.ChildDag, p.Stmt.String())
		}
		if p.Ctx != nil && p.IsSchemaQuery() {
			//u.Debugf("is schema query. ctx nil? %v", p.Ctx == nil)
			if p.Ctx.Schema != nil && p.Ctx.Schema.InfoSchema != nil {
				p.Ctx.Schema = p.Ctx.Schema.InfoSchema
			}
			p.Stmt.SetSystemQry()
		}
		return m.Executor.WalkSelect(p)
	case *plan.Upsert:
		return m.Executor.WalkUpsert(p)
	case *plan.Insert:
		return m.Executor.WalkInsert(p)
	case *plan.Update:
		return m.Executor.WalkUpdate(p)
	case *plan.Delete:
		return m.Executor.WalkDelete(p)
	case *plan.Command:
		return m.Executor.WalkCommand(p)
	}
	panic(fmt.Sprintf("Not implemented for %T", p))
}
func (m *JobExecutor) WalkPreparedStatement(p *plan.PreparedStatement) (Task, error) {
	return nil, ErrNotImplemented
}
func (m *JobExecutor) WalkSelect(p *plan.Select) (Task, error) {
	root := m.NewTask(p)
	return root, m.WalkChildren(p, root)
}
func (m *JobExecutor) WalkUpsert(p *plan.Upsert) (Task, error) {
	root := m.NewTask(p)
	return root, root.Add(NewUpsert(m.Ctx, p))
}
func (m *JobExecutor) WalkInsert(p *plan.Insert) (Task, error) {
	root := m.NewTask(p)
	return root, root.Add(NewInsert(m.Ctx, p))
}
func (m *JobExecutor) WalkUpdate(p *plan.Update) (Task, error) {
	root := m.NewTask(p)
	return root, root.Add(NewUpdate(m.Ctx, p))
}
func (m *JobExecutor) WalkDelete(p *plan.Delete) (Task, error) {
	root := m.NewTask(p)
	return root, root.Add(NewDelete(m.Ctx, p))
}
func (m *JobExecutor) WalkCommand(p *plan.Command) (Task, error) {
	if p.Stmt.Keyword() == lex.TokenSet {
		root := m.NewTask(p)
		return root, root.Add(NewCommand(m.Ctx, p))
	}
	return nil, ErrNotImplemented
}
func (m *JobExecutor) WalkSource(p *plan.Source) (Task, error) {
	//u.Debugf("%p NewSource? %p", m, p)
	if len(p.Static) > 0 {
		//u.Warnf("found static source")
		static := membtree.NewStaticData("static")
		static.SetColumns(p.Cols)
		_, err := static.Put(nil, nil, p.Static)
		if err != nil {
			u.Errorf("Could not put %v", err)
		}
		return NewSourceScanner(m.Ctx, p, static), nil
	} else if p.Conn == nil {
		u.Warnf("no conn? %T", p.DataSource)
		if p.DataSource == nil {
			u.Warnf("no datasource")
			return nil, fmt.Errorf("missing data source")
		}
		source, err := p.DataSource.Open(p.Stmt.SourceName())
		if err != nil {
			return nil, err
		}
		p.Conn = source
		//u.Debugf("setting p.Conn %p %T", p.Conn, p.Conn)
	}

	e, hasSourceExec := p.Conn.(ExecutorSource)
	if hasSourceExec {
		return e.WalkExecSource(p)
	}
	return NewSource(m.Ctx, p)
}
func (m *JobExecutor) WalkSourceExec(p *plan.Source) (Task, error) {

	if p.Conn == nil {
		if p.DataSource == nil {
			u.Warnf("no datasource")
			return nil, fmt.Errorf("missing data source")
		}
		source, err := p.DataSource.Open(p.Stmt.SourceName())
		if err != nil {
			return nil, err
		}
		p.Conn = source
		//u.Debugf("setting p.Conn %p %T", p.Conn, p.Conn)
	}

	e, hasSourceExec := p.Conn.(ExecutorSource)
	if hasSourceExec {
		return e.WalkExecSource(p)
	}
	u.Warnf("source %T does not implement datasource.Scanner", p.Conn)
	return nil, fmt.Errorf("%T Must Implement Scanner for %q", p.Conn, p.Stmt.String())
}
func (m *JobExecutor) WalkWhere(p *plan.Where) (Task, error) {
	return NewWhere(m.Ctx, p), nil
}
func (m *JobExecutor) WalkHaving(p *plan.Having) (Task, error) {
	return NewHaving(m.Ctx, p), nil
}
func (m *JobExecutor) WalkGroupBy(p *plan.GroupBy) (Task, error) {
	return NewGroupBy(m.Ctx, p), nil
}
func (m *JobExecutor) WalkProjection(p *plan.Projection) (Task, error) {
	return NewProjection(m.Ctx, p), nil
}
func (m *JobExecutor) WalkJoin(p *plan.JoinMerge) (Task, error) {
	execTask := NewTaskParallel(m.Ctx)
	//u.Debugf("join.Left: %#v    \nright:%#v", p.Left, p.Right)
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
func (m *JobExecutor) WalkJoinKey(p *plan.JoinKey) (Task, error) {
	return NewJoinKey(m.Ctx, p), nil
}
func (m *JobExecutor) WalkPlanAll(p plan.Task) (Task, error) {
	root, err := m.WalkPlanTask(p)
	if err != nil {
		u.Errorf("all damn %v err=%v", p, err)
		return nil, err
	}
	if len(p.Children()) > 0 {
		dagRoot := m.NewTask(p)
		//u.Debugf("sequential?%v  parallel?%v", p.IsSequential(), p.IsParallel())
		err = dagRoot.Add(root)
		if err != nil {
			u.Errorf("Could not add root: %v", err)
			return nil, err
		}
		return dagRoot, m.WalkChildren(p, dagRoot)
	}
	//u.Debugf("got root? %T for %T", root, p)
	//u.Debugf("len=%d  for children:%v", len(p.Children()), p.Children())
	return root, m.WalkChildren(p, root)
}
func (m *JobExecutor) WalkPlanTask(p plan.Task) (Task, error) {
	//u.Debugf("WalkPlanTask: %p  %T", p, p)
	switch p := p.(type) {
	case *plan.Source:
		//u.Warnf("walkplantask source %#v", m.Executor)
		return m.Executor.WalkSource(p)
	case *plan.Where:
		return m.Executor.WalkWhere(p)
	case *plan.Having:
		return m.Executor.WalkHaving(p)
	case *plan.GroupBy:
		return m.Executor.WalkGroupBy(p)
	case *plan.Projection:
		return m.Executor.WalkProjection(p)
	case *plan.JoinMerge:
		return m.Executor.WalkJoin(p)
	case *plan.JoinKey:
		return m.Executor.WalkJoinKey(p)
	}
	panic(fmt.Sprintf("Task plan-exec Not implemented for %T", p))
}

// WalkChildren walk dag of plan taasks creating execution tasks
func (m *JobExecutor) WalkChildren(p plan.Task, root Task) error {
	for _, t := range p.Children() {
		//u.Debugf("parent: %T  walk child %p %T  %#v", p, t, t, p.Children())
		et, err := m.WalkPlanTask(t)
		if err != nil {
			u.Errorf("could not create task %#v err=%v", t, err)
		}
		if len(t.Children()) == 0 {
			err = root.Add(et)
			if err != nil {
				return err
			}
		} else {
			childRoot := m.Executor.NewTask(t)
			err = root.Add(childRoot)
			if err != nil {
				return err
			}
			err = childRoot.Add(et)
			if err != nil {
				return err
			}
			//u.Warnf("has children but not handled %#v", t)
			for _, c := range t.Children() {
				//u.Warnf("\tchild task %#v", c)
				ct, err := m.WalkPlanTask(c)
				if err != nil {
					u.Errorf("could not create child task %#v err=%v", c, err)
					return err
				}
				if err = childRoot.Add(ct); err != nil {
					u.Errorf("Could not add task %v", err)
					return err
				}
			}
		}
	}
	return nil
}

// Setup this dag of tasks
func (m *JobExecutor) Setup() error {
	if m == nil {
		return fmt.Errorf("JobExecutor is nil?")
	}
	if m.RootTask == nil {
		return fmt.Errorf("No task exists for this job")
	}
	return m.RootTask.Setup(0)
}

// Run this task
func (m *JobExecutor) Run() error {
	if m.Ctx != nil {
		m.Ctx.DisableRecover = m.Ctx.DisableRecover
	}
	//u.Debugf("job run: %#v", m.RootTask)
	return m.RootTask.Run()
}

// Close the normal close of root task
func (m *JobExecutor) Close() error {
	return m.RootTask.Close()
}

// The drain is the last out channel, on last task
func (m *JobExecutor) DrainChan() MessageChan {
	tasks := m.RootTask.Children()
	return tasks[len(tasks)-1].(TaskRunner).MessageOut()
}
