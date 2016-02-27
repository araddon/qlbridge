package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
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

	//u.LogTraceDf(u.WARN, 16, "hello")
	//u.Debugf("P:%p  E:%p  build sqljob.Planner: %T   %#v", planner, executor, planner, planner)
	pln, err := plan.WalkStmt(ctx, stmt, planner)
	//u.Debugf("build sqljob.proj: %#v", pln)

	if err != nil {
		u.Errorf("wat?  %v", err)
		return nil, err
	}
	if pln == nil {
		u.Errorf("wat?  %v", err)
		return nil, fmt.Errorf("No plan root task found? %v", ctx.Raw)
	}

	//u.Warnf("executor? %T  ", executor)
	execRoot, err := executor.WalkPlan(pln)
	//u.Debugf("finished exec task plan: %#v", execRoot)

	if err != nil {
		u.Errorf("wat?  %v", err)
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
		//u.Debugf("plan.Select %T  %T", m, m.Executor)
		return m.Executor.WalkSelect(p)
	case *plan.Upsert:
		return m.Executor.WalkUpsert(p)
	case *plan.Insert:
		return m.Executor.WalkInsert(p)
	case *plan.Update:
		return m.Executor.WalkUpdate(p)
	case *plan.Delete:
		return m.Executor.WalkDelete(p)
	case *plan.Show:
		return m.Executor.WalkShow(p)
	case *plan.Describe:
		return m.Executor.WalkDescribe(p)
	case *plan.Command:
		return m.Executor.WalkCommand(p)
	}
	panic(fmt.Sprintf("Not implemented for %T", p))
}
func (m *JobExecutor) WalkPreparedStatement(p *plan.PreparedStatement) (Task, error) {
	return nil, ErrNotImplemented
}
func (m *JobExecutor) WalkSelect(p *plan.Select) (Task, error) {

	if p.Stmt.IsSysQuery() {
		//u.Debugf("sysquery? %v for %s", p.Stmt.IsSysQuery(), p.Stmt)
		return m.WalkSysQuery(p)
	} else if len(p.Stmt.From) == 0 && len(p.Stmt.Columns) == 1 && strings.ToLower(p.Stmt.Columns[0].As) == "database" {
		// SELECT database;
		//return m.WalkSelectDatabase(p)
		u.Warnf("not implemented select database")
		return nil, ErrNotImplemented
	}
	//u.Debugf("%p walk Select %T Executor?%T", m, m, m.Executor)
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
func (m *JobExecutor) WalkDescribe(p *plan.Describe) (Task, error) {
	u.Warnf("not implemented Describe")
	return nil, ErrNotImplemented
}
func (m *JobExecutor) WalkShow(p *plan.Show) (Task, error) {
	u.Warnf("not implemented Show")
	return nil, ErrNotImplemented
}
func (m *JobExecutor) WalkCommand(p *plan.Command) (Task, error) {
	return nil, ErrNotImplemented
}
func (m *JobExecutor) WalkSource(p *plan.Source) (Task, error) {
	//u.Debugf("NewSource? %#v", p)
	if p.SourceExec {
		return m.WalkSourceExec(p)
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
		u.Debugf("sequential?%v  parallel?%v", p.IsSequential(), p.IsParallel())
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
				ct, err := m.WalkPlanTask(t)
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
	//u.Debugf("job run: %#v", m.RootTask)
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

func (m *JobExecutor) WalkSysQuery(p *plan.Select) (Task, error) {

	root := m.NewTask(p)

	static := membtree.NewStaticData("schema")

	//u.Debugf("Ctx.Projection: %#v", m.Ctx.Projection)
	//u.Debugf("Ctx.Projection.Proj: %#v", m.Ctx.Projection.Proj)
	proj := rel.NewProjection()
	cols := make([]string, len(p.Stmt.Columns))
	row := make([]driver.Value, len(cols))
	for i, col := range p.Stmt.Columns {
		if col.Expr == nil {
			return nil, fmt.Errorf("no column info? %#v", col.Expr)
		}
		switch n := col.Expr.(type) {
		case *expr.IdentityNode:
			coln := strings.ToLower(n.Text)
			cols[i] = col.As
			if strings.HasPrefix(coln, "@@") {
				//u.Debugf("m.Ctx? %#v", m.Ctx)
				//u.Debugf("m.Ctx.Session? %#v", m.Ctx.Session)
				val, ok := m.Ctx.Session.Get(coln)
				//u.Debugf("got session var? %v=%#v", col.As, val)
				if ok {
					proj.AddColumnShort(col.As, val.Type())
					row[i] = val.Value()
				} else {
					proj.AddColumnShort(col.As, value.NilType)
				}
			} else {
				u.Infof("columns?  as=%q    rel=%q", col.As, coln)
			}
			// SELECT current_user
		case *expr.FuncNode:
			// SELECT current_user()
			// n.String()
		}
	}
	static.SetColumns(cols)
	_, err := static.Put(nil, nil, row)
	if err != nil {
		u.Errorf("Could not put %v", err)
	}

	m.Ctx.Projection = plan.NewProjectionStatic(proj)
	//u.Debugf("%p=plan.projection  rel.Projection=%p", m.Projection, p)
	sourcePlan := plan.NewSourceStaticPlan(m.Ctx)
	sourceTask := NewSourceScanner(m.Ctx, sourcePlan, static)
	root.Add(sourceTask)
	return root, nil
}
