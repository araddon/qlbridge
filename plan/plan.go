package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the sql expr.Visitor interface
	_ Visitor       = (*Planner)(nil)
	_ SourceVisitor = (*Planner)(nil)
)

type (
	// Within a Select query, if it has multiple sources such
	//   as sub-select, join, etc this is the plan for single source
	SourcePlan struct {
		*expr.SqlSource
		Ctx          *Context
		DataSource   schema.DataSource
		SourceSchema *schema.SourceSchema
		//Conn  schema.SourceConn
		Proj  *expr.Projection
		Tbl   *schema.Table
		Final bool
	}
	// Plan for full parent query, including its children
	SelectPlan struct {
		*expr.SqlSelect
		Sources []*SourcePlan
	}
	// Insert plan query
	// InsertPlan struct {
	// 	*expr.SqlInsert
	// 	Sources []*SourcePlan
	// }
)

// A PlanTask is a single step of a query-execution plan
//  such as "scan" or "where-filter" or "group-by".
//  These tasks are assembled into a Dag of Tasks.
type PlanTask interface {
	Clone() PlanTask
}

// Some sources can do their own planning for sub-select statements
type SourceSelectPlanner interface {
	// given our plan, trun that into a Task.
	VisitSourceSelect(plan *SourcePlan) (expr.Task, expr.VisitStatus, error)
}

// A planner creates an execution plan for a given Statement, with ability to cache plans
//   to be re-used. this is very simple planner, but potentially better planners with more
//   knowledge of schema, distributed runtimes etc could be plugged
type Planner struct {
	expr.Visitor
	Ctx   *Context
	where *expr.SqlWhere
	tasks []PlanTask
}

func NewSourcePlan(ctx *Context, src *expr.SqlSource, isFinal bool) (*SourcePlan, error) {
	sp := &SourcePlan{SqlSource: src, Ctx: ctx, Final: isFinal}
	err := sp.load(ctx)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

func NewPlanner(ctx *Context) (*Planner, expr.VisitStatus, error) {

	panic("hm, needs context?")
	plan := &Planner{
		Ctx: ctx,
	}
	switch sql := ctx.Stmt.(type) {
	case *expr.SqlSelect:
		plan.where = sql.Where
	}
	_, status, err := ctx.Stmt.Accept(plan)
	//u.Debugf("task:  %T  %#v", task, task)
	if err != nil {
		return nil, status, err
	}

	return plan, status, nil
}

func (m *SourcePlan) load(ctx *Context) error {
	//u.Debugf("SourcePlan.load()")
	fromName := strings.ToLower(m.SqlSource.SourceName())
	ss, err := ctx.Schema.Source(fromName)
	if err != nil {
		return err
	}
	if ss == nil {
		u.Warnf("%p Schema  no %s found", ctx.Schema, fromName)
		return fmt.Errorf("Could not find source for %v", m.SqlSource.SourceName())
	}
	m.SourceSchema = ss
	m.DataSource = ss.DS

	tbl, err := ctx.Schema.Table(fromName)
	if err != nil {
		u.Warnf("%p Schema %v", ctx.Schema, fromName)
		u.Errorf("could not get table: %v", err)
		return err
	}
	// if tbl == nil {
	// 	u.Warnf("wat, no table? %v", fromName)
	// 	return fmt.Errorf("No table found for %s", fromName)
	// }
	m.Tbl = tbl
	//u.Debugf("tbl %#v", tbl)
	err = projecectionForSourcePlan(m)
	return nil
}
func (m *Planner) Wrap(visitor expr.Visitor) expr.Visitor {
	u.Debugf("wrap %T", visitor)
	m.Visitor = visitor
	return m
}

func (m *Planner) VisitSelect(stmt *expr.SqlSelect) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitSource %+v", stmt)
	return nil, expr.VisitError, nil
}

func (m *Planner) VisitInsert(stmt *expr.SqlInsert) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitDelete(stmt *expr.SqlDelete) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitUpdate(stmt *expr.SqlUpdate) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitUpsert(stmt *expr.SqlUpsert) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitShow(stmt *expr.SqlShow) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitDescribe(stmt *expr.SqlDescribe) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitPreparedStmt(stmt *expr.PreparedStatement) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}
func (m *Planner) VisitCommand(stmt *expr.SqlCommand) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, expr.VisitError, expr.ErrNotImplemented
}

// VisitSourceSelect(plan *SourcePlan) (expr.Task, expr.VisitStatus, error)
func (m *Planner) VisitSourceSelect(plan *SourcePlan) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitSourceSelect %+v", plan)
	return nil, expr.VisitError, expr.ErrNotImplemented
}
