package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

type (
	// Sources can often do their own planning for sub-select statements
	//  ie mysql can do its own select, projection mongo can as well
	// - provide interface to allow passing down selection to source
	SourceSelectPlanner interface {
		// given our plan, turn that into a Task.
		// - if VisitStatus is not Final then we need to poly-fill
		VisitSourceSelect(plan *SourcePlan) (rel.Task, rel.VisitStatus, error)
	}
)

type (
	// Within a Select query, if optionaly has multiple sources such
	//   as sub-select, join, etc this is the plan for a single source
	SourcePlan struct {
		From         *rel.SqlSource       // The sub-query, from source
		Ctx          *Context             // query context
		DataSource   schema.DataSource    // The data source for this From
		SourceSchema *schema.SourceSchema // Schema for this source/from
		Proj         *rel.Projection      // projection for this sub-query
		Tbl          *schema.Table        // Table part of SourceSchema for this From
		Final        bool                 // Is this final or not?   if sub-query = false, if single from then True
	}
	// Plan for full parent query, including its children
	SelectPlan struct {
		*rel.SqlSelect
		Sources []*SourcePlan
	}
)

func NewSourcePlan(ctx *Context, src *rel.SqlSource, isFinal bool) (*SourcePlan, error) {
	sp := &SourcePlan{From: src, Ctx: ctx, Final: isFinal}
	err := sp.load(ctx)
	if err != nil {
		return nil, err
	}
	return sp, nil
}
func NewSourceStaticPlan(ctx *Context) *SourcePlan {
	return &SourcePlan{Ctx: ctx, Final: true}
}

func (m *SourcePlan) load(ctx *Context) error {
	//u.Debugf("SourcePlan.load()")
	if m.From == nil {
		return nil
	}
	fromName := strings.ToLower(m.From.SourceName())
	ss, err := ctx.Schema.Source(fromName)
	if err != nil {
		return err
	}
	if ss == nil {
		u.Warnf("%p Schema  no %s found", ctx.Schema, fromName)
		return fmt.Errorf("Could not find source for %v", m.From.SourceName())
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

/*
// A planner creates an execution plan for a given Statement, with ability to cache plans
//   to be re-used. this is very simple planner, but potentially better planners with more
//   knowledge of schema, distributed runtimes etc could be plugged
// type Planner struct {
// 	Visitor
// 	Ctx   *Context
// 	where *rel.SqlWhere
// 	tasks []PlanTask
// }

func NewPlanner(visitor Visitor, ctx *Context) *Planner {

	plan := &Planner{
		Ctx:     ctx,
		Visitor: visitor,
	}
	// switch sql := ctx.Stmt.(type) {
	// case *rel.SqlSelect:
	// 	plan.where = sql.Where
	// }
	// _, status, err := ctx.Stmt.Accept(plan)
	// //u.Debugf("task:  %T  %#v", task, task)
	// if err != nil {
	// 	return nil, status, err
	// }

	return plan
}
func (m *Planner) Wrap(visitor rel.Visitor) rel.Visitor {
	u.Debugf("wrap %T", visitor)
	m.Visitor = visitor
	return m
}

func (m *Planner) VisitSelect(stmt *rel.SqlSelect) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitSource %+v", stmt)
	return nil, rel.VisitError, nil
}

func (m *Planner) VisitInsert(stmt *rel.SqlInsert) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitDelete(stmt *rel.SqlDelete) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitUpdate(stmt *rel.SqlUpdate) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitUpsert(stmt *rel.SqlUpsert) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitShow(stmt *rel.SqlShow) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitDescribe(stmt *rel.SqlDescribe) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

func (m *Planner) VisitPreparedStmt(stmt *rel.PreparedStatement) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}
func (m *Planner) VisitCommand(stmt *rel.SqlCommand) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, rel.VisitError, expr.ErrNotImplemented
}

// VisitSourceSelect(plan *SourcePlan) (rel.Task, rel.VisitStatus, error)
func (m *Planner) VisitSourceSelect(plan *SourcePlan) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitSourceSelect %+v", plan)
	return nil, rel.VisitError, expr.ErrNotImplemented
}
*/
var _ = u.EMPTY
