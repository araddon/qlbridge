package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the sql expr.Visitor interface
	_ Visitor       = (*Planner)(nil)
	_ SourceVisitor = (*Planner)(nil)
)

type (
	SourcePlan struct {
		*expr.SqlSource
		DataSource *datasource.DataSourceFeatures
		Proj       *expr.Projection
		Tbl        *datasource.Table
		Final      bool
		//tasks      []PlanTask
	}
	SelectPlan struct {
		*expr.SqlSelect
		Sources []*SourcePlan
	}
	InsertPlan struct {
		*expr.SqlInsert
		Sources []*SourcePlan
	}
)

// A PlanTask is a part of a Plan, each task may have children
//
type PlanTask interface {
	Clone() PlanTask
}

// Some sources can do their own planning for sub-select statements
type SourceSelectPlanner interface {
	// return a source plan builder, which implements Accept() visitor interface
	//SubSelectVisitor() (expr.SubVisitor, error)
	//Projection
	VisitSourceSelect(plan *SourcePlan) (expr.Task, expr.VisitStatus, error)
}

// A planner creates an execution plan for a given Statement, with ability to cache plans
//   to be re-used. this is very simple planner, but potentially better planners with more
//   knowledge of schema, distributed runtimes etc could be plugged
type Planner struct {
	schema string
	ds     datasource.RuntimeSchema
	where  *expr.SqlWhere
	tasks  []PlanTask
}

func NewSourcePlan(conf *datasource.RuntimeSchema, src *expr.SqlSource, isFinal bool) (*SourcePlan, error) {
	sp := &SourcePlan{SqlSource: src, Final: isFinal}
	err := sp.load(conf)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

func NewPlanner(schema string, stmt expr.SqlStatement, sys datasource.RuntimeSchema) (*Planner, expr.VisitStatus, error) {

	plan := &Planner{
		schema: schema,
		ds:     sys,
	}
	switch sql := stmt.(type) {
	case *expr.SqlSelect:
		plan.where = sql.Where
	}
	_, status, err := stmt.Accept(plan)
	//u.Debugf("task:  %T  %#v", task, task)
	if err != nil {
		return nil, status, err
	}

	return plan, status, nil
}

func (m *SourcePlan) load(conf *datasource.RuntimeSchema) error {
	//u.Debugf("SourcePlan.load()")
	fromName := strings.ToLower(m.SqlSource.SourceName())
	m.DataSource = conf.Sources.Get(fromName)
	if m.DataSource == nil {
		return fmt.Errorf("Could not find source for %v", m.SqlSource.SourceName())
	}

	tbl, err := conf.Table(fromName)
	if err != nil {
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
