package plan

import (
	"encoding/json"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the sql expr.Visitor interface
	_ expr.Visitor = (*Planner)(nil)
)

// A PlanTask is a part of a Plan, each task may have children
//
type PlanTask interface {
	json.Marshaler
	json.Unmarshaler
	Accept(visitor expr.Visitor) (interface{}, error)
	Clone() PlanTask
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

func NewPlanner(schema string, stmt expr.SqlStatement, sys datasource.RuntimeSchema) (*Planner, error) {

	plan := &Planner{
		schema: schema,
		ds:     sys,
	}
	switch sql := stmt.(type) {
	case *expr.SqlSelect:
		plan.where = sql.Where
	}
	task, err := stmt.Accept(plan)
	u.Debugf("task:  %T  %#v", task, task)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (m *Planner) VisitSelect(stmt *expr.SqlSelect) (expr.Task, error) {
	u.Debugf("VisitSource %+v", stmt)
	return nil, nil
}

func (m *Planner) VisitInsert(stmt *expr.SqlInsert) (expr.Task, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *Planner) VisitDelete(stmt *expr.SqlDelete) (expr.Task, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *Planner) VisitUpdate(stmt *expr.SqlUpdate) (expr.Task, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *Planner) VisitUpsert(stmt *expr.SqlUpsert) (expr.Task, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *Planner) VisitShow(stmt *expr.SqlShow) (expr.Task, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *Planner) VisitDescribe(stmt *expr.SqlDescribe) (expr.Task, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *Planner) VisitPreparedStmt(stmt *expr.PreparedStatement) (expr.Task, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, expr.ErrNotImplemented
}
func (m *Planner) VisitCommand(stmt *expr.SqlCommand) (expr.Task, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, expr.ErrNotImplemented
}
