package plan

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/golang/protobuf/proto"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY

	ErrNotImplemented = fmt.Errorf("QLBridge.plan: not implemented")

	// Force structs to implement interfaces
	_ PlanProto = (*Select)(nil)

	// Force Plans to implement Walk
	_ Task = (*Select)(nil)
	_ Task = (*PreparedStatement)(nil)

	// Force SourcePlans to implement
	_ Task = (*Source)(nil)
)

// WalkStatus surfaces status to visit builders
// if visit was completed, successful or needs to be polyfilled
type WalkStatus int

const (
	WalkUnknown  WalkStatus = 0 // not used
	WalkError    WalkStatus = 1 // error
	WalkFinal    WalkStatus = 2 // final, no more building needed
	WalkContinue WalkStatus = 3 // continue visit
)

type (
	PlanProto interface {
		proto.Marshaler
		proto.Unmarshaler
	}

	// Plan Tasks are inherently DAG's of task's implementing
	//  a rel.Task interface
	Task interface {
		Walk(p Planner) error
		WalkStatus(p Planner) (WalkStatus, error)

		// TODO, move to exec.Task
		Run() error
		Close() error

		Children() []Task // children sub-tasks
		Add(Task) error   // Add a child to this dag

		IsSequential() bool
		SetSequential()
		IsParallel() bool
		SetParallel()
	}

	// Task factory creates a task allowing different execution environments
	//     do different task run times
	//
	// - single server channel oriented
	// - in process message passing (not channel)
	// - multi-server message oriented
	// - multi-server file-passing
	TaskPlanner interface {
		// Create a source visitior, aka sub-job-builder
		SourcePlannerMaker(*Source) SourcePlanner
		Sequential(name string) Task
		Parallel(name string) Task
	}

	// Sources can often do their own planning for sub-select statements
	//  ie mysql can do its own select, projection mongo can as well
	// - provide interface to allow passing down selection to source
	SourceSelectPlanner interface {
		// given our plan, turn that into a Task.
		// - if WalkStatus is not Final then we need to poly-fill
		WalkSourceSelect(sourcePlan *Source) error
	}

	// Planner defines the planner interfaces, so our planner package can
	//   expect implementations from downstream packages
	//   in our case:
	//         qlbridge/exec package implements a non-distributed query-planner
	//         dataux/planner implements a distributed query-planner
	//
	Planner interface {
		WalkPreparedStatement(p *PreparedStatement) error
		WalkSelect(p *Select) error
		WalkInsert(p *Insert) error
		WalkUpsert(p *Upsert) error
		WalkUpdate(p *Update) error
		WalkDelete(p *Delete) error
		WalkShow(p *Show) error
		WalkDescribe(p *Describe) error
		WalkCommand(p *Command) error
		WalkInto(p *Into) error

		WalkSourceSelect(s *Source) (WalkStatus, error)
	}

	PlannerSubTasks interface {
		// Select Components
		VisitWhere(stmt *Select) (WalkStatus, error)
		VisitHaving(stmt *Select) (WalkStatus, error)
		VisitGroupBy(stmt *Select) (WalkStatus, error)
		VisitProjection(stmt *Select) (WalkStatus, error)
		//VisitMutateWhere(stmt *Where) (WalkStatus, error)
	}

	// Interface for sub-select Tasks of the Select Statement
	SourcePlanner interface {
		WalkSource(scanner schema.Scanner) (WalkStatus, error)
		WalkSourceJoin(scanner schema.Scanner) (WalkStatus, error)
		WalkWhere() (WalkStatus, error)
	}
)

type (
	PlanBase struct {
		parallel bool
		RootTask Task
		tasks    []Task
	}
	PreparedStatement struct {
		*PlanBase
		Stmt *rel.PreparedStatement
	}
	Select struct {
		*PlanBase
		Stmt *rel.SqlSelect
	}
	Insert struct {
		*PlanBase
		Stmt *rel.SqlInsert
	}
	Upsert struct {
		*PlanBase
		Stmt *rel.SqlUpsert
	}
	Update struct {
		*PlanBase
		Stmt *rel.SqlUpdate
	}
	Delete struct {
		*PlanBase
		Stmt *rel.SqlDelete
	}
	Show struct {
		*PlanBase
		Stmt *rel.SqlShow
	}
	Describe struct {
		*PlanBase
		Stmt *rel.SqlDescribe
	}
	Command struct {
		*PlanBase
		Stmt *rel.SqlCommand
	}

	// Projection holds original query for column info and schema/field types
	Projection struct {
		Sql  *rel.SqlSelect
		Proj *rel.Projection
	}

	// Within a Select query, it optionally has multiple sources such
	//   as sub-select, join, etc this is the plan for a each source
	Source struct {
		// Task info
		*PlanBase

		// Request Information, if cross-node distributed query must be serialized
		Ctx              *Context        // query context, shared across all parts of this request
		From             *rel.SqlSource  // The sub-query statement (may have been rewritten)
		Proj             *rel.Projection // projection for this sub-query
		NeedsHashableKey bool            // do we need group-by, join, partition key for routing purposes?
		Final            bool            // Is this final projection or not?

		// Schema and underlying Source provider info, not serialized/transported
		DataSource   schema.DataSource    // The data source for this From
		SourceSchema *schema.SourceSchema // Schema for this source/from
		Tbl          *schema.Table        // Table schema for this From
	}
	Into struct {
		Stmt *rel.SqlInto
	}
	Where struct {
		Stmt *rel.SqlWhere
	}
	Having struct {
		Stmt *rel.SqlSelect
	}
)

func WalkStmt(stmt rel.SqlStatement, planner Planner) (Task, error) {
	var p Task
	base := NewPlanBase()
	switch st := stmt.(type) {
	case *rel.SqlSelect:
		p = &Select{Stmt: st, PlanBase: base}
	case *rel.PreparedStatement:
		p = &PreparedStatement{Stmt: st, PlanBase: base}
	case *rel.SqlInsert:
		p = &Insert{Stmt: st, PlanBase: base}
	case *rel.SqlUpsert:
		p = &Upsert{Stmt: st, PlanBase: base}
	case *rel.SqlUpdate:
		p = &Update{Stmt: st, PlanBase: base}
	case *rel.SqlDelete:
		p = &Delete{Stmt: st, PlanBase: base}
	case *rel.SqlShow:
		p = &Show{Stmt: st, PlanBase: base}
	case *rel.SqlDescribe:
		p = &Describe{Stmt: st, PlanBase: base}
	case *rel.SqlCommand:
		p = &Command{Stmt: st, PlanBase: base}
	default:
		panic(fmt.Sprintf("Not implemented for %T", stmt))
	}
	return p, p.Walk(planner)
}
func NewPlanBase() *PlanBase {
	return &PlanBase{tasks: make([]Task, 0)}
}
func (m *PlanBase) Children() []Task { return m.tasks }
func (m *PlanBase) Add(task Task) error {
	m.tasks = append(m.tasks, task)
	return nil
}
func (m *PlanBase) Close() error                             { return ErrNotImplemented }
func (m *PlanBase) Run() error                               { return ErrNotImplemented }
func (m *PlanBase) IsParallel() bool                         { return m.parallel }
func (m *PlanBase) IsSequential() bool                       { return !m.parallel }
func (m *PlanBase) SetParallel()                             { m.parallel = true }
func (m *PlanBase) SetSequential()                           { m.parallel = false }
func (m *PlanBase) Walk(p Planner) error                     { return ErrNotImplemented }
func (m *PlanBase) WalkStatus(p Planner) (WalkStatus, error) { return WalkError, ErrNotImplemented }

func (m *Select) Walk(p Planner) error { return p.WalkSelect(m) }
func (m *PreparedStatement) Walk(p Planner) error {
	return p.WalkPreparedStatement(m)
}
func (m *Insert) Walk(p Planner) error                     { return p.WalkInsert(m) }
func (m *Upsert) Walk(p Planner) error                     { return p.WalkUpsert(m) }
func (m *Update) Walk(p Planner) error                     { return p.WalkUpdate(m) }
func (m *Delete) Walk(p Planner) error                     { return p.WalkDelete(m) }
func (m *Show) Walk(p Planner) error                       { return p.WalkShow(m) }
func (m *Describe) Walk(p Planner) error                   { return p.WalkDescribe(m) }
func (m *Command) Walk(p Planner) error                    { return p.WalkCommand(m) }
func (m *Source) WalkStatus(p Planner) (WalkStatus, error) { return p.WalkSourceSelect(m) }

// func (m *Source) Marshal() ([]byte, error)                 { return nil, nil }
// func (m *Source) MarshalTo(data []byte) (n int, err error) { return 0, nil }
// func (m *Source) Unmarshal(data []byte) error              { return nil }

func (m *Select) Marshal() ([]byte, error)                 { return nil, nil }
func (m *Select) MarshalTo(data []byte) (n int, err error) { return 0, nil }
func (m *Select) Unmarshal(data []byte) error              { return nil }
func (m *Select) Size() (n int) {
	// var l int
	// _ = l
	// l = len(m.K)
	// n += 1 + l + sovSql(uint64(l))
	// n += 1 + sovSql(uint64(m.V))
	return m.Stmt.ToPB().Size()
}
