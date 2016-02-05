package plan

import (
	"fmt"
	"strings"

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

		WalkSourceSelect(p *Source) error
		WalkProjectionSource(p *Source) error
		WalkProjectionFinal(p *Select) error
	}

	// Sources can often do their own planning for sub-select statements
	//  ie mysql can do its own (select, projection) mongo, es can as well
	// - provide interface to allow passing down select planning to source
	SourcePlanner interface {
		// given our request statement, turn that into a plan.Task.
		WalkSourceSelect(pl Planner, s *Source) (Task, error)
	}
)

type (
	PlanBase struct {
		parallel bool   // parallel or sequential?
		RootTask Task   // Root task
		tasks    []Task // Children tasks
	}
	PreparedStatement struct {
		*PlanBase
		Stmt *rel.PreparedStatement
	}
	Select struct {
		*PlanBase
		Stmt *rel.SqlSelect
		pb   []byte
	}
	Insert struct {
		*PlanBase
		Stmt   *rel.SqlInsert
		Source schema.Upsert
	}
	Upsert struct {
		*PlanBase
		Stmt   *rel.SqlUpsert
		Source schema.Upsert
	}
	Update struct {
		*PlanBase
		Stmt   *rel.SqlUpdate
		Source schema.Upsert
	}
	Delete struct {
		*PlanBase
		Stmt   *rel.SqlDelete
		Source schema.Deletion
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
		*PlanBase
		Final bool // Is this final projection or not?
		Stmt  *rel.SqlSelect
		Proj  *rel.Projection
	}

	// Within a Select query, it optionally has multiple sources such
	//   as sub-select, join, etc this is the plan for a each source
	Source struct {
		*PlanBase

		// Request Information, if cross-node distributed query must be serialized
		Ctx              *Context        // query context, shared across all parts of this request
		Stmt             *rel.SqlSource  // The sub-query statement (may have been rewritten)
		Proj             *rel.Projection // projection for this sub-query
		NeedsHashableKey bool            // do we need group-by, join, partition key for routing purposes?
		Final            bool            // Is this final projection or not?
		Join             bool            // Join?
		SourceExec       bool            // Source execution?
		ExecPlan         PlanProto       // If SourceExec has a plan?

		// Schema and underlying Source provider info, not serialized or transported
		DataSource   schema.DataSource    // The data source for this From
		Conn         schema.SourceConn    // Connection for this source, only for this source/task
		SourceSchema *schema.SourceSchema // Schema for this source/from
		Tbl          *schema.Table        // Table schema for this From
	}
	// Select INTO table
	Into struct {
		*PlanBase
		Stmt *rel.SqlInto
	}
	GroupBy struct {
		*PlanBase
		Stmt *rel.SqlSelect
	}
	// Where, pre-aggregation filter
	Where struct {
		*PlanBase
		Final bool
		Stmt  *rel.SqlSelect
	}
	// Having, post-aggregation filter
	Having struct {
		*PlanBase
		Stmt *rel.SqlSelect
	}
	// 2 source/input tasks for join
	JoinMerge struct {
		*PlanBase
		Left      Task
		Right     Task
		LeftFrom  *rel.SqlSource
		RightFrom *rel.SqlSource
		ColIndex  map[string]int
	}
	JoinKey struct {
		*PlanBase
		Source *Source
	}
)

// Walk given statement for given Planner to produce a query plan
//  which is a plan.Task and children, ie a DAG of tasks
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
func (m *PlanBase) Close() error         { return ErrNotImplemented }
func (m *PlanBase) Run() error           { return ErrNotImplemented }
func (m *PlanBase) IsParallel() bool     { return m.parallel }
func (m *PlanBase) IsSequential() bool   { return !m.parallel }
func (m *PlanBase) SetParallel()         { m.parallel = true }
func (m *PlanBase) SetSequential()       { m.parallel = false }
func (m *PlanBase) Walk(p Planner) error { return ErrNotImplemented }

func (m *Select) Walk(p Planner) error { return p.WalkSelect(m) }
func (m *PreparedStatement) Walk(p Planner) error {
	return p.WalkPreparedStatement(m)
}
func (m *Insert) Walk(p Planner) error   { return p.WalkInsert(m) }
func (m *Upsert) Walk(p Planner) error   { return p.WalkUpsert(m) }
func (m *Update) Walk(p Planner) error   { return p.WalkUpdate(m) }
func (m *Delete) Walk(p Planner) error   { return p.WalkDelete(m) }
func (m *Show) Walk(p Planner) error     { return p.WalkShow(m) }
func (m *Describe) Walk(p Planner) error { return p.WalkDescribe(m) }
func (m *Command) Walk(p Planner) error  { return p.WalkCommand(m) }
func (m *Source) Walk(p Planner) error   { return p.WalkSourceSelect(m) }

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

func NewSource(ctx *Context, stmt *rel.SqlSource, isFinal bool) (*Source, error) {
	s := &Source{Stmt: stmt, Ctx: ctx, Final: isFinal, PlanBase: NewPlanBase()}
	err := s.load()
	if err != nil {
		return nil, err
	}
	return s, nil
}
func NewSourceStaticPlan(ctx *Context) *Source {
	return &Source{Ctx: ctx, Final: true, PlanBase: NewPlanBase()}
}

// A parallel join merge, uses Key() as value to merge two different input channels
//
//   left source  ->
//                  \
//                    --  join  -->
//                  /
//   right source ->
//
func NewJoinMerge(l, r Task, lf, rf *rel.SqlSource) *JoinMerge {

	m := &JoinMerge{
		PlanBase: NewPlanBase(),
		ColIndex: make(map[string]int),
	}
	m.SetParallel()

	m.Left = l
	m.Right = r
	m.LeftFrom = lf
	m.RightFrom = rf

	// Build an index of source to destination column indexing
	for _, col := range lf.Source.Columns {
		//u.Debugf("left col:  idx=%d  key=%q as=%q col=%v parentidx=%v", len(m.colIndex), col.Key(), col.As, col.String(), col.ParentIndex)
		m.ColIndex[lf.Alias+"."+col.Key()] = col.ParentIndex
		//u.Debugf("left  colIndex:  %15q : idx:%d sidx:%d pidx:%d", m.leftStmt.Alias+"."+col.Key(), col.Index, col.SourceIndex, col.ParentIndex)
	}
	for _, col := range rf.Source.Columns {
		//u.Debugf("right col:  idx=%d  key=%q as=%q col=%v", len(m.colIndex), col.Key(), col.As, col.String())
		m.ColIndex[rf.Alias+"."+col.Key()] = col.ParentIndex
		//u.Debugf("right colIndex:  %15q : idx:%d sidx:%d pidx:%d", m.rightStmt.Alias+"."+col.Key(), col.Index, col.SourceIndex, col.ParentIndex)
	}

	return m
}
func NewJoinKey(s *Source) *JoinKey {
	return &JoinKey{Source: s, PlanBase: NewPlanBase()}
}
func NewWhere(stmt *rel.SqlSelect) *Where {
	return &Where{Stmt: stmt, PlanBase: NewPlanBase()}
}
func NewWhereFinal(stmt *rel.SqlSelect) *Where {
	return &Where{Stmt: stmt, Final: true, PlanBase: NewPlanBase()}
}
func NewHaving(stmt *rel.SqlSelect) *Having {
	return &Having{Stmt: stmt, PlanBase: NewPlanBase()}
}
func NewGroupBy(stmt *rel.SqlSelect) *GroupBy {
	return &GroupBy{Stmt: stmt, PlanBase: NewPlanBase()}
}

func (m *Source) load() error {
	u.Debugf("%p plan.Source.load()", m)
	fromName := strings.ToLower(m.Stmt.SourceName())
	ss, err := m.Ctx.Schema.Source(fromName)
	if err != nil {
		u.Errorf("no schema ? %v", err)
		return err
	}
	if ss == nil {
		u.Warnf("%p Schema  no %s found", m.Ctx.Schema, fromName)
		return fmt.Errorf("Could not find source for %v", m.Stmt.SourceName())
	}
	m.SourceSchema = ss
	m.DataSource = ss.DS

	tbl, err := m.Ctx.Schema.Table(fromName)
	if err != nil {
		u.Warnf("%p Schema %v", m.Ctx.Schema, fromName)
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
	u.Debugf("%p source has projection? %v", m, m.Proj != nil)
	return nil
}
