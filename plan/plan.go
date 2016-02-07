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

	// Force Plans to implement Task
	_ Task = (*PreparedStatement)(nil)
	_ Task = (*Select)(nil)
	_ Task = (*Insert)(nil)
	_ Task = (*Upsert)(nil)
	_ Task = (*Update)(nil)
	_ Task = (*Delete)(nil)
	_ Task = (*Show)(nil)
	_ Task = (*Describe)(nil)
	_ Task = (*Command)(nil)
	_ Task = (*Projection)(nil)
	_ Task = (*Source)(nil)
	_ Task = (*Into)(nil)
	_ Task = (*Where)(nil)
	_ Task = (*Having)(nil)
	_ Task = (*GroupBy)(nil)
	_ Task = (*JoinMerge)(nil)
	_ Task = (*JoinKey)(nil)

	// Force any plan that participates in a Select to implement Proto
	//  which allows us to serialize and distribute to multiple nodes.
	_ PlanProto = (*Select)(nil)
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
	SelectTask interface {
		Equal(Task) bool
	}
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
		Equal(Task) bool
		ToPb() (*PlanPb, error)
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
		Stmt   *rel.SqlSelect
		pbplan *PlanPb
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
		pbplan *PlanPb

		// Request Information, if cross-node distributed query must be serialized
		*SourcePb
		Stmt     *rel.SqlSource  // The sub-query statement (may have been rewritten)
		Proj     *rel.Projection // projection for this sub-query
		ExecPlan PlanProto       // If SourceExec has a plan?

		// Schema and underlying Source provider info, not serialized or transported
		ctx          *Context             // query context, shared across all parts of this request
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
	base := NewPlanBase(false)
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

// Create a sql plan from pb
func SelectPlanFromPbBytes(pb []byte) (*Select, error) {
	p := &PlanPb{}
	if err := proto.Unmarshal(pb, p); err != nil {
		u.Errorf("crap: %v", err)
		return nil, err
	}
	switch {
	case p.Select != nil:
		return SelectFromPB(p)
	}
	return nil, ErrNotImplemented
}
func TaskFromTaskPb(pb *PlanPb) (Task, error) {
	switch {
	case pb.Source != nil:
		return SourceFromPB(pb)
	case pb.Where != nil:
		return WhereFromPB(pb), nil
	case pb.Having != nil:
		return HavingFromPB(pb), nil
	case pb.GroupBy != nil:
		return GroupByFromPB(pb), nil
	case pb.JoinMerge != nil:
		u.Warnf("JoinMerge not implemented: %T", pb)
	case pb.JoinKey != nil:
		u.Warnf("JoinKey not implemented: %T", pb)
	default:
		u.Warnf("not implemented: %T", pb)
	}
	return nil, ErrNotImplemented
}
func NewPlanBase(isParallel bool) *PlanBase {
	return &PlanBase{tasks: make([]Task, 0), parallel: isParallel}
}
func (m *PlanBase) Children() []Task { return m.tasks }
func (m *PlanBase) Add(task Task) error {
	m.tasks = append(m.tasks, task)
	return nil
}
func (m *PlanBase) Close() error       { return ErrNotImplemented }
func (m *PlanBase) Run() error         { return ErrNotImplemented }
func (m *PlanBase) IsParallel() bool   { return m.parallel }
func (m *PlanBase) IsSequential() bool { return !m.parallel }
func (m *PlanBase) SetParallel()       { m.parallel = true }
func (m *PlanBase) SetSequential()     { m.parallel = false }
func (m *PlanBase) ToPb() (*PlanPb, error) {
	pbp := &PlanPb{}
	if len(m.tasks) > 0 {
		pbp.Children = make([]*PlanPb, len(m.tasks))
		for i, t := range m.tasks {
			childPlan, err := t.ToPb()
			if err != nil {
				u.Errorf("%T not implemented? %v", t, err)
				return nil, err
			}
			pbp.Children[i] = childPlan
		}
	}
	return pbp, nil
}
func (m *PlanBase) Equal(t Task) bool { return false }
func (m *PlanBase) EqualBase(p *PlanBase) bool {
	if m == nil && p == nil {
		u.Warnf("wat nil!?")
		return true
	}
	if m == nil && p != nil {
		u.Warnf("wat not nil=?")
		return false
	}
	if m != nil && p == nil {
		u.Warnf("wat not nil=? 222")
		return false
	}

	if m.parallel != p.parallel {
		u.Warnf("wat parallel!?")
		return false
	}
	if len(m.tasks) != len(p.tasks) {
		u.Warnf("ah, recursive kids!? not equal?")
		return false
	}
	return true
}

func (m *PlanBase) Walk(p Planner) error          { return ErrNotImplemented }
func (m *Select) Walk(p Planner) error            { return p.WalkSelect(m) }
func (m *PreparedStatement) Walk(p Planner) error { return p.WalkPreparedStatement(m) }
func (m *Insert) Walk(p Planner) error            { return p.WalkInsert(m) }
func (m *Upsert) Walk(p Planner) error            { return p.WalkUpsert(m) }
func (m *Update) Walk(p Planner) error            { return p.WalkUpdate(m) }
func (m *Delete) Walk(p Planner) error            { return p.WalkDelete(m) }
func (m *Show) Walk(p Planner) error              { return p.WalkShow(m) }
func (m *Describe) Walk(p Planner) error          { return p.WalkDescribe(m) }
func (m *Command) Walk(p Planner) error           { return p.WalkCommand(m) }
func (m *Source) Walk(p Planner) error            { return p.WalkSourceSelect(m) }

func (m *Select) Marshal() ([]byte, error) {
	err := m.serializeToPb()
	if err != nil {
		return nil, err
	}
	return m.pbplan.Marshal()
}
func (m *Select) MarshalTo(data []byte) (int, error) {
	err := m.serializeToPb()
	if err != nil {
		return 0, err
	}
	return m.pbplan.MarshalTo(data)
}
func (m *Select) Size() (n int) {
	m.serializeToPb()
	return m.pbplan.Size()
}
func (m *Select) Unmarshal(data []byte) error {
	if m.pbplan == nil {
		m.pbplan = &PlanPb{Select: &SelectPb{}}
	}
	return m.pbplan.Unmarshal(data)
}
func (m *Select) serializeToPb() error {
	if m.pbplan == nil {
		pbp, err := m.PlanBase.ToPb()
		if err != nil {
			return err
		}
		m.pbplan = pbp
	}
	if m.pbplan.Select == nil && m.Stmt != nil {
		m.pbplan.Select = &SelectPb{Select: m.Stmt.ToPB()}
	}
	return nil
}
func (m *Select) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Select)
	if !ok {
		return false
	}

	if !m.Stmt.Equal(s.Stmt) {
		return false
	}
	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	for i, t := range m.Children() {
		t2 := s.Children()[i]
		if !t2.Equal(t) {
			u.Warnf("hmm   %T  vs %T", t, t2)
			u.Warnf("t!=t:   \n\t%#t \n\t!= %#t", t, t2)
			return false
		}
	}
	return true
}

func SelectFromPB(pb *PlanPb) (*Select, error) {
	m := Select{
		pbplan: pb,
	}
	m.PlanBase = NewPlanBase(pb.Parallel)
	if pb.Select != nil {
		m.Stmt = rel.SqlSelectFromPb(pb.Select.Select)
	}
	if len(pb.Children) > 0 {
		m.tasks = make([]Task, len(pb.Children))
		for i, pbt := range pb.Children {
			childPlan, err := TaskFromTaskPb(pbt)
			if err != nil {
				u.Errorf("%T not implemented? %v", pbt, err)
				return nil, err
			}
			m.tasks[i] = childPlan
		}
	}
	return &m, nil
}

func (m *Source) ToPb() (*PlanPb, error) {
	m.serializeToPb()
	return m.pbplan, nil
}
func (m *Source) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Source)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		u.Warnf("wtf planbase not equal")
		return false
	}
	return true
}
func (m *Source) Marshal() ([]byte, error) {
	m.serializeToPb()
	return m.SourcePb.Marshal()
}
func (m *Source) MarshalTo(data []byte) (n int, err error) {
	m.serializeToPb()
	return m.SourcePb.MarshalTo(data)
}
func (m *Source) Size() (n int) {
	m.serializeToPb()
	return m.SourcePb.Size()
}
func (m *Source) Unmarshal(data []byte) error {
	if m.SourcePb == nil {
		m.SourcePb = &SourcePb{}
	}
	return m.SourcePb.Unmarshal(data)
}
func (m *Source) serializeToPb() error {
	if m.pbplan == nil {
		pbp, err := m.PlanBase.ToPb()
		if err != nil {
			return err
		}
		m.pbplan = pbp
	}
	if m.SourcePb.Projection == nil && m.Proj != nil {
		m.SourcePb.Projection = m.Proj.ToPB()
	}
	if m.SourcePb.SqlSource == nil && m.Stmt != nil {
		m.SourcePb.SqlSource = m.Stmt.ToPB()
	}
	m.pbplan.Source = m.SourcePb
	return nil
}
func SourceFromPB(pb *PlanPb) (*Source, error) {
	m := Source{
		SourcePb: pb.Source,
	}
	if pb.Source.Projection != nil {
		m.Proj = rel.ProjectionFromPb(pb.Source.Projection)
	}
	if pb.Source.SqlSource != nil {
		m.Stmt = rel.SqlSourceFromPb(pb.Source.SqlSource)
	}
	m.PlanBase = NewPlanBase(pb.Parallel)
	if len(pb.Children) > 0 {
		m.tasks = make([]Task, len(pb.Children))
		for i, pbt := range pb.Children {
			childPlan, err := TaskFromTaskPb(pbt)
			if err != nil {
				u.Errorf("%T not implemented? %v", pbt, err)
				return nil, err
			}
			m.tasks[i] = childPlan
		}
	}
	return &m, nil
}

func NewSource(ctx *Context, stmt *rel.SqlSource, isFinal bool) (*Source, error) {
	s := &Source{Stmt: stmt, ctx: ctx, SourcePb: &SourcePb{Final: isFinal}, PlanBase: NewPlanBase(false)}
	err := s.load()
	if err != nil {
		return nil, err
	}
	return s, nil
}
func NewSourceStaticPlan(ctx *Context) *Source {
	return &Source{ctx: ctx, SourcePb: &SourcePb{Final: true}, PlanBase: NewPlanBase(false)}
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
		PlanBase: NewPlanBase(false),
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
	return &JoinKey{Source: s, PlanBase: NewPlanBase(false)}
}
func NewWhere(stmt *rel.SqlSelect) *Where {
	return &Where{Stmt: stmt, PlanBase: NewPlanBase(false)}
}
func NewWhereFinal(stmt *rel.SqlSelect) *Where {
	return &Where{Stmt: stmt, Final: true, PlanBase: NewPlanBase(false)}
}
func NewHaving(stmt *rel.SqlSelect) *Having {
	return &Having{Stmt: stmt, PlanBase: NewPlanBase(false)}
}
func NewGroupBy(stmt *rel.SqlSelect) *GroupBy {
	return &GroupBy{Stmt: stmt, PlanBase: NewPlanBase(false)}
}

func (m *Source) load() error {
	fromName := strings.ToLower(m.Stmt.SourceName())
	ss, err := m.ctx.Schema.Source(fromName)
	if err != nil {
		u.Errorf("no schema ? %v", err)
		return err
	}
	if ss == nil {
		u.Warnf("%p Schema  no %s found", m.ctx.Schema, fromName)
		return fmt.Errorf("Could not find source for %v", m.Stmt.SourceName())
	}
	m.SourceSchema = ss
	m.DataSource = ss.DS

	tbl, err := m.ctx.Schema.Table(fromName)
	if err != nil {
		u.Warnf("%p Schema %v", m.ctx.Schema, fromName)
		u.Errorf("could not get table: %v", err)
		return err
	}
	if tbl == nil {
		//u.Errorf("no table? %v", fromName)
		return fmt.Errorf("No table found for %q", fromName)
	}
	m.Tbl = tbl
	return projecectionForSourcePlan(m)
}

func (m *Into) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Into)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
func (m *Where) ToPb() (*PlanPb, error) {
	pbp, err := m.PlanBase.ToPb()
	if err != nil {
		return nil, err
	}
	pbp.Where = &WherePb{Select: m.Stmt.ToPB()}
	return pbp, nil
}
func (m *Where) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Where)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
func WhereFromPB(pb *PlanPb) *Where {
	m := Where{
		Final: pb.Where.Final,
		Stmt:  rel.SqlSelectFromPb(pb.Where.Select),
	}
	m.PlanBase = NewPlanBase(pb.Parallel)
	return &m
}

func (m *Having) ToPb() (*PlanPb, error) {
	pbp, err := m.PlanBase.ToPb()
	if err != nil {
		return nil, err
	}
	pbp.Having = &HavingPb{Select: m.Stmt.ToPB()}
	return pbp, nil
}
func (m *Having) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Having)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
func HavingFromPB(pb *PlanPb) *Having {
	m := Having{
		Stmt: rel.SqlSelectFromPb(pb.Having.Select),
	}
	m.PlanBase = NewPlanBase(pb.Parallel)
	return &m
}

func (m *GroupBy) ToPb() (*PlanPb, error) {
	pbp, err := m.PlanBase.ToPb()
	if err != nil {
		return nil, err
	}
	pbp.GroupBy = &GroupByPb{Select: m.Stmt.ToPB()}
	return pbp, nil
}
func (m *GroupBy) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*GroupBy)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
func GroupByFromPB(pb *PlanPb) *GroupBy {
	m := GroupBy{
		Stmt: rel.SqlSelectFromPb(pb.GroupBy.Select),
	}
	m.PlanBase = NewPlanBase(pb.Parallel)
	return &m
}

func (m *JoinMerge) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*JoinMerge)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
func (m *JoinKey) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*JoinKey)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
