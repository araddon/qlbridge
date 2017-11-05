// Plan package converts the AST (expr package) into a plan, which is a DAG
// of tasks that comprise that plan, the planner is pluggable.
// The plan tasks are converted to executeable plan in exec.
package plan

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/golang/protobuf/proto"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ErrNotImplemented is plan specific error for not implemented
	ErrNotImplemented = fmt.Errorf("QLBridge.plan: not implemented")
	// ErrNoDataSource no datasource/type found
	ErrNoDataSource = fmt.Errorf("QLBridge.plan: No datasource found")
	// ErrNoPlan no plan
	ErrNoPlan = fmt.Errorf("No Plan")

	// Ensure our tasks implement Task Interface
	_ Task = (*PreparedStatement)(nil)
	_ Task = (*Select)(nil)
	_ Task = (*Insert)(nil)
	_ Task = (*Upsert)(nil)
	_ Task = (*Update)(nil)
	_ Task = (*Delete)(nil)
	_ Task = (*Command)(nil)
	_ Task = (*Create)(nil)
	_ Task = (*Projection)(nil)
	_ Task = (*Source)(nil)
	_ Task = (*Into)(nil)
	_ Task = (*Where)(nil)
	_ Task = (*Having)(nil)
	_ Task = (*GroupBy)(nil)
	_ Task = (*Order)(nil)
	_ Task = (*JoinMerge)(nil)
	_ Task = (*JoinKey)(nil)

	// Force any plan that participates in a Select to implement Proto
	//  which allows us to serialize and distribute to multiple nodes.
	_ Proto = (*Select)(nil)
)

type (
	// SchemaLoader func interface for loading schema.
	SchemaLoader func(name string) (*schema.Schema, error)
	// SelectTask interface to check equality
	SelectTask interface {
		Equal(Task) bool
	}
	// Proto interface to ensure implements protobuf marshalling.
	Proto interface {
		proto.Marshaler
		proto.Unmarshaler
	}

	// Task interface allows different portions of distributed
	// plans (where, group-by, source-scan, project) to have
	// its own planner.  Output is a DAG of tasks to be given
	// to executor.
	// - may be parallel or sequential
	// - must be serializeable to participate in cross network tasks
	Task interface {
		// Walk, give a planner to this task to allow
		// Task to call appropriate parts of planner.
		Walk(p Planner) error

		// Children tasks of this, this task may be participating
		// in parents.
		Children() []Task
		// Add a child to this dag
		Add(Task) error
		IsSequential() bool
		SetSequential()
		IsParallel() bool
		SetParallel()
		Equal(Task) bool
		ToPb() (*PlanPb, error)
	}

	// Planner interface for planners.  Planners take a statement
	// and walk the statement to create a DAG of tasks representing
	// necessary sub-tasks to fulfil statement.
	// implementations of planners:
	// - qlbridge/exec package implements a non-distributed query-planner
	// - dataux/planner implements a distributed query-planner
	Planner interface {
		// DML Statements
		WalkSelect(p *Select) error
		WalkInsert(p *Insert) error
		WalkUpsert(p *Upsert) error
		WalkUpdate(p *Update) error
		WalkDelete(p *Delete) error
		WalkInto(p *Into) error
		WalkSourceSelect(p *Source) error
		WalkProjectionSource(p *Source) error
		WalkProjectionFinal(p *Select) error

		// Other Statements
		WalkPreparedStatement(p *PreparedStatement) error
		WalkCommand(p *Command) error

		// DDL operations
		WalkCreate(p *Create) error
		WalkDrop(p *Drop) error
		WalkAlter(p *Alter) error
	}

	// SourcePlanner Sources can often do their own planning for sub-select statements
	// ie mysql can do its own (select, projection) mongo, es can as well
	// - provide interface to allow passing down select planning to source
	SourcePlanner interface {
		// given our request statement, turn that into a plan.Task.
		WalkSourceSelect(pl Planner, s *Source) (Task, error)
	}
)

type (
	// PlanBase holds dag of child tasks
	PlanBase struct {
		parallel bool   // parallel or sequential?
		RootTask Task   // Root task
		tasks    []Task // Children tasks
	}
	// PreparedStatement plan
	PreparedStatement struct {
		*PlanBase
		Stmt *rel.PreparedStatement
	}
	// Select plan
	Select struct {
		*PlanBase
		Ctx      *Context
		From     []*Source
		Stmt     *rel.SqlSelect
		ChildDag bool
		pbplan   *PlanPb
	}
	// Insert plan
	Insert struct {
		*PlanBase
		Stmt   *rel.SqlInsert
		Source schema.ConnUpsert
	}
	// Upsert task (not official sql) for sql Upsert.
	Upsert struct {
		*PlanBase
		Stmt   *rel.SqlUpsert
		Source schema.ConnUpsert
	}
	// Update plan for sql Update statements.
	Update struct {
		*PlanBase
		Stmt   *rel.SqlUpdate
		Source schema.ConnUpsert
	}
	// Delete plan for sql DELETE where
	Delete struct {
		*PlanBase
		Stmt   *rel.SqlDelete
		Source schema.ConnDeletion
	}
	// Command for sql commands like SET.
	Command struct {
		*PlanBase
		Ctx  *Context
		Stmt *rel.SqlCommand
	}
	// Projection holds original query for column info and schema/field types
	Projection struct {
		*PlanBase
		Final bool // Is this final projection or not?
		P     *Select
		Stmt  *rel.SqlSelect
		Proj  *rel.Projection
	}
	// Source defines a source Within a Select query, it optionally has multiple
	// sources such as sub-select, join, etc this is the plan for a each source
	Source struct {
		*PlanBase
		pbplan *PlanPb

		// Request Information, if cross-node distributed query must be serialized
		*SourcePb
		Stmt     *rel.SqlSource  // The sub-query statement (may have been rewritten)
		Proj     *rel.Projection // projection for this sub-query
		ExecPlan Proto           // If SourceExec has a plan?
		Custom   u.JsonHelper    // Source specific context info

		// Schema and underlying Source provider info, not serialized or transported
		ctx        *Context       // query context, shared across all parts of this request
		DataSource schema.Source  // The data source for this From
		Conn       schema.Conn    // Connection for this source, only for this source/task
		Schema     *schema.Schema // Schema for this source/from
		Tbl        *schema.Table  // Table schema for this From
		Static     []driver.Value // this is static data source
		Cols       []string
	}
	// Into Select INTO table
	Into struct {
		*PlanBase
		Stmt *rel.SqlInto
	}
	// GroupBy clause plan
	GroupBy struct {
		*PlanBase
		Stmt    *rel.SqlSelect
		Partial bool
	}
	// Order By clause
	Order struct {
		*PlanBase
		Stmt *rel.SqlSelect
	}
	// Where pre-aggregation filter
	Where struct {
		*PlanBase
		Final bool
		Stmt  *rel.SqlSelect
	}
	// Having post-aggregation filter plan.
	Having struct {
		*PlanBase
		Stmt *rel.SqlSelect
	}
	// JoinMerge 2 source/input tasks for join
	JoinMerge struct {
		*PlanBase
		Left      Task
		Right     Task
		LeftFrom  *rel.SqlSource
		RightFrom *rel.SqlSource
		ColIndex  map[string]int
	}
	// JoinKey plan
	JoinKey struct {
		*PlanBase
		Source *Source
	}

	// DDL Tasks

	// Create plan for CREATE {SCHEMA|SOURCE|DATABASE}
	Create struct {
		*PlanBase
		Ctx  *Context
		Stmt *rel.SqlCreate
	}
	// Drop plan for DROP {SCHEMA|SOURCE|DATABASE}
	Drop struct {
		*PlanBase
		Ctx  *Context
		Stmt *rel.SqlDrop
	}
	// Alter plan for ALTER {TABLE|COLUMN}
	Alter struct {
		*PlanBase
		Ctx  *Context
		Stmt *rel.SqlAlter
	}
)

// WalkStmt Walk given statement for given Planner to produce a query plan
// which is a plan.Task and children, ie a DAG of tasks
func WalkStmt(ctx *Context, stmt rel.SqlStatement, planner Planner) (Task, error) {
	var p Task
	base := NewPlanBase(false)
	switch st := stmt.(type) {
	case *rel.SqlSelect:
		p = &Select{Stmt: st, PlanBase: base, Ctx: ctx}
	case *rel.SqlInsert:
		p = &Insert{Stmt: st, PlanBase: base}
	case *rel.SqlUpsert:
		p = &Upsert{Stmt: st, PlanBase: base}
	case *rel.SqlUpdate:
		p = &Update{Stmt: st, PlanBase: base}
	case *rel.SqlDelete:
		p = &Delete{Stmt: st, PlanBase: base}
	case *rel.SqlShow:
		sel, err := RewriteShowAsSelect(st, ctx)
		if err != nil {
			return nil, err
		}
		ctx.Stmt = sel
		p = &Select{Stmt: sel, PlanBase: base, Ctx: ctx}
	case *rel.SqlDescribe:
		sel, err := RewriteDescribeAsSelect(st, ctx)
		if err != nil {
			return nil, err
		}
		ctx.Stmt = sel
		p = &Select{Stmt: sel, PlanBase: base}
	case *rel.PreparedStatement:
		p = &PreparedStatement{Stmt: st, PlanBase: base}
	case *rel.SqlCommand:
		p = &Command{Stmt: st, PlanBase: base, Ctx: ctx}
	case *rel.SqlCreate:
		p = &Create{Stmt: st, PlanBase: base, Ctx: ctx}
	case *rel.SqlDrop:
		p = &Drop{Stmt: st, PlanBase: base, Ctx: ctx}
	case *rel.SqlAlter:
		p = &Alter{Stmt: st, PlanBase: base, Ctx: ctx}
	default:
		panic(fmt.Sprintf("Not implemented for %T", stmt))
	}
	return p, p.Walk(planner)
}

// SelectPlanFromPbBytes Create a sql plan from pb.
func SelectPlanFromPbBytes(pb []byte, loader SchemaLoader) (*Select, error) {
	p := &PlanPb{}
	if err := proto.Unmarshal(pb, p); err != nil {
		u.Errorf("error reading protobuf select: %v  \n%s", err, pb)
		return nil, err
	}
	switch {
	case p.Select != nil:
		return SelectFromPB(p, loader)
	}
	return nil, ErrNotImplemented
}

// SelectTaskFromTaskPb create plan task for SqlSelect from pb.
func SelectTaskFromTaskPb(pb *PlanPb, ctx *Context, sel *rel.SqlSelect) (Task, error) {
	switch {
	case pb.Source != nil:
		return SourceFromPB(pb, ctx)
	case pb.Where != nil:
		return WhereFromPB(pb), nil
	case pb.Having != nil:
		return HavingFromPB(pb), nil
	case pb.GroupBy != nil:
		return GroupByFromPB(pb), nil
	case pb.Order != nil:
		return OrderFromPB(pb), nil
	case pb.Projection != nil:
		return ProjectionFromPB(pb, sel), nil
	case pb.JoinMerge != nil:
		u.Warnf("JoinMerge not implemented: %T", pb)
	case pb.JoinKey != nil:
		u.Warnf("JoinKey not implemented: %T", pb)
	default:
		u.Warnf("not implemented: %#v", pb)
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
		return true
	}
	if m == nil && p != nil {
		return false
	}
	if m != nil && p == nil {
		return false
	}

	if m.parallel != p.parallel {
		return false
	}
	if len(m.tasks) != len(p.tasks) {
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
func (m *Command) Walk(p Planner) error           { return p.WalkCommand(m) }
func (m *Source) Walk(p Planner) error            { return p.WalkSourceSelect(m) }
func (m *Create) Walk(p Planner) error            { return p.WalkCreate(m) }
func (m *Drop) Walk(p Planner) error              { return p.WalkDrop(m) }
func (m *Alter) Walk(p Planner) error             { return p.WalkAlter(m) }

// NewCreate creates a new Create Task plan.
func NewCreate(ctx *Context, stmt *rel.SqlCreate) *Create {
	return &Create{Stmt: stmt, PlanBase: NewPlanBase(false), Ctx: ctx}
}

// NewDrop create Drop plan task.
func NewDrop(ctx *Context, stmt *rel.SqlDrop) *Drop {
	return &Drop{Stmt: stmt, PlanBase: NewPlanBase(false), Ctx: ctx}
}

// NewAlter create Alter plan task.
func NewAlter(ctx *Context, stmt *rel.SqlAlter) *Alter {
	return &Alter{Stmt: stmt, PlanBase: NewPlanBase(false), Ctx: ctx}
}

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
		stmtPb := m.Stmt.ToPB()
		ctxpb := m.Ctx.ToPB()
		m.pbplan.Select = &SelectPb{Select: stmtPb, Context: ctxpb}
		//u.Infof("ctx %+v", m.pbplan.Select.Context)
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
	if !m.Ctx.Equal(s.Ctx) {
		return false
	}
	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	for i, t := range m.Children() {
		t2 := s.Children()[i]
		if !t2.Equal(t) {
			//u.Warnf("Not Equal?   %T  vs %T", t, t2)
			//u.Warnf("t!=t:   \n\t%#v \n\t%#v", t, t2)
			return false
		}
	}
	return true
}
func (m *Select) NeedsFinalProjection() bool {
	if m.Stmt.Limit > 0 {
		return true
	}
	return false
}
func (m *Select) IsSchemaQuery() bool {
	// For Single Source statements, lets see if they are switching schema
	if len(m.From) == 1 {
		//u.Debugf("schema:%q name:%q", m.From[0].Stmt.Schema, m.From[0].Stmt.Name)
		schemaName := strings.ToLower(m.From[0].Stmt.Schema)
		if schemaName == "context" || schemaName == "schema" {
			return true
		}
	}
	return false
}

func SelectFromPB(pb *PlanPb, loader SchemaLoader) (*Select, error) {
	m := Select{
		pbplan:   pb,
		ChildDag: true,
	}
	m.PlanBase = NewPlanBase(pb.Parallel)
	if pb.Select != nil {
		m.Stmt = rel.SqlSelectFromPb(pb.Select.Select)
		if pb.Select.Context != nil {
			//u.Infof("got context pb %+v", pb.Select.Context)
			m.Ctx = NewContextFromPb(pb.Select.Context)
			m.Ctx.Stmt = m.Stmt
			m.Ctx.Raw = m.Stmt.Raw
			sch, err := loader(m.Ctx.SchemaName)
			if err != nil {
				u.Errorf("could not load schema: %q  err=%v", m.Ctx.SchemaName, err)
				return nil, err
			}
			m.Ctx.Schema = sch
		}
	}
	if len(pb.Children) > 0 {
		m.tasks = make([]Task, len(pb.Children))
		for i, pbt := range pb.Children {
			//u.Infof("%+v", pbt)
			childPlan, err := SelectTaskFromTaskPb(pbt, m.Ctx, m.Stmt)
			if err != nil {
				u.Errorf("%+v not implemented? %v  %#v", pbt, err, pbt)
				return nil, err
			}
			switch cpt := childPlan.(type) {
			case *Source:
				m.From = append(m.From, cpt)
			}
			m.tasks[i] = childPlan
		}
	}
	return &m, nil
}

func SourceFromPB(pb *PlanPb, ctx *Context) (*Source, error) {
	m := Source{
		SourcePb: pb.Source,
		ctx:      ctx,
	}
	if len(pb.Source.Custom) > 0 {
		m.Custom = make(u.JsonHelper)
		if err := json.Unmarshal(pb.Source.Custom, &m.Custom); err != nil {
			u.Errorf("Could not unmarshall custom data %v", err)
		}
		//u.Debugf("custom %v", m.Custom)
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
			childPlan, err := SelectTaskFromTaskPb(pbt, ctx, m.Stmt.Source)
			if err != nil {
				u.Errorf("%T not implemented? %v", pbt, err)
				return nil, err
			}
			m.tasks[i] = childPlan
		}
	}

	err := m.load()
	if err != nil {
		u.Errorf("could not load? %v", err)
		return nil, err
	}
	if m.Conn == nil {
		err = m.LoadConn()
		if err != nil {
			u.Errorf("conn error? %v", err)
			return nil, err
		}
		if m.Conn == nil {
			if m.Stmt != nil {
				if m.Stmt.IsLiteral() {
					// this is fine
				} else {
					u.Warnf("no data source and not literal query? %s", m.Stmt.String())
					return nil, ErrNoDataSource
				}
			} else {
				//u.Warnf("hm  no conn, no stmt?....")
				//return nil, ErrNoDataSource
			}
		}
	}

	return &m, nil
}

// NewSource create a new plan Task for data source
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
func (m *Source) Context() *Context {
	return m.ctx
}
func (m *Source) LoadConn() error {

	//u.Debugf("LoadConn() nil?%v", m.Conn == nil)
	if m.Conn != nil {
		return nil
	}
	if m.DataSource == nil {
		// Not all sources require a source, ie literal queries
		// and some, information schema, or fully qualifyied schema queries
		// requires schema switching
		if m.IsSchemaQuery() && m.ctx != nil {
			m.ctx.Schema = m.ctx.Schema.InfoSchema
			u.Infof("switching to info schema")
			if err := m.load(); err != nil {
				u.Errorf("could not load schema? %v", err)
				return err
			}
			if m.DataSource == nil {
				return u.LogErrorf("could not load info schema source %v", m.Stmt)
			}
		} else {
			u.Debugf("return bc no datasource ctx=nil?%v schema?%v", m.ctx == nil, m.IsSchemaQuery())
			return nil
		}
	}
	source, err := m.DataSource.Open(m.Stmt.SourceName())
	if err != nil {
		u.Debugf("no source? %T for source %q", m.DataSource, m.Stmt.SourceName())
		return err
	}
	m.Conn = source
	return nil
}
func (m *Source) IsSchemaQuery() bool {
	if m.Stmt != nil && len(m.Stmt.Schema) > 0 {
		//u.Debugf("schema:%q name:%q", m.Stmt.Schema, m.Stmt.Name)
		schemaName := strings.ToLower(m.Stmt.Schema)
		if schemaName == "context" || schemaName == "schema" {
			return true
		}
	}
	return false
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
	if len(m.Custom) > 0 {
		by, err := json.Marshal(m.Custom)
		if err != nil {
			u.Errorf("Could not marshall custom source plan json %v", m.Custom)
		} else {
			m.SourcePb.Custom = by
		}
	}

	m.pbplan.Source = m.SourcePb
	return nil
}
func (m *Source) load() error {
	// u.Debugf("source load schema=%s from=%s  %#v", m.ctx.Schema.Name, m.Stmt.SourceName(), m.Stmt)
	if m.Stmt == nil {
		return nil
	}
	fromName := strings.ToLower(m.Stmt.SourceName())
	if m.ctx == nil {
		return fmt.Errorf("missing context in Source")
	}
	if m.ctx.Schema == nil {
		u.Errorf("missing schema in *plan.Source load() from:%q", fromName)
		return fmt.Errorf("Missing schema for %v", fromName)
	}

	ss, err := m.ctx.Schema.SchemaForTable(fromName)
	if err != nil {
		// u.Debugf("no schema found for %T  %q.%q ? err=%v", m.ctx.Schema, m.Stmt.Schema, fromName, err)
		return nil
	}
	if ss == nil {
		u.Warnf("%p Schema  no %s found", m.ctx.Schema, fromName)
		return fmt.Errorf("Could not find source for %v", m.Stmt.SourceName())
	}
	m.Schema = ss
	// Create a context-datasource
	m.DataSource = ss.DS

	tbl, err := m.ctx.Schema.Table(fromName)
	if err != nil {
		return err
	}
	if tbl == nil {
		return fmt.Errorf("No table found for %q", fromName)
	}
	m.Tbl = tbl

	//u.Infof("schema=%s ds:%T  tbl:%v", m.Schema.Name, m.DataSource, tbl)
	return projectionForSourcePlan(m)
}

// Equal checks if two tasks are equal.
func (m *Projection) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Projection)
	if !ok {
		return false
	}
	if m.Final != s.Final {
		return false
	}
	if !m.Proj.Equal(s.Proj) {
		return false
	}
	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}

// ToPb to protobuf.
func (m *Projection) ToPb() (*PlanPb, error) {
	pbp, err := m.PlanBase.ToPb()
	if err != nil {
		return nil, err
	}
	ppbptr := m.Proj.ToPB()
	ppcpy := *ppbptr
	ppcpy.Final = m.Final
	pbp.Projection = &ppcpy
	return pbp, nil
}

// ProjectionFromPB create Projection from Protobuf.
func ProjectionFromPB(pb *PlanPb, sel *rel.SqlSelect) *Projection {
	m := Projection{
		Proj: rel.ProjectionFromPb(pb.Projection),
	}
	m.Final = pb.Projection.Final
	m.PlanBase = NewPlanBase(pb.Parallel)
	m.Stmt = sel
	return &m
}

// NewJoinMerge A parallel join merge, uses Key() as value to merge
// two different input task/channels.
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

// NewJoinKey creates JoinKey from Source.
func NewJoinKey(s *Source) *JoinKey {
	return &JoinKey{Source: s, PlanBase: NewPlanBase(false)}
}

// NewWhere new Where Task from SqlSelect statement.
func NewWhere(stmt *rel.SqlSelect) *Where {
	return &Where{Stmt: stmt, PlanBase: NewPlanBase(false)}
}

// NewWhereFinal from SqlSelect statement.
func NewWhereFinal(stmt *rel.SqlSelect) *Where {
	return &Where{Stmt: stmt, Final: true, PlanBase: NewPlanBase(false)}
}

// NewHaving from SqlSelect statement.
func NewHaving(stmt *rel.SqlSelect) *Having {
	return &Having{Stmt: stmt, PlanBase: NewPlanBase(false)}
}

// NewGroupBy from SqlSelect statement.
func NewGroupBy(stmt *rel.SqlSelect) *GroupBy {
	return &GroupBy{Stmt: stmt, PlanBase: NewPlanBase(false)}
}

// NewOrder from SqlSelect statement.
func NewOrder(stmt *rel.SqlSelect) *Order {
	return &Order{Stmt: stmt, PlanBase: NewPlanBase(false)}
}

// Equal compares equality of two tasks.
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

func (m *Order) ToPb() (*PlanPb, error) {
	pbp, err := m.PlanBase.ToPb()
	if err != nil {
		return nil, err
	}
	pbp.Order = &OrderPb{Select: m.Stmt.ToPB()}
	return pbp, nil
}
func (m *Order) Equal(t Task) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		return false
	}
	if m != nil && t == nil {
		return false
	}
	s, ok := t.(*Order)
	if !ok {
		return false
	}

	if !m.PlanBase.EqualBase(s.PlanBase) {
		return false
	}
	return true
}
func OrderFromPB(pb *PlanPb) *Order {
	m := Order{
		Stmt: rel.SqlSelectFromPb(pb.Order.Select),
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
