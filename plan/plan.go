package plan

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var _ = u.EMPTY

type (
	// Sources can often do their own planning for sub-select statements
	//  ie mysql can do its own select, projection mongo can as well
	// - provide interface to allow passing down selection to source
	SourceSelectPlanner interface {
		// given our plan, turn that into a Task.
		// - if VisitStatus is not Final then we need to poly-fill
		VisitSourceSelect(sourcePlan *Source) (Task, rel.VisitStatus, error)
	}
	Plan interface {
	}
	Planner interface {
		Accept(visitor Visitor) (Task, rel.VisitStatus, error)
	}
)

/*
type Visitor interface {
	VisitPreparedStmt(stmt *PreparedStatement) (Task, rel.VisitStatus, error)
	VisitSelect(stmt *Select) (Task, rel.VisitStatus, error)
	VisitInsert(stmt *Insert) (Task, rel.VisitStatus, error)
	VisitUpsert(stmt *Upsert) (Task, rel.VisitStatus, error)
	VisitUpdate(stmt *Update) (Task, rel.VisitStatus, error)
	VisitDelete(stmt *Delete) (Task, rel.VisitStatus, error)
	VisitShow(stmt *Show) (Task, rel.VisitStatus, error)
	VisitDescribe(stmt *Describe) (Task, rel.VisitStatus, error)
	VisitCommand(stmt *Command) (Task, rel.VisitStatus, error)
	VisitInto(stmt *Into) (Task, rel.VisitStatus, error)

	// Select Components
	VisitWhere(stmt *Select) (Task, rel.VisitStatus, error)
	VisitHaving(stmt *Select) (Task, rel.VisitStatus, error)
	VisitGroupBy(stmt *Select) (Task, rel.VisitStatus, error)
	VisitProjection(stmt *Select) (Task, rel.VisitStatus, error)
	//VisitMutateWhere(stmt *Where) (Task, rel.VisitStatus, error)
}
// Interface for sub-select Tasks of the Select Statement
type SourceVisitor interface {
	VisitSourceSelect() (Task, rel.VisitStatus, error)
	VisitSource(scanner schema.Scanner) (Task, rel.VisitStatus, error)
	VisitSourceJoin(scanner schema.Scanner) (Task, rel.VisitStatus, error)
	VisitWhere() (Task, rel.VisitStatus, error)
}

*/

type (
	PreparedStatement struct {
		Stmt *rel.PreparedStatement
	}
	Select struct {
		Stmt *rel.SqlSelect
	}
	Insert struct {
		Stmt *rel.SqlInsert
	}
	Upsert struct {
		Stmt *rel.SqlUpsert
	}
	Update struct {
		Stmt *rel.SqlUpdate
	}
	Delete struct {
		Stmt *rel.SqlDelete
	}

	Show struct {
		Stmt *rel.SqlShow
	}
	Describe struct {
		Stmt *rel.SqlDescribe
	}
	Command struct {
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
)

func NewPlanner(stmt rel.SqlStatement) Planner {
	switch st := stmt.(type) {
	case *rel.SqlSelect:
		return &Select{Stmt: st}
	case *rel.PreparedStatement:
		return &PreparedStatement{Stmt: st}
	case *rel.SqlInsert:
		return &Insert{Stmt: st}
	case *rel.SqlUpsert:
		return &Upsert{Stmt: st}
	case *rel.SqlUpdate:
		return &Update{Stmt: st}
	case *rel.SqlDelete:
		return &Delete{Stmt: st}
	case *rel.SqlShow:
		return &Show{Stmt: st}
	case *rel.SqlDescribe:
		return &Describe{Stmt: st}
	case *rel.SqlCommand:
		return &Command{Stmt: st}
	}
	panic(fmt.Sprintf("Not implemented for %T", stmt))
}

func (m *Select) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitSelect(m)
}
func (m *PreparedStatement) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitPreparedStatement(m)
}
func (m *Insert) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitInsert(m)
}
func (m *Upsert) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitUpsert(m)
}
func (m *Update) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitUpdate(m)
}
func (m *Delete) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitDelete(m)
}
func (m *Show) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitShow(m)
}
func (m *Describe) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitDescribe(m)
}
func (m *Command) Accept(visitor Visitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitCommand(m)
}
