package plan

import (
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

// Visitor defines the Visit Pattern, so our planner package can
//   expect implementations from downstream packages
//   in our case:
//         qlbridge/exec package implements a non-distributed query-planner
//         dataux/planner implements a distributed query-planner
//
type Visitor interface {
	VisitPreparedStatement(stmt *PreparedStatement) (Task, rel.VisitStatus, error)
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
	VisitSourceSelect(sp *Source) (Task, rel.VisitStatus, error)
	VisitSource(scanner schema.Scanner) (Task, rel.VisitStatus, error)
	VisitSourceJoin(scanner schema.Scanner) (Task, rel.VisitStatus, error)
	VisitWhere() (Task, rel.VisitStatus, error)
}
