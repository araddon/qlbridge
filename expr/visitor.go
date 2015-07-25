package expr

// Visitor defines the Visit Pattern, so our expr package can
//   expect implementations from downstream packages
//   in our case, a planner.
//
type Visitor interface {
	VisitPreparedStmt(stmt *PreparedStatement) (interface{}, error)
	VisitSelect(stmt *SqlSelect) (interface{}, error)
	VisitInsert(stmt *SqlInsert) (interface{}, error)
	VisitUpsert(stmt *SqlUpsert) (interface{}, error)
	VisitUpdate(stmt *SqlUpdate) (interface{}, error)
	VisitDelete(stmt *SqlDelete) (interface{}, error)
	VisitShow(stmt *SqlShow) (interface{}, error)
	VisitDescribe(stmt *SqlDescribe) (interface{}, error)
	VisitCommand(stmt *SqlCommand) (interface{}, error)
}

// Interface for sub-Tasks of the Select Statement, joins, sub-selects
type SubVisitor interface {
	VisitSubselect(stmt *SqlSource) (interface{}, error)
	VisitJoin(stmt *SqlSource) (interface{}, error)
}
