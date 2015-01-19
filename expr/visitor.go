package expr

type Visitor interface {
	VisitPreparedStmt(stmt *PreparedStatement) (interface{}, error)
	VisitSelect(stmt *SqlSelect) (interface{}, error)
	VisitInsert(stmt *SqlInsert) (interface{}, error)
	//VisitUpsert(stmt *SqlUpsert) (interface{}, error)
	VisitDelete(stmt *SqlDelete) (interface{}, error)
	VisitUpdate(stmt *SqlUpdate) (interface{}, error)
	VisitShow(stmt *SqlShow) (interface{}, error)
	VisitDescribe(stmt *SqlDescribe) (interface{}, error)
}

type TaskVisitor interface {
	// 	VisitSubselect(task *Subselect) (interface{}, error)
	// 	VisitKeyspaceTerm(task *KeyspaceTerm) (interface{}, error)
	// 	VisitJoin(task *Join) (interface{}, error)
	// 	VisitNest(task *Nest) (interface{}, error)
	// 	VisitUnnest(task *Unnest) (interface{}, error)
	// 	VisitUnion(task *Union) (interface{}, error)
	// 	VisitUnionAll(task *UnionAll) (interface{}, error)
	// 	VisitIntersect(task *Intersect) (interface{}, error)
	// 	VisitIntersectAll(task *IntersectAll) (interface{}, error)
	// 	VisitExcept(task *Except) (interface{}, error)
	// 	VisitExceptAll(task *ExceptAll) (interface{}, error)
}
