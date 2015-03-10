package exec

import (
	"github.com/araddon/qlbridge/expr"
)

// exec.Visitor implements standard Sql Visit() patterns to create
//  a job Builder.
// An implementation of Visitor() will be be able to execute/run a Statement
type Visitor interface {
	VisitPreparedStmt(stmt *expr.PreparedStatement) (interface{}, error)
	VisitSelect(stmt *expr.SqlSelect) (interface{}, error)
	VisitInsert(stmt *expr.SqlInsert) (interface{}, error)
	VisitDelete(stmt *expr.SqlDelete) (interface{}, error)
	VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error)
	VisitShow(stmt *expr.SqlShow) (interface{}, error)
	VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error)
}
