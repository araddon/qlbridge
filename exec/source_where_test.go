package exec

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY
)

// We are testing joins serving as source to next join, ie folding one join
// into another, this is a type of brute-force/naive key-value oriented lookup
//
//    SELECT u.user_id, u.email, o.order_date FROM users AS u INNER JOIN orders as o
//        ON u.user_id = o.user_id
//
//   Plan:
//     - choose table with smaller row-count as source1 (say orders)
//     - select order_date, user_id from orders
//         -> feed rows from source1 into source2, use user_id to run
//               select user_id, email from user where user_id IN (1,2,3,4,5,.....X)
//               -> join naivemerge
func TestSourceWhereExpr(t *testing.T) {
	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id
		WHERE u.user_id = "9Ip1aKbeZe2njCDM";
	`
	msgs := make([]datasource.Message, 0)
	resultWriter := NewResultBuffer(&msgs)
	job := buildSource(t, rtConf, "mockcsv", sqlText, resultWriter)

	err := job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have gotten 2 messages but got %v", len(msgs))
}

// Create partial job of just source
func buildSource(t *testing.T, conf *datasource.RuntimeSchema, connInfo, sqlText string, rw *ResultBuffer) *SqlJob {

	stmt, err := expr.ParseSqlVm(sqlText)
	assert.T(t, err == nil)

	tasks := make(Tasks, 0)
	job := NewJobBuilder(conf, connInfo)

	sql := stmt.(*expr.SqlSelect)
	sql.Rewrite()

	// Note, we are doing a custom Job Plan here to
	//   isolate and test just the Source/Where
	task, err := job.VisitSubSelect(sql.From[0])
	assert.T(t, err == nil)

	tasks.Add(task.(TaskRunner))
	tasks.Add(rw)

	taskRoot := NewSequential("select", tasks)
	return &SqlJob{RootTask: taskRoot, Stmt: stmt, Conf: conf}
}
