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

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func buildSourceWhere(t *testing.T, conf *datasource.RuntimeSchema, connInfo, sqlText string) *SqlJob {

	stmt, err := expr.ParseSqlVm(sqlText)
	assert.T(t, err == nil)

	tasks := make(Tasks, 0)
	job := NewJobBuilder(conf, connInfo)

	sql := stmt.(*expr.SqlSelect)
	sql.Rewrite()

	// Note, we are doing a custom Job Plan here to
	// isolate and test just the Source/Where
	task, err := job.VisitSubselect(sql.From[0])
	assert.T(t, err == nil)

	tasks.Add(task.(TaskRunner))
	/*
		from.Seekable = true
		twoTasks := []TaskRunner{prevTask, curTask}
		curMergeTask := NewTaskParallel("select-sources", nil, twoTasks)
		tasks.Add(curMergeTask)

		// TODO:    Fold n <- n+1
		in, err := NewJoinNaiveMerge(prevTask, curTask, m.schema)
		if err != nil {
			return nil, err
		}
		tasks.Add(in)
	*/

	taskRoot := NewSequential("select", tasks)
	return &SqlJob{taskRoot, stmt, conf}
}

func TestJobSourceWhere(t *testing.T) {
	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id;
	`
	job := buildSourceWhere(t, rtConf, "mockcsv", sqlText)

	msgs := make([]datasource.Message, 0)
	resultWriter := NewResultBuffer(&msgs)
	job.RootTask.Add(resultWriter)

	err := job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered out 2 messages %v", len(msgs))
}
