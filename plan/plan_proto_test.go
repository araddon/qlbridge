package plan_test

import (
	"flag"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

var sqlStatements = []string{
	"SELECT count(*), sum(stuff) AS sumostuff FROM orders WHERE age > 20 GROUP BY category HAVING sumostuff > 10;",
}

func init() {
	flag.Parse()
	if testing.Verbose() {
		u.SetupLogging("debug")
	} else {
		u.SetupLogging("warn")
	}
	u.SetColorOutput()
	builtins.LoadAllBuiltins()
}

func selectPlan(t *testing.T, ctx *plan.Context) *plan.Select {
	stmt, err := rel.ParseSql(ctx.Raw)
	assert.Tf(t, err == nil, "Must parse but got %v", err)
	ctx.Stmt = stmt

	planner := plan.NewPlanner(ctx)
	pln, err := plan.WalkStmt(stmt, planner)
	assert.T(t, err == nil)

	sp, ok := pln.(*plan.Select)
	assert.T(t, ok, "must be *plan.Select")
	return sp
}

func TestSelectSerialization(t *testing.T) {
	for _, sqlStatement := range sqlStatements {
		ctx := td.TestContext(sqlStatement)
		p := selectPlan(t, ctx)
		assert.T(t, p != nil)
		pb, err := p.Marshal()
		assert.Tf(t, err == nil, "expected no error but got %v", err)
		assert.T(t, len(pb) > 10, string(pb))
		//u.Infof("pb?  %s", pb)
		p2, err := plan.SelectPlanFromPbBytes(pb)
		assert.Tf(t, err == nil, "expected no error but got %v", err)
		//sp, ok := p2.(*plan.Select)
		//assert.T(t, ok, "must be *plan.Select")
		assert.T(t, p2 != nil)
		assert.T(t, p2.PlanBase != nil, "Has plan Base")
		assert.T(t, p2.Stmt.Raw == p.Stmt.Raw)
		// for _, ct := range p.Children() {
		// 	u.Debugf("child: %#v", ct)
		// }
		// u.Infof("tasks? %v=?%v ", len(p.Children()), p2.PlanBase)
		assert.T(t, p.Equal(p2), "Should be equal plans")
	}
}
