package plan_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

func init() {
	expr.FuncAdd("database", expr.NewFuncLookup("database", nil, value.StringType))
}

type plantest struct {
	q    string
	cols int
}

var planTests = []plantest{
	{"SELECT DATABASE()", 1},
}

var _ = u.EMPTY

func TestPlans(t *testing.T) {
	for _, pt := range planTests {
		ctx := td.TestContext(pt.q)
		u.Infof("running %s for plan check", pt.q)
		p := selectPlan(t, ctx)
		assert.T(t, p != nil)

		u.Infof("%#v", ctx.Projection)
		u.Infof("cols %#v", ctx.Projection)
		if pt.cols > 0 {
			// ensure our projection has these columns
			assert.Tf(t, len(ctx.Projection.Proj.Columns) == pt.cols,
				"expected %d cols got %v  %#v", pt.cols, len(ctx.Projection.Proj.Columns), ctx.Projection.Proj)
		}

	}
}
