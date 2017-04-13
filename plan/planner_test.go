package plan_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
)

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
		assert.True(t, p != nil)

		u.Infof("%#v", ctx.Projection)
		u.Infof("cols %#v", ctx.Projection)
		if pt.cols > 0 {
			// ensure our projection has these columns
			assert.True(t, len(ctx.Projection.Proj.Columns) == pt.cols,
				"expected %d cols got %v  %#v", pt.cols, len(ctx.Projection.Proj.Columns), ctx.Projection.Proj)
		}

	}
}
