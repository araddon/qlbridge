package expr_test

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/vm"
)

var _ = u.EMPTY

type dateTestCase struct {
	filter string
	ts     []string
	tm     time.Time
}

func TestDateMath(t *testing.T) {

	t1 := time.Now()

	readers := []expr.ContextReader{
		datasource.NewContextMap(map[string]interface{}{
			"event":                "login",
			"last_event":           t1,
			"signedup":             t1,
			"subscription_expires": t1.Add(time.Hour * 24 * 6),
			"lastevent":            map[string]time.Time{"signedup": t1},
			"first.event":          map[string]time.Time{"has.period": t1},
		}, true),
	}

	nc := datasource.NewNestedContextReader(readers, t1.Add(time.Minute*1))

	includeStatements := `
		FILTER signedup < "now-2d" ALIAS signedup_onedayago;
		FILTER subscription_expires < "now+1w" ALIAS subscription_expires_oneweek;
	`
	includerCtx := newIncluderCtx(nc, includeStatements)

	tests := []dateTestCase{
		{
			filter: `FILTER last_event < "now-1d"`,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 72),
		},
		{
			filter: `FILTER AND (EXISTS event, last_event < "now-1d", INCLUDE signedup_onedayago)`,
			ts:     []string{"now-1d", "now-2d"},
			tm:     t1.Add(time.Hour * 72),
		},
	}
	// test-todo
	// x include w resolution
	// - variety of +/-
	// - between
	// - urnaryies
	// - false now, will be true in 24 hours, then exit in 48
	// - not cases
	for _, tc := range tests {
		fs := rel.MustParseFilter(tc.filter)

		// Ensure we inline/include all of the expressions
		node, err := expr.InlineIncludes(includerCtx, fs.Filter)
		assert.Equal(t, nil, err)

		// Converter to find/calculate date operations
		dc := expr.NewDateConverter(node)
		assert.True(t, dc.HasDateMath)

		// initially we should not match
		matched, evalOk := vm.Matches(includerCtx, fs)
		assert.True(t, evalOk)
		assert.Equal(t, false, matched)

		// Ensure the expected time-strings are found
		if len(tc.ts) > 0 {
			assert.Equal(t, tc.ts, dc.TimeStrings)
		}

		// Time at which this will now match
		futureContext := newIncluderCtx(datasource.NewNestedContextReader(readers, tc.tm), includeStatements)

		matched, evalOk = vm.Matches(futureContext, fs)
		assert.True(t, evalOk)
		assert.Equal(t, true, matched, tc.filter)

	}
}
