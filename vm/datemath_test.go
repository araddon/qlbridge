package vm_test

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

func TestDateBoundaries(t *testing.T) {

	t1 := time.Now()

	evalCtx := datasource.NewContextMapTs(map[string]interface{}{
		"last_event":           t1.Add(time.Hour * -12),
		"subscription_expires": t1.Add(time.Hour * 24 * 6),
		"lastevent":            map[string]time.Time{"signedup": t1},
		"first.event":          map[string]time.Time{"has.period": t1},
	}, true, t1)

	tests := []dateTestCase{
		{
			filter: `FILTER last_event < "now-1d"`,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{ // This one has no boundary time, ie no need to check
			filter: `FILTER last_event > "now-1d"`,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{
			filter: `FILTER last_event < "now-2d"`,
			ts:     []string{"now-2d"},
			tm:     t1.Add(time.Hour * 36),
		},
		{
			filter: `FILTER "now-1d" > last_event`,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{
			filter: `FILTER  "now-2d" > last_event`,
			ts:     []string{"now-2d"},
			tm:     t1.Add(time.Hour * 36),
		},
		{
			filter: `FILTER "now+1d" < last_event`,
			ts:     []string{"now+1d"},
			tm:     time.Time{},
		},
		{
			filter: `FILTER "now+1h" > last_event`,
			ts:     []string{"now+1h"},
			tm:     time.Time{},
		},
		{
			filter: `FILTER last_event > "now+1d"`,
			ts:     []string{"now+1d"},
			tm:     time.Time{},
		},
	}
	// test-todo
	// - variety of +/-
	// - between
	// - urnaryies
	// - false now, will be true in 24 hours, then exit in 48
	// - not cases
	for _, tc := range tests {
		fs := rel.MustParseFilter(tc.filter)

		// Converter to find/calculate date operations
		dc, err := vm.NewDateConverter(evalCtx, fs.Filter)
		assert.Equal(t, nil, err)
		assert.True(t, dc.HasDateMath)

		// Ensure the expected time-strings are found
		assert.Equal(t, tc.ts, dc.TimeStrings)

		// now look at boundary
		assert.Equal(t, tc.tm, dc.Boundary(), tc.filter)
	}
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
	evalCtx := newIncluderCtx(nc, includeStatements)

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
		node, err := expr.InlineIncludes(evalCtx, fs.Filter)
		assert.Equal(t, nil, err)

		// Converter to find/calculate date operations
		dc, err := vm.NewDateConverter(evalCtx, node)
		assert.Equal(t, nil, err)
		assert.True(t, dc.HasDateMath)

		// initially we should not match
		matched, evalOk := vm.Matches(evalCtx, fs)
		assert.True(t, evalOk)
		assert.Equal(t, false, matched)

		// Ensure the expected time-strings are found
		assert.Equal(t, tc.ts, dc.TimeStrings)

		// Time at which this will match
		futureContext := newIncluderCtx(
			datasource.NewNestedContextReader(readers, tc.tm),
			includeStatements)

		matched, evalOk = vm.Matches(futureContext, fs)
		assert.True(t, evalOk)
		assert.Equal(t, true, matched, tc.filter)

	}
}
