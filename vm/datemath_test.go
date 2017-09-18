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
	match  bool
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
	includeCtx := &includectx{ContextReader: evalCtx}

	tests := []dateTestCase{
		{ // false, will turn true in 12 hours
			filter: `FILTER last_event < "now-1d"`,
			match:  false,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{ // same as previous, but swap left/right
			filter: `FILTER "now-1d" > last_event`,
			match:  false,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{ // This statement is true, but will turn false in 12 hours
			filter: `FILTER last_event > "now-1d"`,
			match:  true,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{ // same as previous but swap left/right
			filter: `FILTER  "now-1d" < last_event`,
			match:  true,
			ts:     []string{"now-1d"},
			tm:     t1.Add(time.Hour * 12),
		},
		{ // false, true in 36 hours
			filter: `FILTER last_event < "now-2d"`,
			match:  false,
			ts:     []string{"now-2d"},
			tm:     t1.Add(time.Hour * 36),
		},
		{ // same as above, but swap left/right
			filter: `FILTER  "now-2d" > last_event`,
			match:  false,
			ts:     []string{"now-2d"},
			tm:     t1.Add(time.Hour * 36),
		},
		{ // False, will always be false
			filter: `FILTER "now+1d" < last_event`,
			match:  false,
			ts:     []string{"now+1d"},
			tm:     time.Time{},
		},
		{ // Same as above but swap left/right
			filter: `FILTER last_event > "now+1d"`,
			match:  false,
			ts:     []string{"now+1d"},
			tm:     time.Time{},
		},
		{ // true, always true
			filter: `FILTER last_event < "now+1h"`,
			match:  true,
			ts:     []string{"now+1h"},
			tm:     time.Time{},
		},
		{
			filter: `FILTER "now+1h" > last_event`,
			match:  true,
			ts:     []string{"now+1h"},
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
		dc, err := vm.NewDateConverter(includeCtx, fs.Filter)
		assert.Equal(t, nil, err)
		assert.True(t, dc.HasDateMath)

		// initially we should not match
		matched, evalOk := vm.Matches(includeCtx, fs)
		assert.True(t, evalOk, tc.filter)
		assert.Equal(t, tc.match, matched)

		// Ensure the expected time-strings are found
		assert.Equal(t, tc.ts, dc.TimeStrings)

		// now look at boundary
		// TODO:  I would like to compare time, but was getting some errors
		// on go 1.9 timezones being different on these two.
		bt := dc.Boundary()
		assert.Equal(t, tc.tm.Unix(), bt.Unix(), tc.filter)
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
	// - urnary
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

		/*
			// TODO:  I was trying to calculate the date in the future that
			// this filter statement would no longer be true.  BUT, need to change
			// tests to change the input event timestamp instead of this approach

			// Time at which this will match
			futureContext := newIncluderCtx(
				datasource.NewNestedContextReader(readers, tc.tm),
				includeStatements)

			matched, evalOk = vm.Matches(futureContext, fs)
			assert.True(t, evalOk)
			assert.Equal(t, true, matched, tc.filter)
		*/
	}
}
