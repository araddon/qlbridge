package expr_test

import (
	"strings"
	"testing"
	"time"

	//"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/vm"
)

var _ = u.EMPTY

type includectx struct {
	expr.ContextReader
	segs map[string]*rel.FilterStatement
}

func newIncluderCtx(cr expr.ContextReader, statements string) *includectx {
	stmts := rel.MustParseFilters(statements)
	segs := make(map[string]*rel.FilterStatement, len(stmts))
	for _, stmt := range stmts {
		segs[strings.ToLower(stmt.Alias)] = stmt
	}
	return &includectx{ContextReader: cr, segs: segs}
}
func (m *includectx) Include(name string) (expr.Node, error) {
	if seg, ok := m.segs[strings.ToLower(name)]; ok {
		return seg.Filter, nil
	}
	return nil, expr.ErrNoIncluder
}

type dateTestCase struct {
	filter string
}

func TestDateMath(t *testing.T) {

	t1 := time.Now()

	readers := []expr.ContextReader{
		datasource.NewContextMap(map[string]interface{}{
			"name":       "bob",
			"city":       "Peoria, IL",
			"zip":        5,
			"signedup":   t1,
			"lastevent":  map[string]time.Time{"signedup": t1},
			"last.event": map[string]time.Time{"has.period": t1},
		}, true),
	}

	nc := datasource.NewNestedContextReader(readers, time.Now())
	incctx := newIncluderCtx(nc, `
		-- Filter All
		FILTER * ALIAS  match_all_include;

		FILTER name == "Yoda" ALIAS is_yoda_true;
		FILTER name == "not gonna happen ALIAS name_false";
		FILTER signedup < "now-1d" ALIAS signedup_onedayago;
	`)

	tests := []string{
		`FILTER lastvisit_ts < "now-1d"`,
		`FILTER AND (EXISTS name, lastvisit_ts < "now-1d")`,
	}
	// include w resolution
	//  variety of +/-
	// between
	// urnaryies
	// false now, will be true in 24 hours, then exit in 48
	// not cases
	for _, exprStr := range tests {
		fs := rel.MustParseFilter(exprStr)
		dc := expr.NewDateConverter(fs.Filter)
		assert.True(t, dc.HasDateMath)
		matched, ok := vm.Matches(incctx, fs)
		assert.Equal(t, false, matched)
		assert.True(t, ok)
	}
}
