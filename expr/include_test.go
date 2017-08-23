package expr_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	//"github.com/araddon/qlbridge/vm"
)

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

type incTest struct {
	in  string
	out string
}

func TestInlineIncludes(t *testing.T) {

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
	includerCtx := newIncluderCtx(nc, `
		FILTER name == "Yoda" ALIAS is_yoda_true;
	`)

	tests := []incTest{
		{
			in:  `lastvisit_ts < "now-1d"`,
			out: `lastvisit_ts < "now-1d"`,
		},
		{
			in:  `AND ( lastvisit_ts < "now-1d", INCLUDE is_yoda_true )`,
			out: `AND ( lastvisit_ts < "now-1d", name == "Yoda" )`,
		},
		{
			in:  `AND ( lastvisit_ts < "now-1d", NOT INCLUDE is_yoda_true )`,
			out: `AND ( lastvisit_ts < "now-1d", NOT (name == "Yoda") )`,
		},
	}
	for _, tc := range tests {
		n := expr.MustParse(tc.in)
		out, err := expr.InlineIncludes(includerCtx, n)
		assert.Equal(t, nil, err)
		assert.NotEqual(t, nil, out)
		if out != nil {
			assert.Equal(t, tc.out, out.String())
		}
	}

	testsErr := []incTest{
		{
			in:  `AND ( lastvisit_ts < "now-1d", INCLUDE not_gonna_be_found )`,
			out: `AND ( lastvisit_ts < "now-1d", name == "Yoda" )`,
		},
	}
	for _, tc := range testsErr {
		n := expr.MustParse(tc.in)
		_, err := expr.InlineIncludes(includerCtx, n)
		assert.NotEqual(t, nil, err)
	}
}
