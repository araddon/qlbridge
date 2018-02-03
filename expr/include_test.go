package expr_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
)

func TestFindIncludes(t *testing.T) {
	f := rel.MustParseFilter(`
			FILTER AND (
				name == "Yoda" 
				INCLUDE yoda_sword
				NOT EXISTS email
				X between 4 and 5
				OR (
					INCLUDE return_of_the_jedi
				)
				"x" in (4,5,Z)
				email(email_name)
			)
			ALIAS yoda;
		`)
	assert.Equal(t, []string{"yoda_sword", "return_of_the_jedi"}, expr.FindIncludes(f.Filter))
}

type includectx struct {
	expr.ContextReader
	filters map[string]*rel.FilterStatement
}

func newIncluderCtx(cr expr.ContextReader, statements string) *includectx {
	stmts := rel.MustParseFilters(statements)
	filters := make(map[string]*rel.FilterStatement, len(stmts))
	for _, stmt := range stmts {
		filters[strings.ToLower(stmt.Alias)] = stmt
	}
	return &includectx{ContextReader: cr, filters: filters}
}
func (m *includectx) Include(name string) (expr.Node, error) {
	if seg, ok := m.filters[strings.ToLower(name)]; ok {
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
		FILTER AND (
			planet == "Dagobah"
			INCLUDE is_yoda_true
		) ALIAS nested_includes_yoda;
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
			in:  `AND ( lastvisit_ts < "now-2d", NOT INCLUDE is_yoda_true )`,
			out: `AND ( lastvisit_ts < "now-2d", NOT (name == "Yoda") )`,
		},
		{
			in:  `AND ( lastvisit_ts < "now-3d", NOT INCLUDE nested_includes_yoda )`,
			out: `AND ( lastvisit_ts < "now-3d", NOT AND ( planet == "Dagobah", (name == "Yoda") ) )`,
		},
	}
	tests = []incTest{
		{
			in:  `AND ( lastvisit_ts < "now-3d", NOT INCLUDE nested_includes_yoda )`,
			out: `AND ( lastvisit_ts < "now-3d", NOT AND ( planet == "Dagobah", name == "Yoda" ) )`,
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

	f := rel.MustParseFilter(`FILTER name == "Yoda" ALIAS yoda_0;`)
	includerCtx.filters[f.Alias] = f

	for i := 1; i < 120; i++ {
		f = rel.MustParseFilter(fmt.Sprintf(`
			FILTER AND (
				name == "Yoda" 
				INCLUDE yoda_%d
			)
			ALIAS yoda_%d;
		`, i-1, i))
		includerCtx.filters[f.Alias] = f
	}

	// We are going to resolve some so they have already been resolved
	f2 := includerCtx.filters["yoda_2"]
	_, err := expr.InlineIncludes(includerCtx, f2.Filter)
	assert.Equal(t, nil, err)

	_, err = expr.InlineIncludes(includerCtx, f.Filter)
	assert.Equal(t, expr.ErrMaxDepth, err)

	// If someone implements includer wrong and doesn't return an error
	badInc := &includectxBad{includerCtx}
	f = rel.MustParseFilter(`FILTER AND ( name == "Yoda", INCLUDE yoda_1000 ) ALIAS bogus;`)
	_, err = expr.InlineIncludes(badInc, f.Filter)
	assert.Equal(t, expr.ErrIncludeNotFound, err)
}

type includectxBad struct {
	*includectx
}

func (m *includectxBad) Include(name string) (expr.Node, error) {
	// If someone implements includer wrong and doesn't return an error
	return nil, nil
}
