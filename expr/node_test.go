package expr_test

import (
	"encoding/json"
	"testing"

	u "github.com/araddon/gou"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/vm"
)

var pbTests = []string{
	`eq(event,"stuff") OR ge(party, 1)`,
	`"Portland" IN ("ohio")`,
	`"xyz" BETWEEN todate("1/1/2015") AND 50`,
	`name == "bob"`,
	`name = 'bob'`,
	`AND ( EXISTS x, EXISTS y )`,
	`AND ( EXISTS x, INCLUDE ref_name )`,
	`company = "Toys R"" Us"`,
}

func TestNodePb(t *testing.T) {
	t.Parallel()
	for _, exprText := range pbTests {
		exp, err := expr.ParseExpression(exprText)
		assert.Equal(t, err, nil, "Should not error parse expr but got ", err, "for ", exprText)
		pb := exp.NodePb()
		assert.True(t, pb != nil, "was nil PB: %#v", exp)
		pbBytes, err := proto.Marshal(pb)
		assert.True(t, err == nil, "Should not error on proto.Marshal but got [%v] for %s pb:%#v", err, exprText, pb)
		n2, err := expr.NodeFromPb(pbBytes)
		assert.True(t, err == nil, "Should not error from pb but got ", err, "for ", exprText)
		assert.True(t, exp.Equal(n2), "Equal?  %v  %v", exp, n2)
		u.Infof("pre/post: \n\t%s\n\t%s", exp, n2)
	}
}

func TestExprRoundTrip(t *testing.T) {
	t.Parallel()
	for _, et := range exprTests {
		exp, err := expr.ParseExpression(et.qlText)
		if et.ok {
			assert.Equal(t, err, nil, "Should not error parse expr but got %v for %s", err, et.qlText)
			by, err := json.MarshalIndent(exp.Expr(), "", "  ")
			assert.Equal(t, err, nil)
			u.Debugf("%s", string(by))
			en := &expr.Expr{}
			err = json.Unmarshal(by, en)
			assert.Equal(t, err, nil)
			_, err = expr.NodeFromExpr(en)
			assert.Equal(t, err, nil, et.qlText)

			// by, _ = json.MarshalIndent(nn.Expr(), "", "  ")
			// u.Debugf("%s", string(by))

			// TODO: Fixme
			// u.Debugf("%s", nn)
			// assert.True(t, nn.Equal(exp), "%s  doesn't match %s", et.qlText, nn.String())

		} else {
			assert.NotEqual(t, nil, err)
		}

	}
}

func TestNodeJson(t *testing.T) {
	t.Parallel()
	for _, exprText := range pbTests {
		exp, err := expr.ParseExpression(exprText)
		assert.Equal(t, err, nil, "Should not error parse expr but got ", err, "for ", exprText)
		by, err := json.MarshalIndent(exp.Expr(), "", "  ")
		assert.Equal(t, err, nil)
		u.Debugf("%s", string(by))
	}
}

var _ = u.EMPTY

func TestIdentityNames(t *testing.T) {
	m := map[string]string{
		`count(visits)`:   "ct_visits",
		`x = y`:           "x",
		`x = y AND q = z`: "x",
		`min(year)`:       "min_year",
		`AND( year > 10)`: "year",
	}
	for expr_str, expected := range m {
		ex, err := expr.ParseExpression(expr_str)
		assert.Equal(t, nil, err)
		assert.Equal(t, expected, expr.FindIdentityName(0, ex, ""))
	}
}

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

func TestDateMath(t *testing.T) {

	readers := []expr.ContextReader{
		datasource.NewContextWrapper(user),
		datasource.NewContextMap(map[string]interface{}{
			"city":       "Peoria, IL",
			"zip":        5,
			"lastevent":  map[string]time.Time{"signedup": t1},
			"last.event": map[string]time.Time{"has.period": t1},
		}, true),
	}

	nc := datasource.NewNestedContextReader(readers, time.Now())
	incctx := newIncluderCtx(nc, `
		-- Filter All
		FILTER * ALIAS  match_all_include;

		FILTER name == "Yoda" ALIAS is_yoda_true;
		FILTER name == "not gonna happen ALIS name_false"
	`)

	tests := []string{
		`lastvisit_ts > "now-1d"`,
		`AND (exists name, lastvisit_ts > "now-1d")`,
	}
	// include
	// between
	// urnaryies
	// false now, will be true in 24 hours, then exit in 48
	// not cases
	for _, exprStr := range tests {
		node, err := expr.ParseExpression(exprStr)
		assert.Equal(t, nil, err)
		assert.True(t, expr.HasDateMath(node))
		matched, ok := vm.Matches(cr, stmt)
	}
}
