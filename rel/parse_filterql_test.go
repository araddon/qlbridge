package rel

import (
	"os"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
	"github.com/bmizerany/assert"
)

var (
	_ = u.EMPTY
)

func init() {
	lex.IDENTITY_CHARS = lex.IDENTITY_SQL_CHARS
	if t := os.Getenv("trace"); t != "" {
		expr.Trace = true
	}
}

func parseFilterQlTest(t *testing.T, ql string) {

	u.Debugf("before: %s", ql)
	req, err := ParseFilterQL(ql)
	//u.Debugf("parse filter %#v  %s", req, ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	u.Debugf("after:  %s", req.String())
	req2, err := ParseFilterQL(req.String())
	assert.Tf(t, err == nil, "must parse roundtrip %v for %s", err, ql)
	req.Raw = ""
	req2.Raw = ""
	assert.T(t, req.Equal(req2), "must roundtrip")
}

func parseFilterSelectTest(t *testing.T, ql string) {

	u.Debugf("parse filter select: %s", ql)
	sel, err := ParseFilterSelect(ql)
	//u.Debugf("parse filter %#v  %s", sel, ql)
	assert.Tf(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	sel2, err := ParseFilterSelect(sel.String())
	assert.Tf(t, err == nil, "must parse roundtrip %v --\n%s", err, sel.String())
	assert.Tf(t, sel2 != nil, "Must parse but didnt")
	// sel.Raw = ""
	// sel2.Raw = ""
	// u.Debugf("after:  %s", sel2.String())
	// assert.Equal(t, sel, sel2, "must roundtrip")
}

type selsTest struct {
	query  string
	expect int
}

func parseFilterSelectsTest(t *testing.T, st selsTest) {

	u.Debugf("parse filter select: %v", st)
	sels, err := NewFilterParser(st.query).ParseFilterSelects()
	assert.Tf(t, err == nil, "Must parse: %s  \n\t%v", st.query, err)
	assert.Tf(t, len(sels) == st.expect, "Expected %d filters got %v", st.expect, len(sels))
	for _, sel := range sels {
		sel2, err := ParseFilterSelect(sel.String())
		assert.Tf(t, err == nil, "must parse roundtrip %v --\n%s", err, sel.String())
		assert.Tf(t, sel2 != nil, "Must parse but didnt")
	}
}

func TestFuncResolver(t *testing.T) {
	t.Parallel()

	funcs := expr.NewFuncRegistry()
	funcs.Add("foo", func(ctx expr.EvalContext) (value.BoolValue, bool) {
		return value.NewBoolValue(true), true
	})

	fs, err := NewFilterParserfuncs(`SELECT foo() FROM name FILTER foo()`, funcs).
		ParseFilter()
	assert.Tf(t, err == nil, "err:%v", err)
	assert.T(t, len(fs.Columns) == 1)

	funcs2 := expr.NewFuncRegistry()
	_, err2 := NewFilterParserfuncs(`SELECT foo() FROM name FILTER foo()`, funcs2).
		ParseFilter()

	assert.T(t, err2 != nil)
	assert.Tf(t, strings.Contains(err2.Error(), "non existent function foo"), "err:%v", err2)
}

func TestFilterErrMsg(t *testing.T) {
	t.Parallel()

	_, err := ParseFilterQL("FILTER * FROM user ALIAS ALIAS stuff")
	assert.NotEqual(t, err, nil, "Should have errored")
	assert.T(t, strings.Contains(err.Error(), "Line 1"), err)
}

func TestFilterQlRoundTrip(t *testing.T) {
	t.Parallel()

	parseFilterQlTest(t, `FILTER "bob@gmail.com" IN ("hello","world")`)
	parseFilterQlTest(t, `FILTER "bob@gmail.com" NOT IN ("hello","world")`)

	parseFilterQlTest(t, `FILTER "bob@gmail.com" IN identityname`)

	parseFilterQlTest(t, `FILTER email CONTAINS "gmail.com"`)

	parseFilterQlTest(t, `FILTER NOT INCLUDE ffe5817811c2270aa5d4aff2d9eafed3`)

	parseFilterQlTest(t, `FILTER AND ( NOT news INTERSECTS ("a"), domains intersects ("b"))`)

	parseFilterQlTest(t, `FILTER email INTERSECTS ("a", "b")`)
	parseFilterQlTest(t, `FILTER email NOT INTERSECTS ("a", "b")`)

	parseFilterQlTest(t, "FILTER EXISTS email ALIAS `Has Spaces Alias`")

	parseFilterQlTest(t, `FILTER AND ( NOT INCLUDE abcd, (lastvisit_ts > "now-1M") ) FROM user`)

	parseFilterQlTest(t, `
		FILTER score > 0
		WITH
			name = "My Little Pony",
			public = false,
			kind = "aspect"
		ALIAS with_attributes
	`)

	parseFilterQlTest(t, `
		FILTER OR ( 
			AND (
				score NOT BETWEEN 5 and 10, 
				email NOT IN ("abc") 
			),
			NOT date > "now-3d"
		)`)
	parseFilterQlTest(t, `
		FILTER AND ( EXISTS user_id, NOT OR ( user_id like "a", user_id like "b", user_id like "c", user_id like "d", user_id like "e", user_id like "f" ) )
	`)
	parseFilterQlTest(t, `
		FILTER OR ( AND ( our_names like "2. has spaces", our_names like "1. has more spa'ces" ), INCLUDE 'f9f0dc74234af7e86ddeb660c50350e1' )
	`)
	parseFilterQlTest(t, `
		FILTER  AND ( NOT INCLUDE '791734b084019d99c82a475264464304', 
			NOT INCLUDE 'd750a11e72b58778e302eb0893788680', NOT INCLUDE '61a624e5ca4153645ddc9e6ebaee8000' )
		`)
	parseFilterQlTest(t, `FILTER AND ( visitct >= "1", NOT INCLUDE 3d4240482815b9848caf2e6f )`)

	parseFilterQlTest(t, `
		FILTER AND ( 
			AND (
				score NOT BETWEEN 5 and 10, 
				email NOT IN ("abc") 
			),
			x > 7
		)`)
	parseFilterQlTest(t, `FILTER AND ( visitct >= "1", INCLUDE 3d4240482815b9848caf2e6f )`)

	parseFilterQlTest(t, `FILTER x > 7`)

	parseFilterQlTest(t, `FILTER AND ( NOT EXISTS email, email NOT IN ("abc") )`)

	parseFilterQlTest(t, `FILTER AND ( score NOT BETWEEN 5 and 10, email NOT IN ("abc") )`)
	parseFilterQlTest(t, `
		FILTER
			AND (
				NAME != NULL
				, tostring(fieldname) == "hello"
			)

			LIMIT 100
	`)
	// Heavy Comments test
	parseFilterQlTest(t, `
      -- this function tests comments
      FILTER
        -- and this expression
        AND (  -- and even here which makes no sense
          NAME != NULL   -- ensures name not nill
          , tostring(fieldname) == "hello"  -- also that fieldname == hello
        ) -- again
        -- and our limit is 100
        LIMIT 100
        -- and some more
    `)
}

func TestFilterQlFingerPrint(t *testing.T) {
	t.Parallel()

	req1, _ := ParseFilterQL(`FILTER visit_ct > 74`)
	req2, _ := ParseFilterQL(`FILTER visit_ct > 101`)
	assert.T(t, req1.FingerPrintID() == req2.FingerPrintID())
}

func TestFilterSelectParse(t *testing.T) {
	t.Parallel()
	parseFilterSelectTest(t, `SELECT a, b, domain(url) FROM name FILTER email NOT INTERSECTS ("a", "b") WITH x="y";`)

	parseFilterSelectsTest(t, selsTest{`
		SELECT a, b, domain(url) FROM name FILTER email NOT INTERSECTS ("a", "b") WITH x="y";
		SELECT a, b, domain(url) FROM name FILTER email NOT INTERSECTS ("a", "b") WITH x="y";
	`, 2})

	ql := `
    SELECT *
    FROM users
    WHERE
      domain(url) == "google.com"
      OR momentum > 20
    ALIAS my_filter_name
	`
	sel, err := ParseFilterSelect(ql)
	assert.Tf(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(sel.Columns) == 1, "Wanted 1 col got : %v", len(sel.Columns))
	assert.Tf(t, sel.Alias == "my_filter_name", "has alias: %q", sel.Alias)
	assert.NotEqual(t, nil, sel.Where, "Should have Where expr ", sel.Where)
	assert.Equalf(t, sel.Where.String(), `domain(url) == "google.com" OR momentum > 20`, "%v", sel.Where)

	ql = `
    SELECT a, b, *
    FROM users
    FILTER AND (
      domain(url) == "google.com"
      momentum > 20
     )
    ALIAS my_filter_name
	`
	sel, err = ParseFilterSelect(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, sel, nil, ql)
	assert.Tf(t, len(sel.Columns) == 3, "Wanted 3 col's got : %v", len(sel.Columns))
	assert.Tf(t, sel.Alias == "my_filter_name", "has alias: %q", sel.Alias)
	assert.Equalf(t, sel.Filter.String(), `AND ( domain(url) == "google.com", momentum > 20 )`, "%v", sel.Filter)

	ql = `
    SELECT a, b, *
    FROM users
    FILTER  domain(url) == "google.com"
    WITH aname = "b", bname = 2
    ALIAS my_filter_name
	`
	sel, err = ParseFilterSelect(ql)
	assert.Tf(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(sel.With) == 2, "Wanted 3 withs's got : %v", sel.With)
}

func TestFilterQLAstCheck(t *testing.T) {
	t.Parallel()
	ql := `
		FILTER 
			AND (
				NAME != NULL, 
				tostring(fieldname) == "hello",
			)

		LIMIT 100
	`
	req, err := ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	f, _ := req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, len(f.Args), 2, "expected 2 child filters got:%d for %s", len(f.Args), req.Filter.String())
	f1 := f.Args[0]
	assert.NotEqual(t, f1, nil)
	assert.Equalf(t, f1.String(), "NAME != NULL", "%v", f1)
	assert.Equalf(t, req.Limit, 100, "wanted limit=100: %v", req.Limit)

	// This should get re-written in simplest form as
	//    FILTER NAME != "bob"
	ql = `FILTER NOT AND ( name == "bob" ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	un := req.Filter.(*expr.UnaryNode)
	bn := un.Arg.(*expr.BinaryNode)
	u.Warnf("t %T", req.Filter)
	assert.Equalf(t, len(bn.Args), 2, "has binary expression: %#v", f)
	assert.Equalf(t, bn.String(), `(name == "bob")`, "Should have expr %v", bn)
	assert.Equalf(t, req.String(), `FILTER NOT (name == "bob") ALIAS root`, "roundtrip? %v", req.String())

	ql = `FILTER OR ( INCLUDE child_1, INCLUDE child_2 ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	f, _ = req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, len(f.Args), 2, "has 2 filter expr: %#v", f)
	assert.Equalf(t, f.Operator.T, lex.TokenLogicOr, "must have or op %v", f.Operator)
	f1 = f.Args[1]
	assert.Equalf(t, f1.String(), `INCLUDE child_2`, "Should have include %q", f1.String())

	ql = `FILTER NOT AND ( name == "bob", OR ( NOT INCLUDE filter_xyz , NOT exists abc ) ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	f, _ = req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, len(f.Args), 2, "has 2 filter expr: %#v", f)
	assert.Equalf(t, f.Negated(), true, "must negate")
	fc := f.Args[1].(*expr.BooleanNode)
	assert.Equalf(t, fc.Operator.T, lex.TokenLogicOr, "is or %#v", fc.Operator)
	f2 := fc.Args[0].(expr.NegateableNode)
	assert.Equal(t, f2.Negated(), true)
	assert.Equalf(t, f2.String(), `NOT INCLUDE filter_xyz`, "Should have include %v", f2)
	//assert.Tf(t, req.String() == ql, "roundtrip? %v", req.String())

	ql = `
    FILTER
      AND (
          -- Lets make sure the date is good
          daysago(datefield) < 100
          -- as well as domain
          , domain(url) == "google.com"
          , INCLUDE my_other_named_filter
          , OR (
              momentum > 20
             , propensity > 50
             , INCLUDE nested_filter
          )
          , NOT AND ( score > 20 , score < 50 )
       )
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equalf(t, req.Alias, "my_filter_name", "has alias: %q", req.Alias)
	u.Info(req.String())
	f = req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, len(f.Args), 5, "expected 5 filters: %#v", f)
	f5 := f.Args[4].(*expr.BooleanNode)
	assert.Tf(t, f5.Negated(), "expr negated? %s", f5.String())
	assert.Equalf(t, len(f5.Args), 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")
	assert.Equalf(t, len(req.Includes()), 2, "has 2 includes: %v", req.Includes())
	//assert.Equalf(t, f5.Expr.NodeType(), UnaryNodeType, "%s != %s", f5.Expr.NodeType(), UnaryNodeType)

	ql = `
    FILTER
      AND (
          -- Lets make sure the date is good
          daysago(datefield) < 100
          -- as well as domain
          domain(url) == "google.com"
          INCLUDE my_other_named_filter
          OR (
              momentum > 20
             propensity > 50
          )
          NOT AND ( score > 20 , score < 50 )
       )
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equalf(t, req.Alias, "my_filter_name", "has alias: %q", req.Alias)
	//u.Info(req.String())
	f = req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, len(f.Args), 5, "expected 5 filters: %#v", f)
	f5 = f.Args[4].(*expr.BooleanNode)
	assert.Tf(t, f5.Negated(), "expr negated? %s", f5.String())
	assert.Equalf(t, len(f5.Args), 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")

	ql = `FILTER AND (
				INCLUDE child_1, 
				INCLUDE child_2
			) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equalf(t, req.Alias, "root", "has alias: %q", req.Alias)
	f = req.Filter.(*expr.BooleanNode)
	for _, f := range f.Args {
		in := f.(*expr.IncludeNode)
		assert.Tf(t, in.Identity.Text != "", "has include filter %q", in.String())
	}
	assert.Equalf(t, len(f.Args), 2, "want 2 filter expr: %d", len(f.Args))

	ql = `FILTER NOT INCLUDE child_1 ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equalf(t, req.Alias, "root", "has alias: %q", req.Alias)
	incn := req.Filter.(*expr.IncludeNode)
	//assert.Tf(t, len(f.Args) == 1, "has 1 filter expr: %#v", f)
	assert.Tf(t, incn.Negated(), "must negate %s", req.String())
	assert.Equal(t, incn.Identity.Text, "child_1")
	//fInc := cf.Filter.Filters[0]
	//assert.Tf(t, fInc.Include != "", "Should have include")

	ql = `
		FILTER *
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	idn := req.Filter.(*expr.IdentityNode)
	assert.Equal(t, idn.Text, "*")

	ql = `
		FILTER match_all
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	idn = req.Filter.(*expr.IdentityNode)
	assert.Equal(t, idn.Text, "match_all")

	ql = `
    FILTER
      AND (
          EXISTS datefield
       )
	FROM user
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equalf(t, req.Alias, "my_filter_name", "has alias: %q", req.Alias)
	assert.Tf(t, req.From == "user", "has FROM: %q", req.From)
	un = req.Filter.(*expr.UnaryNode)
	assert.Equalf(t, un.String(), "EXISTS datefield", "%#v", un)

	ql = `
    FILTER AND ( NOT news INTERSECTS ("a"), domains intersects ("b"))
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	bon := req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, 2, len(bon.Args), "has 2 args")
	lh := bon.Args[0]
	u.Debugf("lh %T  %s  %#v", lh, lh, lh)
	assert.Equalf(t, bon.String(), "AND ( NOT (news INTERSECTS (\"a\")), domains intersects (\"b\") )", "%#v", bon)

	// Make sure we have a HasDateMath flag
	ql = `
		FILTER created > "now-3d"
	`
	req, err = ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	//bn := req.Filter.(*expr.BinaryNode)
	assert.Equalf(t, req.HasDateMath, true, "Must recognize datemath")
}

func TestFilterQL1(t *testing.T) {
	ql := `
    FILTER AND ( NOT news INTERSECTS ("a"), domains intersects ("b"))
	`
	req, err := ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	bon := req.Filter.(*expr.BooleanNode)
	assert.Equalf(t, 2, len(bon.Args), "has 2 args")
	lh := bon.Args[0]
	u.Debugf("lh %T  %s  %#v", lh, lh, lh)
	assert.Equalf(t, bon.String(), "AND ( NOT (news INTERSECTS (\"a\")), domains intersects (\"b\") )", "%#v", bon)
}

func TestFilterQLInvalidCheck(t *testing.T) {
	// This is invalid note the extra paren
	ql := `
		FILTER OR (_uid == "bob", email IN ("steve@steve.com")))
		ALIAS entity_basic_test
	`
	_, err := ParseFilterQL(ql)
	assert.NotEqual(t, err, nil)
}

func TestFilterQLKeywords(t *testing.T) {
	t.Parallel()
	ql := `
	  -- Test comment 1
		FILTER 
		  -- Test comment 2
			AND (
				created < "now-24h",
				deleted == false
			)
		FROM accounts
		LIMIT 100
		ALIAS new_accounts
	`
	fs, err := ParseFilterQL(ql)
	assert.Equal(t, nil, err)
	assert.Equal(t, " Test comment 1", fs.Description)
	assert.Equal(t, "accounts", fs.From)
	assert.Equal(t, 100, fs.Limit)
	assert.Equal(t, "new_accounts", fs.Alias)
}
