package rel

import (
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
}

func parseFilterQlTest(t *testing.T, ql string) {

	u.Debugf("before: %s", ql)
	req, err := ParseFilterQL(ql)
	//u.Debugf("parse filter %#v  %s", req, ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	req2, err := ParseFilterQL(req.String())
	assert.Tf(t, err == nil, "must parse roundtrip %v", err)
	req.Raw = ""
	req2.Raw = ""
	u.Debugf("after:  %s", req2.String())
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
	sels, err := NewFilterParser().Statement(st.query).ParseFilters()
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

	var funcs = expr.NewFuncRegistry()
	funcs.Add("foo", func(ctx expr.EvalContext) (value.BoolValue, bool) {
		return value.NewBoolValue(true), true
	})

	fs, err := NewFilterParser().
		Statement(`SELECT foo() FROM name FILTER foo()`).
		BuildVM().
		FuncResolver(funcs).
		ParseFilter()
	assert.Tf(t, err == nil, "err:%v", err)
	assert.T(t, len(fs.Columns) == 1)

	_, err2 := NewFilterParser().
		Statement(`SELECT foo() FROM name FILTER foo()`).
		BuildVM().
		ParseFilter()

	assert.T(t, err2 != nil)
	assert.Tf(t, strings.Contains(err2.Error(), "non existent function foo"), "err:%v", err2)
}

func TestFilterQlRoundTrip(t *testing.T) {
	t.Parallel()

	parseFilterQlTest(t, `FILTER "bob@gmail.com" IN ("hello","world")`)
	parseFilterQlTest(t, `FILTER "bob@gmail.com" NOT IN ("hello","world")`)

	parseFilterQlTest(t, `FILTER "bob@gmail.com" IN identityname`)

	parseFilterQlTest(t, `FILTER email CONTAINS "gmail.com"`)

	parseFilterQlTest(t, `FILTER email INTERSECTS ("a", "b")`)
	parseFilterQlTest(t, `FILTER email NOT INTERSECTS ("a", "b")`)

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
	assert.Tf(t, len(sel.Filter.Filters) == 1, "has 1 filters: %#v", sel.Filter)
	fs := sel.Filter.Filters[0]
	assert.Tf(t, fs.Expr != nil, "")
	assert.Tf(t, fs.Expr.String() == `domain(url) == "google.com" OR momentum > 20`, "%v", fs.Expr)

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
	assert.Tf(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(sel.Columns) == 3, "Wanted 3 col's got : %v", len(sel.Columns))
	assert.Tf(t, sel.Alias == "my_filter_name", "has alias: %q", sel.Alias)
	assert.Tf(t, len(sel.Filter.Filters) == 1, "has 1 filters: %#v", sel.Filter)
	fs = sel.Filter.Filters[0]
	assert.Tf(t, fs.String() == `AND ( domain(url) == "google.com", momentum > 20 )`, "%v", fs)

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
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "expected 1 filters got:%d for %s", len(req.Filter.Filters), req.Filter.String())
	cf := req.Filter.Filters[0]
	assert.Tf(t, len(cf.Filter.Filters) == 2, "expected 2 filters got:%d for %s", len(cf.Filter.Filters), cf.String())
	f1 := cf.Filter.Filters[0]
	assert.Tf(t, f1.Expr != nil, "")
	assert.Tf(t, f1.Expr.String() == "NAME != NULL", "%v", f1.Expr)
	assert.Tf(t, req.Limit == 100, "wanted limit=100: %v", req.Limit)

	ql = `FILTER NOT AND ( name == "bob" ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	cf = req.Filter.Filters[0]
	assert.Tf(t, cf.Filter.Negate == true, "must negate")
	fex := cf.Filter.Filters[0]
	assert.Tf(t, fex.Expr.String() == `name == "bob"`, "Should have expr %v", fex)
	assert.Tf(t, req.String() == `FILTER NOT name == "bob" ALIAS root`, "roundtrip? %v", req.String())

	ql = `FILTER OR ( INCLUDE child_1, INCLUDE child_2 ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	cf = req.Filter.Filters[0]
	assert.Tf(t, len(cf.Filter.Filters) == 2, "has 2 filter expr: %#v", cf.Filter.Filters)
	assert.Tf(t, req.Filter.Op == lex.TokenOr, "must have or op %v", req.Filter.Op)
	f1 = cf.Filter.Filters[1]
	assert.Tf(t, f1.String() == `INCLUDE child_2`, "Should have include %q", f1.String())

	ql = `FILTER NOT ( name == "bob", OR ( NOT INCLUDE filter_xyz , NOT exists abc ) ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	cf = req.Filter.Filters[0]
	assert.Tf(t, len(cf.Filter.Filters) == 2, "has 2 filter expr: %#v", cf.Filter.Filters)
	assert.Tf(t, cf.Filter.Negate == true, "must negate")
	f1 = cf.Filter.Filters[1]
	assert.Tf(t, len(f1.Filter.Filters) == 2, "has 2 filter subfilter: %#v", f1.String())
	assert.Tf(t, f1.Filter.Op == lex.TokenOr, "is or %#v", f1.Filter.Op)
	f2 := f1.Filter.Filters[0]
	assert.T(t, f2.Negate == true)
	assert.Tf(t, f2.String() == `NOT INCLUDE filter_xyz`, "Should have include %v", f2)
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
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	u.Info(req.String())
	cf = req.Filter.Filters[0]
	assert.Equalf(t, len(cf.Filter.Filters), 5, "expected 5 filters: %#v", cf.Filter)
	f5 := cf.Filter.Filters[4]
	assert.Tf(t, f5.Negate || f5.Filter.Negate, "expr negated? %s", f5.String())
	assert.Tf(t, len(f5.Filter.Filters) == 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")
	assert.Tf(t, len(req.Includes()) == 2, "has 2 includes: %v", req.Includes())
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
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	u.Info(req.String())
	cf = req.Filter.Filters[0]
	assert.Equalf(t, len(cf.Filter.Filters), 5, "expected 5 filters: %#v", cf.Filter)
	f5 = cf.Filter.Filters[4]
	assert.Tf(t, f5.Negate || f5.Filter.Negate, "expr negated? %s", f5.String())
	assert.Tf(t, len(f5.Filter.Filters) == 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")

	ql = `FILTER AND (
				INCLUDE child_1, 
				INCLUDE child_2
			) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	cf = req.Filter.Filters[0]
	for _, f := range cf.Filter.Filters {
		assert.Tf(t, f.Include != "", "has include filter %q", f.String())
	}
	assert.Tf(t, len(cf.Filter.Filters) == 2, "want 2 filter expr: %#v", cf.Filter.Filters)

	ql = `FILTER NOT INCLUDE child_1 ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	assert.Tf(t, req.Filter.Negate == true || req.Filter.Filters[0].Negate, "must negate %s", req.String())
	fInc := cf.Filter.Filters[0]
	assert.Tf(t, fInc.Include != "", "Should have include")

	ql = `
		FILTER *
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	fAll := req.Filter.Filters[0]
	assert.Tf(t, fAll.MatchAll, "Should have match all")

	ql = `
		FILTER match_all
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	fAll = req.Filter.Filters[0]
	assert.Tf(t, fAll.MatchAll, "Should have match all")

	// Make sure we support following features
	//  - naked single valid expressions that are compound
	//  - naked expression syntax (ie, not AND())
	ql = `
		FILTER 
			NAME != NULL
			AND
			tostring(fieldname) == "hello"
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)

	ql = `
    FILTER
      AND (
          EXISTS datefield
       )
	FROM user
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	assert.Tf(t, req.From == "user", "has FROM: %q", req.From)
	cf = req.Filter.Filters[0]
	assert.Tf(t, len(cf.Filter.Filters) == 1, "has 1 filters: %#v", cf.Filter)
	f1 = cf.Filter.Filters[0]
	assert.Tf(t, f1.Expr != nil, "")
	assert.Tf(t, f1.Expr.String() == "EXISTS datefield", "%#v", f1.Expr.String())

	// Make sure we have a HasDateMath flag
	ql = `
		FILTER created > "now-3d"
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	assert.Tf(t, req.HasDateMath == true, "Must recognize datemath")
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
