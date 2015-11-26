package expr

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
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
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	req2, err := ParseFilterQL(req.String())
	assert.Tf(t, err == nil, "must parse roundtrip %v", err)
	req.Raw = ""
	req2.Raw = ""
	u.Debugf("after:  %s", req2.String())
	assert.Equal(t, req, req2, "must roundtrip")
}

func TestFilterQlRoundTrip(t *testing.T) {

	parseFilterQlTest(t, `
		FILTER OR ( 
			AND (
				score NOT BETWEEN 5 and 10, 
				email NOT IN ("abc") 
			),
			NOT date > "now-3d"
		)`)
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

func TestFilterQLAstCheck(t *testing.T) {

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
    SELECT *
    FROM users
    WHERE
      domain(url) == "google.com"
      OR momentum > 20
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filters: %#v", req.Filter)
	fs := req.Filter.Filters[0]
	assert.Tf(t, fs.Expr != nil, "")
	assert.Tf(t, fs.Expr.String() == `domain(url) == "google.com" OR momentum > 20`, "%v", fs.Expr)

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
}
