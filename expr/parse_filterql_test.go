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
	req, err := ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
}

func TestFilterQlLexOnly(t *testing.T) {

	parseFilterQlTest(t, `
		FILTER x > 7
	`)

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
	assert.Tf(t, len(req.Filter.Filters) == 2, "has 2 filters: %#v", req.Filter)
	f1 := req.Filter.Filters[0]
	assert.Tf(t, f1.Expr != nil, "")
	assert.Tf(t, f1.Expr.String() == "NAME != NULL", "%v", f1.Expr)
	assert.Tf(t, req.Limit == 100, "wanted limit=100: %v", req.Limit)

	ql = `FILTER NOT AND ( name == "bob" ) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	assert.Tf(t, req.Filter.Negate == true, "must negate")
	fex := req.Filter.Filters[0]
	assert.Tf(t, fex.Expr.String() == `name == "bob"`, "Should have expr %v", fex)
	assert.Tf(t, req.String() == ql, "roundtrip? %v", req.String())

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
          )
          , NOT AND ( score > 20 , score < 50 )
       )
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	assert.Equalf(t, len(req.Filter.Filters), 5, "expected 5 filters: %#v", req.Filter)
	f5 := req.Filter.Filters[4]
	assert.Tf(t, f5.Negate || f5.Filter.Negate, "expr negated? %s", f5.String())
	assert.Tf(t, len(f5.Filter.Filters) == 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")
	//assert.Equalf(t, f5.Expr.NodeType(), UnaryNodeType, "%s != %s", f5.Expr.NodeType(), UnaryNodeType)

	ql = `FILTER AND (
				INCLUDE child_1, 
				INCLUDE child_2
			) ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	for _, f := range req.Filter.Filters {
		assert.Tf(t, f.Include != "", "has include filter %q", f.String())
	}
	assert.Tf(t, len(req.Filter.Filters) == 2, "want 2 filter expr: %#v", req.Filter.Filters)

	ql = `FILTER NOT INCLUDE child_1 ALIAS root`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filter expr: %#v", req.Filter.Filters)
	assert.Tf(t, req.Filter.Negate == true, "must negate")
	fInc := req.Filter.Filters[0]
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
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	assert.Tf(t, len(req.Filter.Filters) == 1, "has 1 filters: %#v", req.Filter)
	f1 = req.Filter.Filters[0]
	assert.Tf(t, f1.Expr != nil, "")
	assert.Tf(t, f1.Expr.String() == "EXISTS datefield", "%#v", f1.Expr.String())
}
