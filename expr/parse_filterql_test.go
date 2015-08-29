package expr

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/bmizerany/assert"
	"testing"
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
          , NOT score > 20
       )
    ALIAS my_filter_name
	`
	req, err = ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, req.Alias == "my_filter_name", "has alias: %q", req.Alias)
	assert.Tf(t, len(req.Filter.Filters) == 4, "has 4 filters: %#v", req.Filter)
	f4 := req.Filter.Filters[3]
	assert.Tf(t, f4.Expr != nil, "")
	assert.Tf(t, f4.Expr.String() == "NOT score > 20", "%v", f4.Expr)

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
