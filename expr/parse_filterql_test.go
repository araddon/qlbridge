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
		FILTER 
			AND (
				NAME != NULL
				, tostring(fieldname) == "hello"
			)
	`)
}

func TestFilterQLAstCheck(t *testing.T) {

	ql := `
		FILTER 
			AND (
				NAME != NULL
				, tostring(fieldname) == "hello"
			)
	`
	req, err := ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.Filter.Filters) == 2, "has 2 filters: %#v", req.Filter)
	f1 := req.Filter.Filters[0]
	assert.Tf(t, f1.Expr != nil, "")
	assert.Tf(t, f1.Expr.String() == "NAME != NULL", "%v", f1.Expr)

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
}

func TestFilterQLWithJson(t *testing.T) {

	ql := `
		FILTER 
		  AND (
		     NAME != NULL
		  ) 
		WITH {
			"key":"value2"
			,"keyint":45,
			"keyfloat":55.5, 
			"keybool": true,
			"keyarraymixed":["a",2,"b"],
			"keyarrayobj":[
				{"hello":"value","age":55},
				{"hello":"value","age":55}
			],
			"keyobj":{"hello":"value","age":55},
			"keyobjnested":{
				"hello":"value",
				"array":[
					"a",
					2,
					"b"
				]
			}
		}
		`
	req, err := ParseFilterQL(ql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.Tf(t, len(req.With) == 8, "has with: %v", req.With)
	assert.Tf(t, len(req.With.Helper("keyobj")) == 2, "has 2obj keys: %v", req.With.Helper("keyobj"))
	u.Infof("req.With:  \n%s", req.With.PrettyJson())
}
