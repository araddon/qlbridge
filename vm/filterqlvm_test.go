package vm

import (
	"fmt"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var _ = u.EMPTY

func TestFilterQlVm(t *testing.T) {
	t.Parallel()

	e := datasource.NewContextSimpleNative(map[string]interface{}{
		"name": "Yoda",
		"city": "Peoria, IL",
		"zip":  5,
	})

	hits := []string{
		`FILTER name == "Yoda"`,
		`FILTER name != "yoda"`, // we should be case-sensitive by default
		`FILTER name = "Yoda"`,  // is equivalent to ==
		`FILTER "Yoda" == name`,
		`FILTER name != "Anakin"`,
		`FILTER first_name != "Anakin"`, // key doesn't exist
		`FILTER tolower(name) == "yoda"`,
		`FILTER OR (
			EXISTS name,
			EXISTS not_a_key,
		)`,
		// show that line-breaks serve as expression separators
		`FILTER OR (
			EXISTS name
			EXISTS not_a_key
		)`,
		//`FILTER a == "Yoda" AND b == "Peoria, IL" AND c == 5`,
		`FILTER AND (name == "Yoda", city == "Peoria, IL", zip == 5)`,
		`FILTER AND (zip == 5, "Yoda" == name, OR ( city IN ( "Portland, OR", "New York, NY", "Peoria, IL" ) ) )`,
		`FILTER OR (
			EXISTS q, 
			AND ( 
				zip > 0, 
				OR ( zip > 10000, zip < 100 ) 
			), 
			NOT ( name == "Yoda" ) )`,
	}

	for _, q := range hits {
		fs, err := expr.ParseFilterQL(q)
		assert.Equal(t, nil, err)
		match, err := NewFilterVm(nil).Matches(e, fs)
		assert.Equalf(t, nil, err, "error matching on query %q: %v", q, err)
		assert.T(t, match, q)
	}

	misses := []string{
		`FILTER name == "yoda"`, // casing

		"FILTER OR (false, false, AND (true, false))",
		`FILTER AND (name == "Yoda", city == "xxx", zip == 5)`,
	}

	for _, q := range misses {
		fs, err := expr.ParseFilterQL(q)
		assert.Equal(t, nil, err)
		match, err := NewFilterVm(nil).Matches(e, fs)
		assert.Equal(t, nil, err)
		assert.T(t, !match)
	}
}

type includer struct{}

func (includer) Include(name string) (*expr.FilterStatement, error) {
	if name != "test" {
		return nil, fmt.Errorf("Expected name 'test' but received: %s", name)
	}
	return expr.ParseFilterQL("FILTER AND (x > 5)")
}

func TestInclude(t *testing.T) {
	t.Parallel()

	e1 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 6, "y": "1"})
	e2 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 4, "y": "1"})

	q, err := expr.ParseFilterQL("FILTER AND (x < 9000, INCLUDE test)")
	assert.Equal(t, nil, err)

	filterVm := NewFilterVm(includer{})

	{
		match, err := filterVm.Matches(e1, q)
		assert.Equal(t, nil, err)
		assert.T(t, match)
	}

	{
		match, err := filterVm.Matches(e2, q)
		assert.Equal(t, nil, err)
		assert.T(t, !match)
	}

	// Matches should return an error when the query includes an invalid INCLUDE
	{
		q, err := expr.ParseFilterQL("FILTER AND (x < 9000, INCLUDE shouldfail)")
		assert.Equal(t, nil, err)
		_, err = filterVm.Matches(e1, q)
		assert.NotEqual(t, nil, err)
	}
}
