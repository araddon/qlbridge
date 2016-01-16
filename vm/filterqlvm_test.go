package vm

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
)

var _ = u.EMPTY

// Our test struct, try as many different field types as possible
type User struct {
	Name          string
	Created       time.Time
	Updated       *time.Time
	Authenticated bool
	HasSession    *bool
	Roles         []string
	BankAmount    float64
	Address       Address
	Data          json.RawMessage
	Context       u.JsonHelper
}
type Address struct {
	City string
	Zip  int
}

func (m *User) FullName() string {
	return m.Name + ", Jedi"
}

func TestFilterQlVm(t *testing.T) {
	t.Parallel()

	t1, _ := dateparse.ParseAny("12/18/2015")
	//u.Infof("t1 %v", t1)
	nminus1 := time.Now().Add(time.Hour * -1)
	tr := true
	user := &User{
		Name:          "Yoda",
		Created:       t1,
		Updated:       &nminus1,
		Authenticated: true,
		HasSession:    &tr,
		Address:       Address{"Detroit", 55},
		Roles:         []string{"admin", "api"},
		BankAmount:    55.5,
	}

	readers := []expr.ContextReader{
		datasource.NewContextWrapper(user),
		datasource.NewContextSimpleNative(map[string]interface{}{
			"city": "Peoria, IL",
			"zip":  5,
		}),
	}

	nc := datasource.NewNestedContextReader(readers, time.Now())

	hits := []string{
		`FILTER name == "Yoda"`,                         // upper case sensitive name
		`FILTER name != "yoda"`,                         // we should be case-sensitive by default
		`FILTER name = "Yoda"`,                          // is equivalent to ==
		`FILTER "Yoda" == name`,                         // reverse order of identity/value
		`FILTER name != "Anakin"`,                       // negation on missing fields == true
		`FILTER first_name != "Anakin"`,                 // key doesn't exist
		`FILTER tolower(name) == "yoda"`,                // use functions in evaluation
		`FILTER FullName == "Yoda, Jedi"`,               // use functions on structs in evaluation
		`FILTER Address.City == "Detroit"`,              // traverse struct with path.field
		`FILTER name LIKE "*da"`,                        // LIKE
		`FILTER name NOT LIKE "*kin"`,                   // LIKE Negation
		`FILTER name CONTAINS "od"`,                     // Contains
		`FILTER name NOT CONTAINS "kin"`,                // Contains
		`FILTER roles INTERSECTS ("user", "api")`,       // Intersects
		`FILTER roles NOT INTERSECTS ("user", "guest")`, // Intersects
		`FILTER Created < "now-1d"`,                     // Date Math
		`FILTER Updated > "now-2h"`,                     // Date Math
		`FILTER *`,                                      // match all
		`FILTER OR (
			EXISTS name,       -- inline comments
			EXISTS not_a_key,  -- more inline comments
		)`,
		// show that line-breaks serve as expression separators
		`FILTER OR (
			EXISTS name
			EXISTS not_a_key   -- even if they have inline comments
		)`,
		//`FILTER a == "Yoda" AND b == "Peoria, IL" AND c == 5`,
		`FILTER AND (name == "Yoda", city == "Peoria, IL", zip == 5, BankAmount > 50)`,
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
		fs, err := rel.ParseFilterQL(q)
		assert.Equal(t, nil, err)
		match, err := NewFilterVm(nil).Matches(nc, fs)
		assert.Equalf(t, nil, err, "error matching on query %q: %v", q, err)
		assert.T(t, match, q)
	}

	misses := []string{
		`FILTER name == "yoda"`, // casing
		"FILTER OR (false, false, AND (true, false))",
		`FILTER AND (name == "Yoda", city == "xxx", zip == 5)`,
	}

	for _, q := range misses {
		fs, err := rel.ParseFilterQL(q)
		assert.Equal(t, nil, err)
		match, err := NewFilterVm(nil).Matches(nc, fs)
		assert.Equal(t, nil, err)
		assert.T(t, !match)
	}
}

type includer struct{}

func (includer) Include(name string) (*rel.FilterStatement, error) {
	if name != "test" {
		return nil, fmt.Errorf("Expected name 'test' but received: %s", name)
	}
	return rel.ParseFilterQL("FILTER AND (x > 5)")
}

func TestInclude(t *testing.T) {
	t.Parallel()

	e1 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 6, "y": "1"})
	e2 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 4, "y": "1"})

	q, err := rel.ParseFilterQL("FILTER AND (x < 9000, INCLUDE test)")
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
		q, err := rel.ParseFilterQL("FILTER AND (x < 9000, INCLUDE shouldfail)")
		assert.Equal(t, nil, err)
		_, err = filterVm.Matches(e1, q)
		assert.NotEqual(t, nil, err)
	}
}
