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
	"github.com/araddon/qlbridge/value"
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
	Hits          map[string]int64
	FirstEvent    map[string]time.Time
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
		Hits:          map[string]int64{"foo": 5},
		FirstEvent:    map[string]time.Time{"signedup": t1},
	}
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

	hits := []string{
		`FILTER name == "Yoda"`,                              // upper case sensitive name
		`FILTER name != "yoda"`,                              // we should be case-sensitive by default
		`FILTER name = "Yoda"`,                               // is equivalent to ==
		`FILTER "Yoda" == name`,                              // reverse order of identity/value
		`FILTER name != "Anakin"`,                            // negation on missing fields == true
		`FILTER first_name != "Anakin"`,                      // key doesn't exist
		`FILTER tolower(name) == "yoda"`,                     // use functions in evaluation
		`FILTER FullName == "Yoda, Jedi"`,                    // use functions on structs in evaluation
		`FILTER Address.City == "Detroit"`,                   // traverse struct with path.field
		`FILTER name LIKE "*da"`,                             // LIKE
		`FILTER name NOT LIKE "*kin"`,                        // LIKE Negation
		`FILTER name CONTAINS "od"`,                          // Contains
		`FILTER name NOT CONTAINS "kin"`,                     // Contains
		`FILTER roles INTERSECTS ("user", "api")`,            // Intersects
		`FILTER roles NOT INTERSECTS ("user", "guest")`,      // Intersects
		`FILTER Created < "now-1d"`,                          // Date Math
		`FILTER Updated > "now-2h"`,                          // Date Math
		`FILTER FirstEvent.signedup < "now-2h"`,              // Date Math on map[string]time
		`FILTER FirstEvent.signedup == "12/18/2015"`,         // Date equality on map[string]time
		`FILTER lastevent.signedup < "now-2h"`,               // Date Math on map[string]time
		`FILTER lastevent.signedup == "12/18/2015"`,          // Date equality on map[string]time
		"FILTER `lastevent`.`signedup` == \"12/18/2015\"",    // escaping of field names using backticks
		"FILTER `last.event`.`has.period` == \"12/18/2015\"", // escaping of field names using backticks
		`FILTER *`, // match all
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
		// Coerce strings to numbers when appropriate
		`FILTER AND (zip == "5", BankAmount > "50")`,
		`FILTER bankamount > "9.4"`,
		`FILTER AND (zip == 5, "Yoda" == name, OR ( city IN ( "Portland, OR", "New York, NY", "Peoria, IL" ) ) )`,
		`FILTER OR (
			EXISTS q, 
			AND ( 
				zip > 0, 
				OR ( zip > 10000, zip < 100 ) 
			), 
			NOT ( name == "Yoda" ) )`,
		`FILTER hits.foo > 1.5`,
		`FILTER hits.foo > "1.5"`,
		`FILTER NOT ( hits.foo > 5.5 )`,
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
		`FILTER lastevent.signedup > "now-2h"`,      // Date Math on map[string]time
		`FILTER lastevent.signedup != "12/18/2015"`, // Date equality on map[string]time
	}

	for _, q := range misses {
		fs, err := rel.ParseFilterQL(q)
		assert.Equal(t, nil, err)
		match, err := NewFilterVm(nil).Matches(nc, fs)
		assert.Equal(t, nil, err)
		assert.T(t, !match)
	}

	// Filter Select Statements
	filterSelects := []fsel{
		fsel{`select name, zip FROM mycontext FILTER name == "Yoda"`, map[string]interface{}{"name": "Yoda", "zip": 5}},
	}
	for _, test := range filterSelects {

		//u.Debugf("about to parse: %v", test.qlText)
		sel, err := rel.ParseFilterSelect(test.query)
		assert.T(t, err == nil, "expected no error but got ", err, " for ", test.query)

		writeContext := datasource.NewContextSimple()
		_, err = EvalFilterSelect(sel, nil, writeContext, nc)
		assert.T(t, err == nil, "expected no error but got ", err, " for ", test.query)

		for key, val := range test.expect {
			v := value.NewValue(val)
			v2, ok := writeContext.Get(key)
			assert.Tf(t, ok, "Get(%q)=%v but got: %#v", key, val, writeContext.Row())
			assert.Equalf(t, v2.Value(), v.Value(), "?? %s  %v!=%v %T %T", key, v.Value(), v2.Value(), v.Value(), v2.Value())
		}
	}
}

type fsel struct {
	query  string
	expect map[string]interface{}
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

type nilincluder struct{}

func (nilincluder) Include(name string) (*rel.FilterStatement, error) {
	return nil, nil
}

// TestNilIncluder ensures we don't panic if an Includer returns nil. They
// shouldn't, but they do, so we need to be defensive.
func TestNilIncluder(t *testing.T) {
	t.Parallel()
	e1 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 6, "y": "1"})
	q, err := rel.ParseFilterQL("FILTER INCLUDE shouldfail")
	if err != nil {
		t.Fatalf("Error parsing query: %v", err)
	}

	fvm := NewFilterVm(nilincluder{})
	_, err = fvm.Matches(e1, q)
	if err == nil {
		t.Fatal("Expected error didn't occur!")
	}
}
