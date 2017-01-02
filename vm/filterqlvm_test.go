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
	incctx := expr.NewIncludeContext(nc)

	// hits := []string{
	// 	`FILTER NOT ( FakeDate > "now-1d") `, // Date Math (negated, missing field)
	// }
	hits := []string{
		`FILTER name == "Yoda"`,                                // upper case sensitive name
		`FILTER name != "yoda"`,                                // we should be case-sensitive by default
		`FILTER name = "Yoda"`,                                 // is equivalent to ==
		`FILTER "Yoda" == name`,                                // reverse order of identity/value
		`FILTER name != "Anakin"`,                              // negation on missing fields == true
		`FILTER first_name != "Anakin"`,                        // key doesn't exist
		`FILTER tolower(name) == "yoda"`,                       // use functions in evaluation
		`FILTER FullName == "Yoda, Jedi"`,                      // use functions on structs in evaluation
		`FILTER Address.City == "Detroit"`,                     // traverse struct with path.field
		`FILTER name LIKE "*da"`,                               // LIKE
		`FILTER name NOT LIKE "*kin"`,                          // LIKE Negation
		`FILTER name CONTAINS "od"`,                            // Contains
		`FILTER name NOT CONTAINS "kin"`,                       // Contains
		`FILTER roles INTERSECTS ("user", "api")`,              // Intersects
		`FILTER roles NOT INTERSECTS ("user", "guest")`,        // Intersects
		`FILTER Created BETWEEN "12/01/2015" AND "01/01/2016"`, // Between Operator
		`FILTER Created < "now-1d"`,                            // Date Math
		`FILTER NOT ( Created > "now-1d") `,                    // Date Math (negated)
		`FILTER NOT ( FakeDate > "now-1d") `,                   // Date Math (negated, missing field)
		`FILTER Updated > "now-2h"`,                            // Date Math
		`FILTER FirstEvent.signedup < "now-2h"`,                // Date Math on map[string]time
		`FILTER FirstEvent.signedup == "12/18/2015"`,           // Date equality on map[string]time
		`FILTER lastevent.signedup < "now-2h"`,                 // Date Math on map[string]time
		`FILTER lastevent.signedup == "12/18/2015"`,            // Date equality on map[string]time
		"FILTER `lastevent`.`signedup` == \"12/18/2015\"",      // escaping of field names using backticks
		"FILTER `last.event`.`has.period` == \"12/18/2015\"",   // escaping of field names using backticks
		// Maps when used as IN, INTERSECTS have weird inferred "keys"
		`FILTER hits IN ("foo")`,
		`FILTER hits NOT IN ("not-gonna-happen")`,
		`FILTER lastevent IN ("signedup")`,
		`FILTER lastevent NOT IN ("not-gonna-happen")`,
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
	//u.Debugf("len hits: %v", len(hitsx))
	//expr.Trace = true

	for _, q := range hits {
		fs, err := rel.ParseFilterQL(q)
		assert.Equal(t, nil, err)
		match, ok := Matches(incctx, fs)
		assert.Tf(t, ok, "should be ok matching on query %q: %v", q, ok)
		assert.T(t, match, q)
		jsonAst, err := json.MarshalIndent(fs.Filter.Expr(), "", "  ")
		e := &expr.Expr{}
		err = json.Unmarshal(jsonAst, e)
		assert.Equal(t, nil, err)
		n, err := expr.NodeFromExpr(e)
		assert.Equal(t, nil, err)
		jsonAst2, _ := json.MarshalIndent(n.Expr(), "", "  ")
		u.Debugf("\n1) %#v \n2) %#v", fs.Filter, n)
		assert.Tf(t, fs.Filter.Equal(n), "Must round-trip node to json and back: %s\n%s\n%s",
			q, string(jsonAst), string(jsonAst2))
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
		match, _ := Matches(incctx, fs)
		assert.T(t, !match)
	}

	// Filter Select Statements
	filterSelects := []fsel{
		fsel{`select name, zip FROM mycontext FILTER name == "Yoda"`, map[string]interface{}{"name": "Yoda", "zip": 5}},
	}
	for _, test := range filterSelects {

		//u.Debugf("about to parse: %v", test.qlText)
		sel, err := rel.ParseFilterSelect(test.query)
		assert.Equalf(t, nil, err, "got %v for %s", err, test.query)

		writeContext := datasource.NewContextSimple()
		_, ok := EvalFilterSelect(sel, writeContext, incctx)
		assert.Tf(t, ok, "expected no error but got for %s", test.query)

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

type includer struct {
	expr.EvalContext
}

func matchTest(cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return Matches(&includer{cr}, stmt)
}

func (includer) Include(name string) (expr.Node, error) {
	if name != "test" {
		return nil, fmt.Errorf("Expected name 'test' but received: %s", name)
	}
	f, err := rel.ParseFilterQL("FILTER AND (x > 5)")
	if err != nil {
		return nil, err
	}
	return f.Filter, nil
}

func TestInclude(t *testing.T) {
	t.Parallel()

	e1 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 6, "y": "1"})
	e2 := datasource.NewContextSimpleNative(map[string]interface{}{"x": 4, "y": "1"})

	q, err := rel.ParseFilterQL("FILTER AND (x < 9000, INCLUDE test)")
	assert.Equal(t, nil, err)

	{
		match, ok := matchTest(e1, q)
		assert.T(t, ok)
		assert.T(t, match)
	}

	{
		match, ok := matchTest(e2, q)
		assert.T(t, ok)
		assert.T(t, !match)
	}

	// Matches should return an error when the query includes an invalid INCLUDE
	{
		q, err := rel.ParseFilterQL("FILTER AND (x < 9000, INCLUDE shouldfail)")
		assert.Equal(t, nil, err)
		_, ok := matchTest(e1, q) // Should fail to evaluate because no includer
		assert.T(t, !ok)
	}
}

type nilincluder struct{}

func (nilincluder) Include(name string) (expr.Node, error) {
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
	ctx := expr.NewIncludeContext(e1)
	err = ResolveIncludes(ctx, q.Filter)
	assert.NotEqual(t, err, nil)
	_, ok := Matches(ctx, q)
	assert.T(t, !ok, "Should not be ok")
}
