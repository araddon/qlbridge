package vm_test

import (
	"flag"
	"log"
	"os"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

const (
	noError   = true
	hasError  = false
	parseOk   = true
	evalError = false
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
	Trace        *bool = flag.Bool("t", false, "Trace Logging?")
)

func TestMain(m *testing.M) {
	flag.Parse()
	if *VerboseTests {
		u.SetLogger(log.New(os.Stderr, "", log.Lshortfile), "debug")
		u.SetColorOutput()
	}
	if *Trace {
		expr.Trace = true
	}
	builtins.LoadAllBuiltins()
	// Now run the actual Tests
	os.Exit(m.Run())
}

var (
	t0       = dateparse.MustParse("12/18/2015")
	tfuture  = dateparse.MustParse("12/18/2039")
	tcreated = dateparse.MustParse("12/18/2019")
	// This is the message context which will be added to all tests below
	//  and be available to the VM runtime for evaluation by using
	//  key's such as "int5" or "user_id"
	msgContext = datasource.NewContextMap(map[string]interface{}{
		"int5":          value.NewIntValue(5),
		"str5":          value.NewStringValue("5"),
		"created":       value.NewTimeValue(tcreated),
		"createdfuture": value.NewTimeValue(tfuture),
		"bvalt":         value.NewBoolValue(true),
		"bvalf":         value.NewBoolValue(false),
		"user_id":       value.NewStringValue("abc"),
		"urls":          value.NewStringsValue([]string{"abc", "123"}),
		"hits":          value.NewMapIntValue(map[string]int64{"google.com": 5, "bing.com": 1}),
		"email":         value.NewStringValue("bob@bob.com"),
		"mt":            value.NewMapTimeValue(map[string]time.Time{"event0": t0, "event1": tfuture}),
	}, true)
	vmTestsx = []vmTest{
		vmt(`mt.event1 > now()`, true, noError),
	}
	// list of tests
	vmTests = []vmTest{

		// Date math
		vmt(`createdfuture > "now-1M"`, true, noError),
		vmt(`now() > todate("01/01/2014")`, true, noError),
		vmt(`todate("now+3d") > now()`, true, noError),
		vmt(`created < 2032220220175`, true, noError), // Really not sure i want to support this?
		// date operations on map[string]time data types
		vmt(`mt.event0 > now()`, false, noError),
		vmt(`mt.event1 > now()`, true, noError),
		vmt(`mt.not_event > now()`, false, noError),

		vmt(`!exists(user_id) OR toint(not_a_field) > 21`, false, noError),
		vmt(`exists(user_id) OR toint(not_a_field) > 21`, true, noError),
		vmt(`!exists(user_id) OR toint(str5) >= 1`, true, noError),
		vmt(`!exists(user_id) OR toint(str5) < 1`, false, noError),

		// Contains func
		vmt(`contains(key,"-")`, false, noError),
		vmt(`not(contains(key,"-"))`, true, noError),
		vmt(`contains(email,"@")`, true, noError),
		vmt(`not(contains(email,"@"))`, false, noError),

		vmt(`not(contains(key,"-")) AND not(contains(email,"@"))`, false, noError),
		vmt(`not(contains(key,"-")) OR not(contains(email,"@"))`, true, noError),
		vmt(`not(contains(key,"-")) OR not(contains(not_real,"@"))`, true, noError),
		// eac of these is true, but some are missing (but bc not are true)
		vmt(`str5 NOT IN ("nope") AND userid NOT IN ("abc") AND email NOT IN ("jane@bob.com")`, true, noError),

		// Native LIKE keyword
		vmt(`["portland"] LIKE "*land"`, true, noError),
		vmt(`["chicago"] LIKE "*land"`, false, noError),
		vmt(`["New York"] LIKE "New York"`, true, noError),
		vmt(`"New York" LIKE ["Boston","New York"]`, true, noError),
		vmt(`"New York" LIKE split("Boston,New York", ",")`, true, noError),
		vmt(`"New York" LIKE split("Boston",",")`, false, noError),
		vmtall(`user_id LIKE mt`, nil, parseOk, evalError),
		vmt(`urls LIKE "a*"`, true, noError),
		vmt(`urls LIKE "d*"`, false, noError),
		vmt(`split("chicago,portland",",") LIKE "*land"`, true, noError),
		vmt(`split("chicago,portland",",") LIKE "*sea"`, false, noError),
		vmt(`email LIKE "bob*"`, true, noError),
		vmt(`email LIKE "bob"`, false, noError),
		vmt(`email LIKE "*.com"`, true, noError),

		// Native Contains keyword
		vmt(`[1,2,3] contains int5`, false, noError),
		vmt(`[1,2,3] NOT contains int5`, true, noError),
		vmt(`[1,2,3,5] contains int5`, true, noError),
		vmt(`[1,2,3,5] NOT contains int5`, false, noError),
		vmt(`email contains "bob"`, true, noError),
		vmt(`email contains ["lss","bob"]`, true, noError),
		vmt(`email contains split("lss,bob",",")`, true, noError),
		vmt(`email contains split("lss,qr",",")`, false, noError),
		vmt(`email NOT contains "bob"`, false, noError),
		vmt(`urls contains "abc"`, true, noError),
		vmt(`urls NOT contains "abc"`, false, noError),
		// Should this be correct?  By "Contains" do we change behavior
		// depending on if is array, and we mean equality on array entry or?
		vmt(`urls contains "ab"`, true, noError),

		// Between:  Ternary Node Tests
		vmt(`10 BETWEEN 1 AND 50`, true, noError),
		vmt(`10 BETWEEN "1" AND 50`, true, noError),
		vmt(`10 BETWEEN 1 AND "50"`, true, noError),
		vmt(`10 BETWEEN 1 AND "55.5"`, true, noError),
		vmt(`15.5 BETWEEN 1 AND "55.5"`, true, noError),
		vmt(`10 BETWEEN 20 AND 50`, false, noError),
		vmt(`10 BETWEEN 5 AND toint("50.5")`, true, noError),
		vmt(`10 BETWEEN int5 AND 50`, true, noError),
		vmtall(`10 BETWEEN 20 AND true`, nil, parseOk, evalError),
		vmt(`created BETWEEN "12/18/2015" AND "12/18/2020"`, true, noError),
		vmt(`created BETWEEN "now-50w" AND "12/18/2020"`, true, noError),

		// In:  Multi Arg Tests
		vmtall(`10 IN ("a","b",10, 4.5)`, true, parseOk, evalError),
		vmtall(`10 IN ("a","b",20, 4.5)`, false, parseOk, evalError),
		vmtall(`"a" IN ("a","b",10, 4.5)`, true, parseOk, evalError),
		// this is little different, it is an array
		vmtall(`"a" IN ["a","b",10, 4.5]`, true, parseOk, evalError),
		// NEGATED
		vmtall(`10 NOT IN ("a","b" 4.5)`, true, parseOk, evalError),
		vmtall(`NOT (10 IN ("a","b" 4.5))`, true, parseOk, evalError),
		//vmtall(`NOT 10 IN ("a","b" 4.5)`, true, parseOk, evalError),
		vmtall(`"a" NOT IN ("a","b" 4.5)`, false, parseOk, evalError),
		vmt(`email NOT IN ("bob@bob.com")`, false, noError),
		vmt(`NOT email IN ("bob@bob.com")`, false, noError),
		// true because negated
		vmtall(`toint(not_a_field) NOT IN ("a","b" 4.5)`, true, parseOk, noError),

		vmt(`"a" IN urls`, false, noError),
		vmt(`"abc" IN urls`, true, noError),
		vmt(`"com" IN hits`, false, noError),
		vmt(`"google.com" IN hits`, true, noError),
		vmt(`"event0" IN mt`, true, noError),
		vmt(`"event_no" IN mt`, false, noError),
		vmt(`emaildomain(email) in "google.com"`, false, noError),

		vmtall(`"hello" == split("hell-no", ",")`, nil, parseOk, evalError),

		// Binary String
		vmt(`user_id == "abc"`, true, noError),
		vmt(`user_id != "abcd"`, true, noError),
		vmt(`user_id == "abcd"`, false, noError),
		vmt(`user_id != "abc"`, false, noError),
		vmtall(`user_id > "abc"`, nil, parseOk, evalError),
		vmt(`user_id LIKE "*bc"`, true, noError),
		vmt(`user_id LIKE "\*bc"`, false, noError),
		vmt(`user_id != NULL`, true, noError),

		// Binary Bool
		vmt(`bvalt == true`, true, noError),
		vmt(`bvalt = true`, true, noError),
		vmt(`bvalf == false`, true, noError),
		vmt(`bvalf = false`, true, noError),
		vmt(`bvalt == bvalf`, false, noError),
		vmt(`bvalt != bvalf`, true, noError),
		vmt(`(toint(not_a_field) > 0) || true`, true, noError),
		vmt(`user_id == true`, false, noError),

		// Boolean Logic DSL
		vmt(`AND (email == "bob@bob.com")`, true, noError),
		vmt(`AND (email == "bob@bob.com", EXISTS urls )`, true, noError),
		vmt(`NOT AND (email == "bob@bob.com", EXISTS urls )`, false, noError),
		vmt(`AND (email == "bob@bob.com", EXISTS not_a_field )`, false, noError),
		vmt(`OR (email == "bob@bob.com", EXISTS not_a_field )`, true, noError),
		vmt(`OR (email != "bob@bob.com", EXISTS not_a_field )`, false, noError),
		vmt(`
		OR (
			email != "bob@bob.com"
			AND (
				NOT EXISTS not_a_field
				int5 == 5 
			)
		)`, true, noError),

		// Math
		vmt(`5 + 4`, int64(9), noError),
		vmt(`5.2 + 4`, float64(9.2), noError),
		vmt(`(4 + 5) / 2`, int64(4), noError),
		vmt(`6 > 5`, true, noError),
		vmt(`6 > 5.5`, true, noError),
		vmt(`6.5 > 5.5`, true, noError),
		vmt(`6 == 6`, true, noError),
		vmt(`6 != 5`, true, noError),
		vmt(`!eq(5,6)`, true, noError),
		// Number on left
		vmtall(`5.5 +  ["hello"]`, false, parseOk, noError),
		vmtall(`5.5 == ["5.5"]`, true, parseOk, noError),
		vmtall(`5.5 == ["hello", 3, "5.5"]`, true, parseOk, noError),
		vmtall(`5.5 == ["5.9", 99, "hello"]`, false, parseOk, noError),

		// Numeric Boolean coerce
		vmt(`"5.5" == 5.5`, true, noError),
		vmt(`"5.5" > 5`, true, noError),
		// Boolean, with Context
		vmt(`bvalt == true`, true, noError),
		vmt(`bvalf == false`, true, noError),
		vmt(`bvalf == true`, false, noError),
		vmt(`!(bvalf == true)`, true, noError),

		vmt(`EXISTS int5`, true, noError),
		vmt(`EXISTS not_a_field`, false, noError),
		vmt(`EXISTS bvalt`, true, noError),
		vmt(`EXISTS bvalf`, true, noError),
		vmt(`EXISTS toint(not_a_field)`, false, noError),

		// TODO:  support () wrapping parts of binary expression
		vmt(`6 == (5 + 1)`, true, noError),
		// TODO:  urnary
		vmt(`true || !eq(5,6)`, true, noError),

		// Context Based Tests
		vmt(`int5 + 5`, int64(10), noError),
		vmt(`int5 * 6`, int64(30), noError),
		vmt(`toint(str5 * 6)`, int64(30), noError),
		vmt(`toint(str5 + 6)`, int64(11), noError),

		// context lookups? simple
		vmt(`user_id`, "abc", noError),

		// functional syntax
		vmt(`eq(toint(int5),5)`, true, noError),
		vmt(`eq(toint(int5),6)`, false, noError),

		// TODO:
		//vmt("eq/toint types", `eq(toint(notreal || 1),6)`, false, noError),
		//vmt("eq/toint types", `eq(toint(notreal || 6),6)`, true, noError),
		vmt(`2 * (3 + 5)`, int64(16), noError),

		vmt(`(bvalt == true && bvalf == false)`, true, noError),
		vmt(`(fld1 != "stuff" AND (field2 == "stuff" AND toint(fieldx) > 7))`, false, noError),

		// Ensure we can parse even with all the parens, linebreaks etc
		vmt(`(
			(fld1 != "stuff" AND field = true)
			OR
		   (field2 == "stuff" AND toint(fieldx) > 7)
		)`, false, noError),

		// Code Elide/Collapse into simplest form
		vmt(`user_id == "abc"`, true, noError),
		vmt(`NOT (user_id != "abc")`, true, noError),
		vmt(`user_id != "abcd"`, true, noError),
		vmt(`NOT (user_id == "abcd")`, true, noError),
		vmt(`email contains "bob"`, true, noError),
		vmt(`NOT (email NOT contains "bob")`, true, noError),
		vmt(`exists email`, true, noError),
		vmt(`NOT (NOT EXISTS email)`, true, noError),
		vmt(`exists not_a_field`, false, noError),
		vmt(`NOT (NOT EXISTS not_a_field)`, false, noError),
		vmt(`int5 > 10`, false, noError),
		vmt(`NOT (int5 <= 10)`, false, noError),
		vmt(`int5 < 10`, true, noError),
		vmt(`NOT (int5 >= 10)`, true, noError),
		vmt(`int5 >= 10`, false, noError),
		vmt(`NOT (int5 < 10)`, false, noError),
		vmt(`int5 <= 10`, true, noError),
		vmt(`NOT (int5 > 10)`, true, noError),

		// Test some error/nil/non-eval expressions
		vmtall(`namex + true`, nil, parseOk, evalError),
		vmtall(`namex + true || namex2 + true`, false, parseOk, noError),
		vmtall(`(namex + true) == (namex2 + true)`, false, parseOk, noError),
		vmtall(`(namex + true) != (namex2 + true)`, false, parseOk, noError),
		vmtall(`(namex + true) > (namex2 + true)`, false, parseOk, noError),
		vmtall(`(namex + true) + (namex2 + true)`, nil, parseOk, evalError),
	}
)

func TestRunExpr(t *testing.T) {

	for _, test := range vmTests {

		u.Debugf("about to parse: %v", test.qlText)
		n, err := expr.ParseExpression(test.qlText)

		//u.Debugf("After Parse: %v  err=%v", test.qlText, err)
		switch {
		case err == nil && !test.parseok:
			t.Errorf("%q: 1 expected error; got none", test.qlText)
			continue
		case err != nil && test.parseok:
			t.Errorf("%q: 2 unexpected error: %v", test.qlText, err)
			continue
		case err != nil && !test.parseok:
			// expected error, got one
			if testing.Verbose() {
				u.Infof("%s: %s\n\t%s", test.qlText, test.qlText, err)
			}
			continue
		}

		val, ok := vm.Eval(test.context, n)
		errVal := false
		if val != nil {
			if _, isErr := val.(value.ErrorValue); isErr {
				errVal = true
			}
		}
		//u.Infof("results:  %T %v ok?=%v", val, val, ok)
		if !ok && test.evalok {
			t.Errorf("\n%s \n\t%v\nexpected\n\t'%v'", test.qlText, val, test.result)
		}
		if test.result == nil && (val != nil && !errVal) {
			t.Errorf("%s - should have nil result, but got: %v", test.qlText, val)
			continue
		}
		if test.result != nil && val == nil {
			t.Errorf("%s - should have non-nil result but was nil", test.qlText)
			continue
		}

		//u.Infof("results=%T   %#v", val, val)
		if test.result != nil && val.Value() != test.result {
			t.Fatalf("\n%s \n\t%v--%T\nexpected\n\t%v--%T", test.qlText, val.Value(), val.Value(), test.result, test.result)
		} else if test.result == nil {
			// we expected nil
		}
	}
}

type vmTest struct {
	qlText  string
	parseok bool
	evalok  bool
	context expr.EvalContext
	result  interface{} // ?? what is this?
}

func vmt(qltext string, result interface{}, ok bool) vmTest {
	return vmTest{qlText: qltext, parseok: ok, evalok: ok, result: result, context: &includer{msgContext}}
}
func vmtall(qltext string, result interface{}, parseOk, evalOk bool) vmTest {
	return vmTest{qlText: qltext, parseok: parseOk, evalok: evalOk, result: result, context: &includer{msgContext}}
}
func vmtctx(qltext string, result interface{}, c expr.ContextReader, ok bool) vmTest {
	return vmTest{qlText: qltext, context: &includer{c}, result: result, parseok: ok, evalok: ok}
}
