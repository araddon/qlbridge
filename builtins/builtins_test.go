package builtins

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/vm"
	"github.com/bmizerany/assert"
	"net/url"
	"sort"
	"strings"
	"time"
	//"reflect"
	"testing"
)

var _ = u.EMPTY

func init() {
	LoadAllBuiltins()
	u.SetupLogging("debug")
	u.SetColorOutput()
}

type testBuiltins struct {
	expr string
	val  vm.Value
}

var (
	// This is used so we have a constant understood time for message context
	// normally we would use time.Now()
	//   "Apr 7, 2014 4:58:55 PM"
	ts          = time.Date(2014, 4, 7, 16, 58, 55, 00, time.UTC)
	readContext = vm.NewContextUrlValuesTs(url.Values{"event": {"hello"}, "reg_date": {"10/13/2014"}}, ts)
	float3pt1   = float64(3.1)
)

var builtinTests = []testBuiltins{

	{`count(nonfield)`, vm.ErrValue},

	{`eq(5,5)`, vm.BoolValueTrue},
	{`eq('hello', event)`, vm.BoolValueTrue},
	{`eq(5,6)`, vm.BoolValueFalse},
	{`eq(true,eq(5,5))`, vm.BoolValueTrue},
	{`eq(true,false)`, vm.BoolValueFalse},

	{`not(true)`, vm.BoolValueFalse},
	{`not(eq(5,6))`, vm.BoolValueTrue},

	{`ge(5,5)`, vm.BoolValueTrue},
	{`ge(5,6)`, vm.BoolValueFalse},
	{`ge(5,3)`, vm.BoolValueTrue},
	{`ge(5,"3")`, vm.BoolValueTrue},

	{`le(5,5)`, vm.BoolValueTrue},
	{`le(5,6)`, vm.BoolValueTrue},
	{`le(5,3)`, vm.BoolValueFalse},
	{`le(5,"3")`, vm.BoolValueFalse},

	{`lt(5,5)`, vm.BoolValueFalse},
	{`lt(5,6)`, vm.BoolValueTrue},
	{`lt(5,3)`, vm.BoolValueFalse},
	{`lt(5,"3")`, vm.BoolValueFalse},

	{`gt(5,5)`, vm.BoolValueFalse},
	{`gt(5,6)`, vm.BoolValueFalse},
	{`gt(5,3)`, vm.BoolValueTrue},
	{`gt(5,"3")`, vm.BoolValueTrue},
	{`gt(5,toint("3.5"))`, vm.BoolValueTrue},

	{`contains("5tem",5)`, vm.BoolValueTrue},
	{`contains("5item","item")`, vm.BoolValueTrue},
	{`contains("the-hello",event)`, vm.BoolValueTrue},
	{`contains("the-item",event)`, vm.BoolValueFalse},

	{`tolower("Apple")`, vm.NewStringValue("apple")},

	{`join("apple", event, "oranges", "--")`, vm.NewStringValue("apple--hello--oranges")},

	{`split("apples,oranges",",")`, vm.NewStringsValue([]string{"apples", "oranges"})},

	{`oneof("apples","oranges")`, vm.NewStringValue("apples")},
	{`oneof(notincontext,event)`, vm.NewStringValue("hello")},

	{`email("Bob@Bob.com")`, vm.NewStringValue("bob@bob.com")},
	{`email("Bob <bob>")`, vm.ErrValue},
	{`email("Bob <bob@bob.com>")`, vm.NewStringValue("bob@bob.com")},

	{`emailname("Bob<bob@bob.com>")`, vm.NewStringValue("Bob")},

	{`emaildomain("Bob<bob@gmail.com>")`, vm.NewStringValue("gmail.com")},

	{`host("https://www.Google.com/search?q=golang")`, vm.NewStringValue("www.google.com")},
	{`host("www.Google.com/?q=golang")`, vm.NewStringValue("www.google.com")},
	//{`host("notvalid")`, vm.NewStringValue("notvalid")},

	{`path("https://www.Google.com/search?q=golang")`, vm.NewStringValue("/search")},
	{`path("www.Google.com/?q=golang")`, vm.NewStringValue("/")},
	{`path("c://Windows/really")`, vm.NewStringValue("//windows/really")},
	{`path("/home/aaron/vm")`, vm.NewStringValue("/home/aaron/vm")},

	{`qs("https://www.Google.com/search?q=golang","q")`, vm.NewStringValue("golang")},
	{`qs("www.Google.com/?q=golang","q")`, vm.NewStringValue("golang")},

	{`toint("5")`, vm.NewIntValue(5)},
	{`toint("hello")`, vm.ErrValue},

	{`yy("10/13/2014")`, vm.NewIntValue(14)},
	{`yy("01/02/2006")`, vm.NewIntValue(6)},
	{`yy()`, vm.NewIntValue(int64(ts.Year() - 2000))},

	{`mm("10/13/2014")`, vm.NewIntValue(10)},
	{`mm("01/02/2006")`, vm.NewIntValue(1)},

	{`yymm("10/13/2014")`, vm.NewStringValue("1410")},
	{`yymm("01/02/2006")`, vm.NewStringValue("0601")},

	{`hourofday("Apr 7, 2014 4:58:55 PM")`, vm.NewIntValue(16)},
	{`hourofday()`, vm.NewIntValue(16)},

	{`hourofweek("Apr 7, 2014 4:58:55 PM")`, vm.NewIntValue(40)},

	{`totimestamp("Apr 7, 2014 4:58:55 PM")`, vm.NewIntValue(1396889935)},

	{`todate("Apr 7, 2014 4:58:55 PM")`, vm.NewTimeValue(ts)},
}

// Need to think about this a bit, as expression vm resolves IdentityNodes in advance
//   such that we get just value, so exists() doesn't even work
// {`exists(event)`, vm.BoolValueTrue},
// {`exists("event")`, vm.BoolValueTrue},
// {`exists(stuff)`, vm.BoolValueFalse},
// {`exists("notreal")`, vm.BoolValueFalse},
// {`exists(5)`, vm.BoolValueFalse},

func TestBuiltins(t *testing.T) {
	for _, biTest := range builtinTests {

		writeContext := vm.NewContextSimple()

		exprVm, err := vm.NewVm(biTest.expr)
		assert.Tf(t, err == nil, "nil err: %v", err)

		err = exprVm.Execute(writeContext, readContext)
		if biTest.val.Err() {

			assert.Tf(t, err != nil, "nil err: %v", err)

		} else {
			tval := biTest.val
			assert.Tf(t, err == nil, "not nil err: %s  %v", biTest.expr, err)

			val, ok := writeContext.Get("")
			assert.Tf(t, ok, "Not ok Get? %#v", writeContext)

			u.Infof("Type:  %T  %T", val, tval.Value)

			switch biTest.val.(type) {
			case vm.StringsValue:
				u.Infof("Sweet, is StringsValue:")
				sa := tval.(vm.StringsValue).Value().([]string)
				sb := val.Value().([]string)
				sort.Strings(sa)
				sort.Strings(sb)
				assert.Tf(t, strings.Join(sa, ",") == strings.Join(sb, ","),
					"should be == expect %v but was %v  %v", tval.Value(), val.Value(), biTest.expr)
			default:
				assert.Tf(t, val.Value() == tval.Value(),
					"should be == expect %v but was %v  %v", tval.Value(), val.Value(), biTest.expr)
			}
		}

	}
}
