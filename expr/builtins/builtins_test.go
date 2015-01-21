package builtins

import (
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/bmizerany/assert"
)

var _ = u.EMPTY

func init() {
	LoadAllBuiltins()
	u.SetupLogging("debug")
	u.SetColorOutput()

	// change quotes marks to NOT include double-quotes so we can use for values
	lex.IdentityQuoting = []byte{'[', '`'}
}

type testBuiltins struct {
	expr string
	val  value.Value
}

var (
	// This is used so we have a constant understood time for message context
	// normally we would use time.Now()
	//   "Apr 7, 2014 4:58:55 PM"
	ts          = time.Date(2014, 4, 7, 16, 58, 55, 00, time.UTC)
	readContext = datasource.NewContextUrlValuesTs(url.Values{"event": {"hello"}, "reg_date": {"10/13/2014"}}, ts)
	float3pt1   = float64(3.1)
)

var builtinTests = []testBuiltins{

	{`count(nonfield)`, value.ErrValue},

	{`eq(5,5)`, value.BoolValueTrue},
	{`eq('hello', event)`, value.BoolValueTrue},
	{`eq(5,6)`, value.BoolValueFalse},
	{`eq(true,eq(5,5))`, value.BoolValueTrue},
	{`eq(true,false)`, value.BoolValueFalse},

	{`not(true)`, value.BoolValueFalse},
	{`not(eq(5,6))`, value.BoolValueTrue},

	{`ge(5,5)`, value.BoolValueTrue},
	{`ge(5,6)`, value.BoolValueFalse},
	{`ge(5,3)`, value.BoolValueTrue},
	{`ge(5,"3")`, value.BoolValueTrue},

	{`le(5,5)`, value.BoolValueTrue},
	{`le(5,6)`, value.BoolValueTrue},
	{`le(5,3)`, value.BoolValueFalse},
	{`le(5,"3")`, value.BoolValueFalse},

	{`lt(5,5)`, value.BoolValueFalse},
	{`lt(5,6)`, value.BoolValueTrue},
	{`lt(5,3)`, value.BoolValueFalse},
	{`lt(5,"3")`, value.BoolValueFalse},

	{`gt(5,5)`, value.BoolValueFalse},
	{`gt(5,6)`, value.BoolValueFalse},
	{`gt(5,3)`, value.BoolValueTrue},
	{`gt(5,"3")`, value.BoolValueTrue},
	{`gt(5,toint("3.5"))`, value.BoolValueTrue},

	{`contains("5tem",5)`, value.BoolValueTrue},
	{`contains("5item","item")`, value.BoolValueTrue},
	{`contains("the-hello",event)`, value.BoolValueTrue},
	{`contains("the-item",event)`, value.BoolValueFalse},

	{`tolower("Apple")`, value.NewStringValue("apple")},

	{`join("apple", event, "oranges", "--")`, value.NewStringValue("apple--hello--oranges")},

	{`split("apples,oranges",",")`, value.NewStringsValue([]string{"apples", "oranges"})},

	{`oneof("apples","oranges")`, value.NewStringValue("apples")},
	{`oneof(notincontext,event)`, value.NewStringValue("hello")},

	{`email("Bob@Bob.com")`, value.NewStringValue("bob@bob.com")},
	{`email("Bob <bob>")`, value.ErrValue},
	{`email("Bob <bob@bob.com>")`, value.NewStringValue("bob@bob.com")},

	{`emailname("Bob<bob@bob.com>")`, value.NewStringValue("Bob")},

	{`emaildomain("Bob<bob@gmail.com>")`, value.NewStringValue("gmail.com")},

	{`host("https://www.Google.com/search?q=golang")`, value.NewStringValue("www.google.com")},
	{`host("www.Google.com/?q=golang")`, value.NewStringValue("www.google.com")},
	//{`host("notvalid")`, value.NewStringValue("notvalid")},

	{`path("https://www.Google.com/search?q=golang")`, value.NewStringValue("/search")},
	{`path("www.Google.com/?q=golang")`, value.NewStringValue("/")},
	{`path("c://Windows/really")`, value.NewStringValue("//windows/really")},
	{`path("/home/aaron/vm")`, value.NewStringValue("/home/aaron/vm")},

	{`qs("https://www.Google.com/search?q=golang","q")`, value.NewStringValue("golang")},
	{`qs("www.Google.com/?q=golang","q")`, value.NewStringValue("golang")},

	{`toint("5")`, value.NewIntValue(5)},
	{`toint("hello")`, value.ErrValue},

	{`yy("10/13/2014")`, value.NewIntValue(14)},
	{`yy("01/02/2006")`, value.NewIntValue(6)},
	{`yy()`, value.NewIntValue(int64(ts.Year() - 2000))},

	{`mm("10/13/2014")`, value.NewIntValue(10)},
	{`mm("01/02/2006")`, value.NewIntValue(1)},

	{`yymm("10/13/2014")`, value.NewStringValue("1410")},
	{`yymm("01/02/2006")`, value.NewStringValue("0601")},

	{`hourofday("Apr 7, 2014 4:58:55 PM")`, value.NewIntValue(16)},
	{`hourofday()`, value.NewIntValue(16)},

	{`hourofweek("Apr 7, 2014 4:58:55 PM")`, value.NewIntValue(40)},

	{`totimestamp("Apr 7, 2014 4:58:55 PM")`, value.NewIntValue(1396889935)},

	{`todate("Apr 7, 2014 4:58:55 PM")`, value.NewTimeValue(ts)},
}

// Need to think about this a bit, as expression vm resolves IdentityNodes in advance
//   such that we get just value, so exists() doesn't even work
// {`exists(event)`, value.BoolValueTrue},
// {`exists("event")`, value.BoolValueTrue},
// {`exists(stuff)`, value.BoolValueFalse},
// {`exists("notreal")`, value.BoolValueFalse},
// {`exists(5)`, value.BoolValueFalse},

func TestBuiltins(t *testing.T) {
	for _, biTest := range builtinTests {

		writeContext := datasource.NewContextSimple()

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
			case value.StringsValue:
				u.Infof("Sweet, is StringsValue:")
				sa := tval.(value.StringsValue).Value().([]string)
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
