package builtins

import (
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var _ = u.EMPTY

func init() {
	//if testing.Verbose() {
	u.SetupLogging("debug")
	u.SetColorOutput()
	//}
	LoadAllBuiltins()
}

type testBuiltins struct {
	expr string
	val  value.Value
}

var (
	// This is used so we have a constant understood time for message context
	// normally we would use time.Now()
	//   "Apr 7, 2014 4:58:55 PM"
	regDate     = "10/13/2014"
	ts          = time.Date(2014, 4, 7, 16, 58, 55, 00, time.UTC)
	ts2         = time.Date(2014, 4, 7, 0, 0, 0, 00, time.UTC)
	regTime, _  = dateparse.ParseAny(regDate)
	readContext = datasource.NewContextUrlValuesTs(url.Values{
		"event":        {"hello"},
		"reg_date":     {"10/13/2014"},
		"msdate":       {"1438445529707"},
		"price":        {"$55"},
		"email":        {"email@email.com"},
		"emails":       {"email1@email.com", "email2@email.com"},
		"url":          {"http://www.site.com/membership/all.html"},
		"score_amount": {"22"},
		"tag_name":     {"bob"},
		"tags":         {"a", "b", "c", "d"},
	}, ts)
	float3pt1 = float64(3.1)
)

var builtinTestsx = []testBuiltins{
	{`email(emails)`, value.NewStringValue("email1@email.com")},
}
var builtinTests = []testBuiltins{

	/*
		Logical bool Evaluation Functions
		Evaluate to true/false
	*/
	{`eq(5,5)`, value.BoolValueTrue},
	{`eq("hello", event)`, value.BoolValueTrue},
	{`eq(5,6)`, value.BoolValueFalse},
	{`eq(5.5,6)`, value.BoolValueFalse},
	{`eq(true,eq(5,5))`, value.BoolValueTrue},
	{`eq(true,false)`, value.BoolValueFalse},
	{`eq(not_a_field,5)`, value.BoolValueFalse},
	{`eq(eq(not_a_field,5),false)`, value.BoolValueTrue},

	{`ne(5,5)`, value.BoolValueFalse},
	{`ne("hello", event)`, value.BoolValueFalse},
	{`ne("hello", fakeevent)`, value.BoolValueTrue},
	{`ne(5,6)`, value.BoolValueTrue},
	{`ne(true,eq(5,5))`, value.BoolValueFalse},
	{`ne(true,false)`, value.BoolValueTrue},
	{`ne(oneof(event,"yes"),"")`, value.BoolValueTrue},
	{`eq(oneof(fakeevent,"yes"),"yes")`, value.BoolValueTrue},

	{`not(true)`, value.BoolValueFalse},
	{`not(eq(5,6))`, value.BoolValueTrue},
	{`not(eq(5,not_a_field))`, value.BoolValueTrue},
	{`not(eq(5,len("12345")))`, value.BoolValueFalse},
	{`not(eq(5,len(not_a_field)))`, value.BoolValueTrue},

	{`ge(5,5)`, value.BoolValueTrue},
	{`ge(5,6)`, value.BoolValueFalse},
	{`ge(5,3)`, value.BoolValueTrue},
	{`ge(5.5,3)`, value.BoolValueTrue},
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
	{`gt(toint(total_amount),0)`, nil}, // error because no total_amount?
	{`gt(toint(total_amount),0) || true`, value.BoolValueTrue},
	{`gt(toint(price),1)`, value.BoolValueTrue},

	{`exists(event)`, value.BoolValueTrue},
	{`exists(price)`, value.BoolValueTrue},
	{`exists(toint(price))`, value.BoolValueTrue},
	{`exists(-1)`, value.BoolValueTrue},
	{`exists(non_field)`, value.BoolValueFalse},

	/*
		Logical Bool evaluation of List/Array types
	*/

	{`any(5)`, value.BoolValueTrue},
	{`any("value")`, value.BoolValueTrue},
	{`any(event)`, value.BoolValueTrue},
	{`any(notrealfield)`, value.BoolValueFalse},

	{`all("Apple")`, value.BoolValueTrue},
	{`all("Apple")`, value.BoolValueTrue},
	{`all("Apple",event)`, value.BoolValueTrue},
	{`all("Apple",event,true)`, value.BoolValueTrue},
	{`all("Apple",event)`, value.BoolValueTrue},
	{`all("Linux",true,not_a_realfield)`, value.BoolValueFalse},
	{`all("Linux",false)`, value.BoolValueFalse},
	{`all("Linux","")`, value.BoolValueFalse},
	{`all("Linux",notreal)`, value.BoolValueFalse},

	{`oneof("apples","oranges")`, value.NewStringValue("apples")},
	{`oneof(notincontext,event)`, value.NewStringValue("hello")},
	{`oneof(not_a_field, email("Bob <bob@bob.com>"))`, value.NewStringValue("bob@bob.com")},
	{`oneof(email, email(not_a_field))`, value.NewStringValue("email@email.com")},
	{`oneof(email, email(not_a_field)) NOT IN ("a","b",10, 4.5) `, value.NewBoolValue(true)},
	{`oneof(email, email(not_a_field)) IN ("email@email.com","b",10, 4.5) `, value.NewBoolValue(true)},
	{`oneof(email, email(not_a_field)) IN ("b",10, 4.5) `, value.NewBoolValue(false)},

	/*
		Map, List, Array functions
	*/
	{`map(event, 22)`, value.NewMapValue(map[string]interface{}{"hello": 22})},
	{`map(event, toint(score_amount))`, value.NewMapValue(map[string]interface{}{"hello": 22})},

	{`maptime(event)`, value.NewMapTimeValue(map[string]time.Time{"hello": ts})},
	{`maptime(event, "2016-02-03T22:00:00")`, value.NewMapTimeValue(map[string]time.Time{"hello": time.Date(2016, 2, 3, 22, 0, 0, 0, time.UTC)})},

	{`filter(match("score_","tag_"),"nam*")`, value.NewMapValue(map[string]interface{}{"amount": "22"})},
	{`filter(match("score_","tag_"),"name")`, value.NewMapValue(map[string]interface{}{"amount": "22"})},
	{`filter(split("apples,oranges",","),"ora*")`, value.NewStringsValue([]string{"apples"})},
	{`filter(split("apples,oranges",","), ["ora*","notmatch","stuff"] )`, value.NewStringsValue([]string{"apples"})},

	{`match("score_")`, value.NewMapValue(map[string]interface{}{"amount": "22"})},
	{`match("score_","tag_")`, value.NewMapValue(map[string]interface{}{"amount": "22", "name": "bob"})},
	{`mapkeys(match("score_","tag_"))`, value.NewStringsValue([]string{"amount", "name"})},
	{`mapvalues(match("score_","tag_"))`, value.NewStringsValue([]string{"22", "bob"})},
	{`mapvalues(will_not_match)`, value.NewStringsValue(nil)},
	{`mapinvert(match("score_","tag_"))`, value.NewMapStringValue(map[string]string{"22": "amount", "bob": "name"})},
	{`match("nonfield_")`, value.ErrValue},

	{`len(["5","6"])`, value.NewIntValue(2)},
	{`len(split(reg_date,"/"))`, value.NewIntValue(3)},

	{`array.index(tags,1)`, value.NewStringValue("b")},
	{`array.index(tags,6)`, nil},
	{`array.slice(tags,2)`, value.NewStringsValue([]string{"c", "d"})},
	{`array.slice(tags,1,3)`, value.NewStringsValue([]string{"b", "c"})},
	{`array.slice(tags,1,4)`, value.NewStringsValue([]string{"b", "c", "d"})},
	{`array.slice(tags,1,7)`, value.ErrValue},

	/*
		String Functions
	*/

	{`contains("5tem",5)`, value.BoolValueTrue},
	{`contains("5item","item")`, value.BoolValueTrue},
	{`contains("the-hello",event)`, value.BoolValueTrue},
	{`contains("the-item",event)`, value.BoolValueFalse},
	{`contains(price,"$")`, value.BoolValueTrue},
	{`contains(url,"membership/all.html")`, value.BoolValueTrue},
	{`contains(not_a_field,"nope")`, value.BoolValueFalse},
	{`false == contains(not_a_field,"nope")`, value.BoolValueTrue},

	{`hasprefix("5tem",5)`, value.BoolValueTrue},
	{`hasprefix("hello world",event)`, value.BoolValueTrue},
	{`hasprefix(event,"he")`, value.BoolValueTrue},
	{`hasprefix(event,"ham")`, value.BoolValueFalse},
	{`hasprefix("5tem","5y")`, value.BoolValueFalse},
	{`hassuffix("tem","m")`, value.BoolValueTrue},
	{`hassuffix("hello",event)`, value.BoolValueTrue},
	{`hassuffix(event,"lo")`, value.BoolValueTrue},
	{`hassuffix(event,"ham")`, value.BoolValueFalse},
	{`hassuffix("5tem","5y")`, value.BoolValueFalse},

	{`tolower("Apple")`, value.NewStringValue("apple")},

	{`join("apple", event, "oranges", "--")`, value.NewStringValue("apple--hello--oranges")},
	{`join(["apple","peach"], ",")`, value.NewStringValue("apple,peach")},
	{`join("apple","","peach",",")`, value.NewStringValue("apple,peach")},

	{`split("apples,oranges",",")`, value.NewStringsValue([]string{"apples", "oranges"})},

	{`replace("M20:30","M")`, value.NewStringValue("20:30")},
	{`replace("/search/for+stuff","/search/")`, value.NewStringValue("for+stuff")},
	{`replace("M20:30","M","")`, value.NewStringValue("20:30")},
	{`replace("M20:30","M","Hour ")`, value.NewStringValue("Hour 20:30")},

	// len is also a list operation above
	{`len("abc")`, value.NewIntValue(3)},
	{`len(not_a_field)`, nil},
	{`len(not_a_field) >= 10`, value.BoolValueFalse},
	{`len("abc") >= 2`, value.BoolValueTrue},
	{`CHAR_LENGTH("abc") `, value.NewIntValue(3)},
	{`CHAR_LENGTH(CAST("abc" AS CHAR))`, value.NewIntValue(3)},

	/*
		hashing functions
	*/
	{`hash.sip("http://www.google.com?q=123")`, value.NewIntValue(5673948842516703987)},
	{`hash.md5("hello")`, value.NewStringValue("5d41402abc4b2a76b9719d911017c592")},
	{`hash.sha1("hello")`, value.NewStringValue("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d")},
	{`hash.sha256("hello")`, value.NewStringValue("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")},

	{`hash.sip("http://www.google.com?q=123") % 10`, value.NewIntValue(5673948842516703987 % 10)},

	{`encoding.b64encode("hello world")`, value.NewStringValue("aGVsbG8gd29ybGQ=")},
	{`encoding.b64decode("aGVsbG8gd29ybGQ=")`, value.NewStringValue("hello world")},

	/*
		Special Type Functions:  Email, url's
	*/

	{`email("Bob@Bob.com")`, value.NewStringValue("bob@bob.com")},
	{`email("Bob <bob>")`, value.ErrValue},
	{`email("Bob <bob@bob.com>")`, value.NewStringValue("bob@bob.com")},
	{`email(emails)`, value.NewStringValue("email1@email.com")},

	{`emailname("Bob<bob@bob.com>")`, value.NewStringValue("Bob")},
	{`emaildomain("Bob<bob@gmail.com>")`, value.NewStringValue("gmail.com")},

	{`host("https://www.Google.com/search?q=golang")`, value.NewStringValue("www.google.com")},
	{`host("www.Google.com/?q=golang")`, value.NewStringValue("www.google.com")},
	//{`host("notvalid")`, value.NewStringValue("notvalid")},
	{`hosts("www.Google.com/?q=golang", "www.golang.org/")`, value.NewStringsValue([]string{"www.google.com", "www.golang.org"})},

	{`urldecode("hello+world")`, value.NewStringValue("hello world")},
	{`urldecode("hello world")`, value.NewStringValue("hello world")},
	{`urldecode("2Live_Reg")`, value.NewStringValue("2Live_Reg")},
	{`urldecode("https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Dgolang")`, value.NewStringValue("https://www.google.com/search?q=golang")},

	{`domain("https://www.Google.com/search?q=golang")`, value.NewStringValue("google.com")},
	{`domains("https://www.Google.com/search?q=golang")`, value.NewStringsValue([]string{"google.com"})},
	{`domains("https://www.Google.com/search?q=golang","http://www.ign.com")`, value.NewStringsValue([]string{"google.com", "ign.com"})},

	{`path("https://www.Google.com/search?q=golang")`, value.NewStringValue("/search")},
	{`path("https://www.Google.com/blog/hello.html")`, value.NewStringValue("/blog/hello.html")},
	{`path("www.Google.com/?q=golang")`, value.NewStringValue("/")},
	{`path("c://Windows/really")`, value.NewStringValue("//windows/really")},
	{`path("/home/aaron/vm")`, value.NewStringValue("/home/aaron/vm")},

	{`qs("https://www.Google.com/search?q=golang","q")`, value.NewStringValue("golang")},
	{`qs("www.Google.com/?q=golang","q")`, value.NewStringValue("golang")},

	{`url.matchqs("http://www.google.com/blog?mc_eid=123&mc_id=1&pid=123&utm_campaign=free")`, value.NewStringValue("www.google.com/blog")},
	{`url.matchqs("http://www.google.com/blog?mc_eid=123&mc_id=1&pid=123&utm_campaign=free", "pid", "mc_eid")`,
		value.NewStringValue("www.google.com/blog?mc_eid=123&pid=123")},
	{`url.matchqs("http://www.google.com/blog?mc_eid=123&mc_id=1&pid=123&utm_campaign=free", "mc_*")`,
		value.NewStringValue("www.google.com/blog?mc_eid=123&mc_id=1")},
	{`url.matchqs("http://www.google.com/blog")`, value.NewStringValue("www.google.com/blog")},
	{`url.matchqs("http://not a url")`, value.ErrValue},

	{`urlminusqs("http://www.Google.com/search?q1=golang&q2=github","q1")`, value.NewStringValue("http://www.Google.com/search?q2=github")},
	{`urlminusqs("http://www.Google.com/search?q1=golang&q2=github","q3")`, value.NewStringValue("http://www.Google.com/search?q1=golang&q2=github")},
	{`urlminusqs("http://www.Google.com/search?q1=golang","q1")`, value.NewStringValue("http://www.Google.com/search")},

	{`urlmain("http://www.Google.com/search?q1=golang&q2=github")`, value.NewStringValue("www.Google.com/search")},

	/*
		Casting and type-coercion functions
	*/
	{`cast(reg_date as time)`, value.NewTimeValue(regTime)},
	{`CAST(score_amount AS int))`, value.NewIntValue(22)},
	{`CAST(score_amount AS string))`, value.NewStringValue("22")},
	{`CAST(score_amount AS char))`, value.NewByteSliceValue([]byte("22"))},

	// ts2         = time.Date(2014, 4, 7, 0, 0, 0, 00, time.UTC)
	// Eu style
	{`todate("02/01/2006","07/04/2014")`, value.NewTimeValue(ts2)},
	{`todate("1/2/06","4/7/14")`, value.NewTimeValue(ts2)},
	{`todate("4/7/14")`, value.NewTimeValue(ts2)},
	{`todate("Apr 7, 2014 4:58:55 PM")`, value.NewTimeValue(ts)},
	{`todate("Apr 7, 2014 4:58:55 PM") < todate("now-3m")`, value.NewBoolValue(true)},

	{`toint("5")`, value.NewIntValue(5)},
	{`toint("hello")`, value.ErrValue},
	{`toint("$ 5.22")`, value.NewIntValue(5)},
	{`toint("5.56")`, value.NewIntValue(5)},
	{`toint("$5.56")`, value.NewIntValue(5)},
	{`toint("5,555.00")`, value.NewIntValue(5555)},
	{`toint("€ 5,555.00")`, value.NewIntValue(5555)},
	{`toint(5555.05)`, value.NewIntValue(5555)},

	{`tonumber("5")`, value.NewNumberValue(float64(5))},
	{`tonumber("hello")`, value.ErrValue},
	{`tonumber("$ 5.22")`, value.NewNumberValue(float64(5.22))},
	{`tonumber("5.56")`, value.NewNumberValue(float64(5.56))},
	{`tonumber("$5.56")`, value.NewNumberValue(float64(5.56))},
	{`tonumber("5,555.00")`, value.NewNumberValue(float64(5555.00))},
	{`tonumber("€ 5,555.00")`, value.NewNumberValue(float64(5555.00))},

	/*
		Date functions
	*/

	{`seconds("M10:30")`, value.NewNumberValue(630)},
	{`seconds(replace("M10:30","M"))`, value.NewNumberValue(630)},
	{`seconds("M100:30")`, value.NewNumberValue(6030)},
	{`seconds("00:30")`, value.NewNumberValue(30)},
	{`seconds("30")`, value.NewNumberValue(30)},
	{`seconds(30)`, value.NewNumberValue(30)},
	{`seconds("2015/07/04")`, value.NewNumberValue(1435968000)},

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

	{`extract(reg_date, "%B")`, value.NewStringValue("October")},
	{`extract(reg_date, "%d")`, value.NewStringValue("13")},
	{`extract("1257894000", "%B - %d")`, value.NewStringValue("November - 10")},
	{`extract("1257894000000", "%B - %d")`, value.NewStringValue("November - 10")},

	{`unixtrunc("1438445529707")`, value.NewStringValue("1438445529")},
	{`unixtrunc("1438445529", "ms")`, value.NewStringValue("1438445529000")},
	{`unixtrunc(todate(msdate))`, value.NewStringValue("1438445529")},
	{`unixtrunc(todate(msdate), "seconds")`, value.NewStringValue("1438445529.707")},
	{`unixtrunc(reg_date, "milliseconds")`, value.NewStringValue("1413158400000")},
	{`unixtrunc(reg_date, "seconds")`, value.NewStringValue("1413158400.0")},

	/*
		Math & Aggs
	*/
	{`sum(1,2)`, value.NewNumberValue(3)},
	{`sum(1,[2,3])`, value.NewNumberValue(6)},
	{`sum(1,"2")`, value.NewNumberValue(3)},
	{`sum(["1","2"])`, value.NewNumberValue(3)},
	{`sum("hello")`, value.ErrValue},

	{`avg(1,2)`, value.NewNumberValue(1.5)},
	{`avg(1,[2,3])`, value.NewNumberValue(2.0)},
	{`avg(1,"2")`, value.NewNumberValue(1.5)},
	{`avg(["1","2"])`, value.NewNumberValue(1.5)},
	{`avg("hello")`, value.ErrValue},

	{`pow(5,2)`, value.NewNumberValue(25)},
	{`pow(2,2)`, value.NewNumberValue(4)},
	{`pow(NotAField,2)`, value.ErrValue},

	{`sqrt(4)`, value.NewNumberValue(2)},
	{`sqrt(25)`, value.NewNumberValue(5)},
	{`sqrt(NotAField)`, value.ErrValue},

	{`count(4)`, value.NewIntValue(1)},
	{`count(not_a_field)`, value.ErrValue},
	{`count(not_a_field)`, nil},
}

func TestBuiltins(t *testing.T) {
	for _, biTest := range builtinTests {

		//u.Debugf("expr:  %v", biTest.expr)
		exprNode, err := expr.ParseExpression(biTest.expr)
		assert.Equalf(t, err, nil, "parse err: %v on %s", err, biTest.expr)

		val, ok := vm.Eval(readContext, exprNode)
		if biTest.val == nil {
			assert.Tf(t, !ok, "Should not have evaluated? ok?%v val=%v", ok, val)
		} else if biTest.val.Err() {

			assert.Tf(t, !ok, "%v  expected err: %v", biTest.expr, ok)

		} else {

			assert.Tf(t, ok, "Should have evaluated: %s  %#v", biTest.expr, val)

			tval := biTest.val
			//u.Debugf("Type:  %T  %T", val, tval.Value)

			switch testVal := biTest.val.(type) {
			case nil:
				assert.Tf(t, !ok, "Not ok Get? %#v")
			case value.StringsValue:
				//u.Infof("Sweet, is StringsValue:")
				sa := tval.(value.StringsValue).Value().([]string)
				sb := val.Value().([]string)
				sort.Strings(sa)
				sort.Strings(sb)
				assert.Tf(t, strings.Join(sa, ",") == strings.Join(sb, ","),
					"should be == expect %v but was %v  %v", tval.Value(), val.Value(), biTest.expr)
			case value.MapValue:
				if len(testVal.Val()) == 0 {
					// we didn't expect it to work?
					_, ok := val.(value.MapValue)
					assert.Tf(t, !ok, "Was able to convert to mapvalue but should have failed %#v", val)
				} else {
					mv, ok := val.(value.MapValue)
					assert.Tf(t, ok, "Was able to convert to mapvalue: %#v", val)
					//u.Debugf("mv: %T  %v", mv, val)
					assert.Tf(t, len(testVal.Val()) == mv.Len(), "Should have same size maps")
					mivals := mv.Val()
					for k, v := range testVal.Val() {
						valVal := mivals[k]
						//u.Infof("k:%v  v:%v   valval:%v", k, v.Value(), valVal.Value())
						assert.Equalf(t, valVal.Value(), v.Value(), "Must have found k/v:  %v \n\t%#v \n\t%#v", k, v, valVal)
					}
				}
			case value.Map:
				mv, ok := val.(value.Map)
				assert.Tf(t, ok, "Was able to convert to mapvalue: %#v", val)
				//u.Debugf("mv: %T  %v", mv, val)
				assert.Tf(t, testVal.Len() == mv.Len(), "Should have same size maps")
				mivals := mv.MapValue()
				for k, v := range testVal.MapValue().Val() {
					valVal, _ := mivals.Get(k)
					//u.Infof("k:%v  v:%v   valval:%v", k, v.Value(), valVal.Value())
					assert.Equalf(t, valVal.Value(), v.Value(), "Must have found k/v:  %v \n\t%#v \n\t%#v", k, v, valVal)
				}
			case value.ByteSliceValue:
				assert.Tf(t, val.ToString() == tval.ToString(),
					"should be == expect %v but was %v  %v", tval.ToString(), val.ToString(), biTest.expr)
			default:
				assert.Tf(t, val.Value() == tval.Value(),
					"should be == expect %v but was %v  %v", tval.Value(), val.Value(), biTest.expr)
			}
		}

	}
}
