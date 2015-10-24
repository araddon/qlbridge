package vm

import (
	"flag"
	"reflect"
	"strings"
	"testing"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

const (
	noError   = true
	hasError  = false
	parseOk   = true
	evalError = false
)

var (
	VerboseTests *bool   = flag.Bool("vv", false, "Verbose Logging?")
	NameMatch    *string = flag.String("name", "", "expression name must conain `name`")
)

func init() {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}

	expr.FuncAdd("eq", Eq)
	expr.FuncAdd("toint", ToInt)
	expr.FuncAdd("yy", Yy)
	expr.FuncAdd("exists", Exists)
}

var (

	// This is the message context which will be added to all tests below
	//  and be available to the VM runtime for evaluation by using
	//  key's such as "int5" or "user_id"
	msgContext = datasource.NewContextSimpleData(map[string]value.Value{
		"int5":    value.NewIntValue(5),
		"str5":    value.NewStringValue("5"),
		"bvalt":   value.NewBoolValue(true),
		"bvalf":   value.NewBoolValue(false),
		"user_id": value.NewStringValue("abc"),
		"urls":    value.NewStringsValue([]string{"abc", "123"}),
		"hits":    value.NewMapIntValue(map[string]int64{"google.com": 5, "bing.com": 1}),
	})

	// list of tests
	vmTests = []vmTest{

		vmt("OR with urnary", `!exists(user_id) OR toint(not_a_field) > 21`, false, noError),
		vmt("OR with urnary", `exists(user_id) OR toint(not_a_field) > 21`, true, noError),
		vmt("OR with urnary", `!exists(user_id) OR toint(str5) >= 1`, true, noError),
		vmt("OR with urnary", `!exists(user_id) OR toint(str5) < 1`, false, noError),

		// Between:  Tri Node Tests
		vmt("tri between ints", `10 BETWEEN 1 AND 50`, true, noError),
		vmt("tri between ints false", `10 BETWEEN 20 AND 50`, false, noError),
		vmtall("tri between ints false", `10 BETWEEN 20 AND true`, nil, parseOk, evalError),
		// In:  Multi Arg Tests
		vmtall("multi-arg:   In (x,y,z) ", `10 IN ("a","b",10, 4.5)`, true, parseOk, evalError),
		vmtall("multi-arg:   In (x,y,z) ", `10 IN ("a","b",20, 4.5)`, false, parseOk, evalError),
		vmtall("multi-arg:   In (x,y,z) ", `"a" IN ("a","b",10, 4.5)`, true, parseOk, evalError),
		// NEGATED
		vmtall("multi-arg:   In (x,y,z) ", `10 NOT IN ("a","b" 4.5)`, true, parseOk, evalError),
		vmtall("multi-arg:   In (x,y,z) ", `"a" NOT IN ("a","b" 4.5)`, false, parseOk, evalError),
		// Not able to evaluate
		vmtall("multi-arg:   In (x,y,z) ", `toint(not_a_field) NOT IN ("a","b" 4.5)`, false, parseOk, evalError),

		vmt("slices: not in scalar ident", `"a" IN urls`, false, noError),
		vmt("slices: in scalar ident", `"abc" IN urls`, true, noError),
		vmt("slices: not in map ident", `"com" IN hits`, false, noError),
		vmt("slices: in map ident", `"google.com" IN hits`, true, noError),

		// Binary String
		vmt("binary string ==", `user_id == "abc"`, true, noError),
		vmt("binary string ==", `user_id != "abcd"`, true, noError),
		vmt("binary string ==", `user_id == "abcd"`, false, noError),
		vmt("binary string ==", `user_id != "abc"`, false, noError),
		vmtall("binary math err on string +", `user_id > "abc"`, nil, parseOk, evalError),
		vmt("binary string LIKE", `user_id LIKE "*bc"`, true, noError),
		vmt("binary string LIKE", `user_id LIKE "\*bc"`, false, noError),

		// Binary Bool
		vmt("binary bool ==", `bvalt == true`, true, noError),
		vmt("binary bool =", `bvalt = true`, true, noError),
		vmt("binary bool ==", `bvalf == false`, true, noError),
		vmt("binary bool =", `bvalf = false`, true, noError),
		vmt("binary bool ==", `bvalt == bvalf`, false, noError),
		vmt("binary bool !=", `bvalt != bvalf`, true, noError),
		vmt("binary bool !=", `(toint(not_a_field) > 0) || true`, true, noError),
		vmtall("binary error on bool == string", `user_id == true`, nil, parseOk, evalError),

		// Math
		vmt("general int addition", `5 + 4`, int64(9), noError),
		vmt("general float addition", `5.2 + 4`, float64(9.2), noError),
		vmt("associative math", `(4 + 5) / 2`, int64(4), noError),
		vmt("boolean ?", `6 > 5`, true, noError),
		vmt("boolean ?", `6 > 5.5`, true, noError),
		vmt("boolean ?", `6 == 6`, true, noError),
		vmt("boolean ?", `6 != 5`, true, noError),
		vmt("boolean urnary", `!eq(5,6)`, true, noError),
		vmt("boolean ?", `bvalt == true`, true, noError),
		vmt("boolean ?", `bvalf == false`, true, noError),
		vmt("boolean ?", `bvalf == true`, false, noError),
		vmt("boolean ?", `!(bvalf == true)`, true, noError),

		vmt("exists ?", `EXISTS int5`, true, noError),
		vmt("exists ?", `EXISTS not_a_field`, false, noError),
		vmt("exists ?", `EXISTS bvalt`, true, noError),
		vmt("exists ?", `EXISTS bvalf`, true, noError),

		// TODO
		//vmt("boolean ?", `!true`, false, noError),
		// TODO:  support () wrapping parts of binary expression
		//vmt("boolean ?", `6 == (5 + 1)`, true, noError),
		// TODO:  urnary
		//vmt("boolean ?", `true || !eq(5,6)`, true, noError),

		// Context Based Tests
		vmt("ctx read int5 value and add", `int5 + 5`, int64(10), noError),
		vmt("ctx multiply", `int5 * 6`, int64(30), noError),
		vmt("ctx cast str, multiply", `toint(str5 * 6)`, int64(30), noError),
		vmt("ctx cast str, addition", `toint(str5 + 6)`, int64(11), noError),

		// context lookups? simple
		vmt("ctx lookup ", `user_id`, "abc", noError),

		// functional syntax
		vmt("eq/toint types", `eq(toint(int5),5)`, true, noError),
		vmt("eq/toint types", `eq(toint(int5),6)`, false, noError),

		// TODO:
		//vmt("eq/toint types", `eq(toint(notreal || 1),6)`, false, noError),
		//vmt("eq/toint types", `eq(toint(notreal || 6),6)`, true, noError),
		vmt("math ?", `2 * (3 + 5)`, int64(16), noError),
	}
)

func TestRunExpr(t *testing.T) {

	for _, test := range vmTests {

		if *NameMatch != "" && !strings.Contains(test.name, *NameMatch) {
			continue
		}

		//u.Debugf("about to parse: %v", test.qlText)
		exprVm, err := NewVm(test.qlText)

		//u.Infof("After Parse: %v  err=%v", test.qlText, err)
		switch {
		case err == nil && !test.parseok:
			t.Errorf("%q: 1 expected error; got none", test.name)
			continue
		case err != nil && test.parseok:
			t.Errorf("%q: 2 unexpected error: %v", test.name, err)
			continue
		case err != nil && !test.parseok:
			// expected error, got one
			if testing.Verbose() {
				u.Infof("%s: %s\n\t%s", test.name, test.qlText, err)
			}
			continue
		}

		writeContext := datasource.NewContextSimple()
		err = exprVm.Execute(writeContext, test.context)
		if exprVm.Tree != nil && exprVm.Tree.Root != nil {
			//Eval(writeContext, exprVm.Tree.Root)
		}

		results, _ := writeContext.Get("")
		//u.Infof("results:  %T %v  err=%v", results, results, err)
		if err != nil && test.evalok {
			t.Errorf("\n%s -- %v: \n\t%v\nexpected\n\t'%v'", test.name, test.qlText, results, test.result)
		}
		if test.result == nil && results != nil {
			t.Errorf("%s - should have nil result, but got: %v", test.name, results)
			continue
		}
		if test.result != nil && results == nil {
			t.Errorf("%s - should have non-nil result but was nil", test.name)
			continue
		}

		//u.Infof("results=%T   %#v", results, results)
		if test.result != nil && results.Value() != test.result {
			t.Fatalf("\n%s -- %v: \n\t%v--%T\nexpected\n\t%v--%T", test.name, test.qlText, results.Value(), results.Value(), test.result, test.result)
		} else if test.result == nil {
			// we expected nil
		}
	}
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(ctx expr.EvalContext, itemA, itemB value.Value) (value.BoolValue, bool) {
	//return BoolValue(itemA == itemB)
	//rvb := value.CoerceTo(itemA.Rv(), itemB.Rv())
	//u.Infof("Eq():    a:%T  b:%T     %v=%v?", itemA, itemB, itemA.Value(), rvb)
	//u.Infof("Eq()2:  %T %T", itemA.Rv(), rvb)
	return value.NewBoolValue(reflect.DeepEqual(itemA.Value(), itemB.Value())), true
}

func ToInt(ctx expr.EvalContext, item value.Value) (value.IntValue, bool) {
	iv, _ := value.ToInt64(reflect.ValueOf(item.Value()))
	return value.NewIntValue(iv), true
	//return IntValue(2)
}

// Exists:  Answers True/False if the field exists and is non null
//
//     exists(real_field) => true
//     exists("value") => true
//     exists("") => false
//     exists(empty_field) => false
//     exists(2) => true
//     exists(todate(date_field)) => true
//
func Exists(ctx expr.EvalContext, item interface{}) (value.BoolValue, bool) {

	//u.Debugf("Exists():  %T  %v", item, item)
	switch node := item.(type) {
	case expr.IdentityNode:
		_, ok := ctx.Get(node.Text)
		if ok {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, true
	case expr.StringNode:
		_, ok := ctx.Get(node.Text)
		if ok {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, true
	case value.StringValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.BoolValue:
		return value.BoolValueTrue, true
	case value.NumberValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.IntValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.TimeValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.StringsValue, value.SliceValue, value.MapIntValue:
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}

func Yy(ctx expr.EvalContext, item value.Value) (value.IntValue, bool) {

	//u.Info("yy:   %T", item)
	val, ok := value.ToString(item.Rv())
	if !ok || val == "" {
		return value.NewIntValue(0), false
	}
	//u.Infof("v=%v   %v  ", val, item.Rv())
	if t, err := dateparse.ParseAny(val); err == nil {
		yy := t.Year()
		if yy >= 2000 {
			yy = yy - 2000
		} else if yy >= 1900 {
			yy = yy - 1900
		}
		//u.Infof("Yy = %v   yy = %v", item, yy)
		return value.NewIntValue(int64(yy)), true
	}

	return value.NewIntValue(0), false
}

type vmTest struct {
	name    string
	qlText  string
	parseok bool
	evalok  bool
	context expr.ContextReader
	result  interface{} // ?? what is this?
}

func vmt(name, qltext string, result interface{}, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, parseok: ok, evalok: ok, result: result, context: msgContext}
}
func vmtall(name, qltext string, result interface{}, parseOk, evalOk bool) vmTest {
	return vmTest{name: name, qlText: qltext, parseok: parseOk, evalok: evalOk, result: result, context: msgContext}
}
func vmtctx(name, qltext string, result interface{}, c expr.ContextReader, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, context: c, result: result, parseok: ok, evalok: ok}
}
