package vm

import (
	"flag"
	"reflect"
	"testing"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/bmizerany/assert"
)

const (
	noError  = true
	hasError = false
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
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
	})

	// list of tests
	vmTests = []vmTest{

		vmt("boolean ?", `bvalf == false`, true, noError),

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

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(ctx expr.EvalContext, itemA, itemB value.Value) (value.BoolValue, bool) {
	//return BoolValue(itemA == itemB)
	rvb := value.CoerceTo(itemA.Rv(), itemB.Rv())
	//u.Infof("Eq():    a:%T  b:%T     %v=%v?", itemA, itemB, itemA.Value(), rvb)
	return value.NewBoolValue(reflect.DeepEqual(itemA.Rv(), rvb)), true
}

func ToInt(ctx expr.EvalContext, item value.Value) (value.IntValue, bool) {
	iv, _ := value.ToInt64(reflect.ValueOf(item.Value()))
	return value.NewIntValue(iv), true
	//return IntValue(2)
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
	ok      bool
	context expr.ContextReader
	result  interface{} // ?? what is this?
}

func vmt(name, qltext string, result interface{}, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, ok: ok, result: result, context: msgContext}
}
func vmtctx(name, qltext string, result interface{}, c expr.ContextReader, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, context: c, result: result, ok: ok}
}
func TestRunExpr(t *testing.T) {

	for _, test := range vmTests {

		u.Debugf("about to parse: %v", test.qlText)
		exprVm, err := NewVm(test.qlText)

		u.Infof("After Parse: %v  err=%v", test.qlText, err)
		switch {
		case err == nil && !test.ok:
			t.Errorf("%q: 1 expected error; got none", test.name)
			continue
		case err != nil && test.ok:
			t.Errorf("%q: 2 unexpected error: %v", test.name, err)
			continue
		case err != nil && !test.ok:
			// expected error, got one
			if testing.Verbose() {
				u.Infof("%s: %s\n\t%s", test.name, test.qlText, err)
			}
			continue
		}

		writeContext := datasource.NewContextSimple()
		err = exprVm.Execute(writeContext, test.context)
		results, _ := writeContext.Get("")
		//u.Infof("results:  %v", writeContext)
		if err != nil && test.ok {
			t.Errorf("\n%s -- %v: \n\t%v\nexpected\n\t'%v'", test.name, test.qlText, results, test.result)
		}
		assert.Tf(t, results != nil, "Should not have nil result: %v", results)
		//u.Infof("results=%T   %#v", results, results)
		if results.Value() != test.result {
			t.Fatalf("\n%s -- %v: \n\t%v--%T\nexpected\n\t%v--%T", test.name, test.qlText, results.Value(), results.Value(), test.result, test.result)
		}
	}
}
