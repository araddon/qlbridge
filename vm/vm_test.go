package vm

import (
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
)

/*
This is an Expression vm with context, that is it will accept a context environment which
can be used to lookup values in addition to parsed values for evaluation





*/
type vmTest struct {
	name    string
	qlText  string
	ok      bool
	context ContextReader
	result  interface{} // ?? what is this?
}

var (

	// This is the message contex which will be added to all tests below
	// and be available to the VM runtime for evaluation
	msgContext = ContextSimple{map[string]Value{"int5": NewIntValue(5), "user_id": NewStringValue("abc")}}

	// list of tests
	vmTests = []vmTest{
		vmt("general int addition", `5 + 4`, int64(9), noError),
		vmt("general float addition", `5.2 + 4`, float64(9.2), noError),
		vmt("associative math", `(4 + 5) / 2`, int64(4), noError),
		vmt("boolean ?", `6 > 5`, true, noError),
		vmt("boolean ?", `6 > 5.5`, true, noError),
		vmt("boolean ?", `6 == 6`, true, noError),
		//vmt("boolean ?", `6 + (6 > 5)`, true, noError),

		// Context Based Tests
		vmt("ctx addition", `int5 + 5`, int64(10), noError),
		vmt("ctx addition", `int5 + 5`, int64(10), noError),

		// context lookups?
		vmt("ctx lookup ", `user_id`, "abc", noError),

		// functional syntax
		vmt("eq/toint types", `eq(toint(int5),5)`, true, noError),
		vmt("eq/toint types", `eq(toint(int5),6)`, false, noError),
		//vmt("eq/toint types", `eq(oneof(item5,int5),6)`, false, hasError),
	}
)

func vmt(name, qltext string, result interface{}, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, ok: ok, result: result, context: msgContext}
}
func vmtctx(name, qltext string, result interface{}, c ContextReader, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, context: c, result: result, ok: ok}
}
func TestRunExpr(t *testing.T) {

	for _, test := range vmTests {

		exprVm, err := NewVm(test.qlText)

		u.Infof("After Parse:  %v", err)
		switch {
		case err == nil && !test.ok:
			t.Errorf("%q: 1 expected error; got none", test.name)
			continue
		case err != nil && test.ok:
			t.Errorf("%q: 2 unexpected error: %v", test.name, err)
			continue
		case err != nil && !test.ok:
			// expected error, got one
			if *VerboseTests {
				u.Infof("%s: %s\n\t%s", test.name, test.qlText, err)
			}
			continue
		}

		writeContext := NewContextSimple()
		err = exprVm.Execute(writeContext, test.context)
		results := writeContext.Get("")
		u.Infof("results:  %v", writeContext)
		if err != nil && test.ok {
			t.Errorf("\n%s -- %v: \n\t%v\nexpected\n\t'%v'", test.name, test.qlText, results, test.result)
		}
		assert.Tf(t, results != nil, "Should not have nil result: %v", results)
		u.Infof("results=%T   %#v", results, results)
		if results.Value() != test.result {
			t.Errorf("\n%s -- %v: \n\t%v--%T\nexpected\n\t%v--%T", test.name, test.qlText, results.Value(), results.Value(), test.result, test.result)
		}
	}
}
