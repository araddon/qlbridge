package exprvm

import (
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
)

type vmTest struct {
	name    string
	qlText  string
	ok      bool
	context Context
	result  interface{} // ?? what is this?
}

var (
	msgContext = ContextSimple{map[string]Value{"item": NewIntValue(5)}}

	msgContext2 = ContextSimple{map[string]Value{"item": NewStringValue("4")}}

	// list of tests
	vmTests = []vmTest{
		vmt("general expr eval", `5 > 4`, true, noError),
	}

	// vmTests = []vmTest{
	// 	vmt("general expr test", `eq(item,5)`, true, noError),
	// 	vmt("general expr test false", `eq(toint(item),7)`, false, noError),
	// 	vmt("general lookup context toint", `toint(item)`, int64(5), noError),
	// 	vmt("general expr eval", `5 > 4`, true, noError),
	// 	vmtctx("general lookup context toint", `toint(item)`, int64(4), msgContext2, noError),
	// }
)

func vmt(name, qltext string, result interface{}, ok bool) vmTest {
	return vmTest{name: name, qlText: qltext, ok: ok, result: result, context: msgContext}
}
func vmtctx(name, qltext string, result interface{}, c Context, ok bool) vmTest {
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

		results, err := exprVm.Execute(test.context)
		u.Infof("results:  %v", results)
		if err != nil && test.ok {
			t.Errorf("\n%s -- %v: \n\t%v\nexpected\n\t'%v'", test.name, test.qlText, results, test.result)
		}
		assert.Tf(t, results != nil, "Should not have nil result: %v", results)
		if results.Value() != test.result {
			t.Errorf("\n%s -- %v: \n\t%v\nexpected\n\t'%v'", test.name, test.qlText, results.Value(), test.result)
		}
	}
}
