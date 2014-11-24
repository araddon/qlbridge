package exprvm

import (
	u "github.com/araddon/gou"
	"testing"
)

type vmTest struct {
	name   string
	qlText string
	ok     bool
	result interface{} // ?? what is this?
}

var vmTests = []vmTest{
	{"general expr test", `eq(toint(item),5)`, noError, `true`},
	{"general expr test false", `eq(toint(item),7)`, noError, `false`},
}

func TestRunExpr(t *testing.T) {
	context := ContextSimple{map[string]Value{"item": IntValue(5)}}
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

		results, err := exprVm.Execute(context)
		u.Infof("results:  %v", results)
		if err != nil && test.ok {
			t.Errorf("\n%s -- (%v): \n\t%v\nexpected\n\t%v", test.name, test.qlText, results, test.result)
		}
		// if results != test.result {
		// 	t.Errorf("\n%s -- (%v): \n\t%v\nexpected\n\t%v", test.name, test.qlText, results, test.result)
		// }
	}
}
