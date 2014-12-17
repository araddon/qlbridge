package builtins

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/vm"
	"github.com/bmizerany/assert"
	"net/url"
	//"reflect"
	"testing"
)

var _ = u.EMPTY

func init() {
	LoadAllBuiltins()
	u.SetupLogging("debug")
}

type testBuiltins struct {
	expr string
	vm.Value
}

var readContext = vm.NewContextUrlValues(url.Values{"event": {"hello"}})
var float3pt1 = float64(3.1)
var builtinTests = []testBuiltins{
	{`eq(5,5)`, vm.BoolValueTrue},
	{`eq('hello', event)`, vm.BoolValueTrue},
	{`eq(5,6)`, vm.BoolValueFalse},
	// TODO:  add to lexer - identity a bool token
	//{`eq(true,eq(5,5))`, vm.BoolValueTrue},

	{`ge(5,5)`, vm.BoolValueTrue},
	{`ge(5,6)`, vm.BoolValueFalse},
	{`ge(5,3)`, vm.BoolValueTrue},
	{`ge(5,"3")`, vm.BoolValueTrue},
	{`gt(5,5)`, vm.BoolValueFalse},
	{`gt(5,6)`, vm.BoolValueFalse},
	{`gt(5,3)`, vm.BoolValueTrue},
	{`gt(5,"3")`, vm.BoolValueTrue},
	{`gt(5,toint("3.5"))`, vm.BoolValueTrue},
	{`toint("5")`, vm.NewIntValue(5)},
}

func TestBuiltins(t *testing.T) {
	for _, biTest := range builtinTests {

		writeContext := vm.NewContextSimple()

		exprVm, err := vm.NewVm(biTest.expr)
		assert.Tf(t, err == nil, "nil err: %v", err)

		err = exprVm.Execute(writeContext, readContext)
		assert.Tf(t, err == nil, "nil err: %v", err)

		val, ok := writeContext.Get("")
		assert.Tf(t, ok, "Not ok Get? %#v", writeContext)

		assert.Tf(t, val == biTest.Value, "should be == expect %v but was: %v  %#v", biTest.Value, val, biTest)
	}
}
