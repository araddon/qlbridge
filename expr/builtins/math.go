package builtins

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Sqrt square root function.  Must be able to coerce to number.
//
//    sqrt(4)            =>  2, true
//    sqrt(9)            =>  3, true
//    sqrt(not_number)   =>  0, false
//
type Sqrt struct{}

// Type is NumberType
func (m *Sqrt) Type() value.ValueType { return value.NumberType }

// Validate Must have 1 arg
func (m *Sqrt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected exactly 1 args for sqrt(arg) but got %s", n)
	}
	return sqrtEval, nil
}

func sqrtEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.NewNumberNil(), false
	}

	nv, ok := args[0].(value.NumericValue)
	if !ok {
		return value.NewNumberNil(), false
	}
	fv := nv.Float()
	fv = math.Sqrt(fv)
	return value.NewNumberValue(fv), true
}

// Pow exponents, raise x to the power of y
//
//    pow(5,2)            =>  25, true
//    pow(3,2)            =>  9, true
//    pow(not_number,2)   =>  NilNumber, false
//
type Pow struct{}

// Type is Number
func (m *Pow) Type() value.ValueType { return value.NumberType }

// Must have 2 arguments, both must be able to be coerced to Number
func (m *Pow) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for Pow(numer, power) but got %s", n)
	}
	return powerEval, nil
}

func powerEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.NewNumberNil(), false
	}
	if args[1] == nil || args[1].Err() || args[1].Nil() {
		return value.NewNumberNil(), false
	}
	fv, _ := value.ValueToFloat64(args[0])
	pow, _ := value.ValueToFloat64(args[1])
	if math.IsNaN(fv) || math.IsNaN(pow) {
		return value.NewNumberNil(), false
	}
	fv = math.Pow(fv, pow)
	return value.NewNumberValue(fv), true
}
