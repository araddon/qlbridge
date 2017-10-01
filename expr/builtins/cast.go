package builtins

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// ToString cast as string.  must be able to convert to string
//
type ToString struct{}

// Type string
func (m *ToString) Type() value.ValueType { return value.StringType }
func (m *ToString) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToString(arg) but got %s", n)
	}
	return toStringEval, nil
}
func toStringEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0].Nil() {
		return value.EmptyStringValue, true
	}
	return value.NewStringValue(args[0].ToString()), true
}

// Cast type coercion, cast to an explicit type.
//
//    cast(identity AS <type>) => 5.0
//    cast(reg_date AS string) => "2014/01/12"
//
// Types:  [char, string, int, float]
//
type Cast struct{}

// Type one of value types
func (m *Cast) Type() value.ValueType { return value.UnknownType }
func (m *Cast) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 2 {
		return castEvalNoAs, nil
	}
	if len(n.Args) == 3 {
		return castEval, nil
	}
	return nil, fmt.Errorf(`Expected 2 or 3 args for Cast(arg AS <type>) OR cast(field,"string") but got %s`, n)
}
func castEvalNoAs(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return nil, false
	}
	vt := value.ValueFromString(vals[1].ToString())

	// http://www.cheatography.com/davechild/cheat-sheets/mysql/
	if vt == value.UnknownType {
		switch strings.ToLower(vals[1].ToString()) {
		case "char":
			vt = value.ByteSliceType
		default:
			return nil, false
		}
	}
	val, err := value.Cast(vt, vals[0])
	if err != nil {
		return nil, false
	}
	return val, true
}
func castEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	// identity AS identity
	//  0        1    2
	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return nil, false
	}
	// This is enforced by parser, so no need
	// if vals[2] == nil || vals[2].Nil() || vals[2].Err() {
	// 	return nil, false
	// }
	vt := value.ValueFromString(vals[2].ToString())

	// http://www.cheatography.com/davechild/cheat-sheets/mysql/
	if vt == value.UnknownType {
		switch strings.ToLower(vals[2].ToString()) {
		case "char":
			vt = value.ByteSliceType
		default:
			return nil, false
		}
	}
	val, err := value.Cast(vt, vals[0])
	if err != nil {
		return nil, false
	}
	return val, true
}

// ToBool cast as boolean
//
type ToBool struct{}

// Type bool
func (m *ToBool) Type() value.ValueType { return value.BoolType }
func (m *ToBool) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToBool(arg) but got %s", n)
	}
	return toBoolEval, nil
}

func toBoolEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.BoolValueFalse, false
	}
	b, ok := value.ValueToBool(args[0])
	if !ok {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(b), true
}

// ToInt Convert to Integer:   Best attempt at converting to integer.
//
//   toint("5")          => 5, true
//   toint("5.75")       => 5, true
//   toint("5,555")      => 5555, true
//   toint("$5")         => 5, true
//   toint("5,555.00")   => 5555, true
//
type ToInt struct{}

// Type integer
func (m *ToInt) Type() value.ValueType { return value.IntType }
func (m *ToInt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToInt(arg) but got %s", n)
	}
	return toIntEval, nil
}
func toIntEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	switch val := vals[0].(type) {
	case value.TimeValue:
		iv := val.Val().UnixNano() / 1e6 // Milliseconds
		return value.NewIntValue(iv), true
	case value.NumberValue:
		return value.NewIntValue(val.Int()), true
	case value.IntValue:
		return value.NewIntValue(val.Int()), true
	default:
		iv, ok := value.ValueToInt64(vals[0])
		if ok {
			return value.NewIntValue(iv), true
		}
	}
	return value.NewIntValue(0), false
}

// ToNumber Convert to Number:   Best attempt at converting to integer
//
//   tonumber("5") => 5.0
//   tonumber("5.75") => 5.75
//   tonumber("5,555") => 5555
//   tonumber("$5") => 5.00
//   tonumber("5,555.00") => 5555
//
type ToNumber struct{}

// Type number
func (m *ToNumber) Type() value.ValueType { return value.NumberType }
func (m *ToNumber) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToNumber(arg) but got %s", n)
	}
	return toNumberEval, nil
}

func toNumberEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	fv, ok := value.ValueToFloat64(vals[0])
	if !ok {
		return value.NewNumberNil(), false
	}
	return value.NewNumberValue(fv), true
}
