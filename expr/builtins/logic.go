package builtins

import (
	"fmt"
	"math"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// Not urnary negation function
//
//    not(eq(5,5)) => false, true
//    not(eq("false")) => false, true
//
type Not struct{}

// Type bool
func (m *Not) Type() value.ValueType { return value.BoolType }

func (m *Not) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected exactly 1 arg for NOT(arg) but got %s", n)
	}
	return notEval, nil
}
func notEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	boolVal, ok := value.ValueToBool(args[0])
	if ok {
		return value.NewBoolValue(!boolVal), true
	}
	return value.BoolValueFalse, false
}

// Equal function?  returns true if items are equal
//
//    // given context   {"name":"wil","event":"stuff", "int4": 4}
//
//    eq(int4,5)  => false
//
type Eq struct{}

// Type bool
func (m *Eq) Type() value.ValueType { return value.BoolType }

func (m *Eq) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for EQ(lh, rh) but got %s", n)
	}
	return equalEval, nil
}
func equalEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	eq, err := value.Equal(vals[0], vals[1])
	if err == nil {
		return value.NewBoolValue(eq), true
	}
	return value.BoolValueFalse, false
}

// Ne Not Equal function?  returns true if items are equal
//
//    // given   {"5s":"5","item4":4,"item4s":"4"}
//
//    ne(`5s`,5) => true, true
//    ne(`not_a_field`,5) => false, true
//    ne(`item4s`,5) => false, true
//    ne(`item4`,5) => false, true
//
type Ne struct{}

// Type bool
func (m *Ne) Type() value.ValueType { return value.BoolType }
func (m *Ne) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for NE(lh, rh) but got %s", n)
	}
	return notEqualEval, nil
}
func notEqualEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	eq, err := value.Equal(vals[0], vals[1])
	if err == nil {
		return value.NewBoolValue(!eq), true
	}
	return value.BoolValueFalse, false
}

// Gt GreaterThan is left hand > right hand.
// Must be able to convert items to Floats.
//
//     gt(5,6)  => true, true
type Gt struct{}

// Type bool
func (m *Gt) Type() value.ValueType { return value.BoolType }
func (m *Gt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for Gt(lh, rh) but got %s", n)
	}
	return greaterThanEval, nil
}

func greaterThanEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left > right), true
}

// Ge GreaterThan or Equal func. Must be able to convert items to Floats.
//
type Ge struct{}

// Type bool
func (m *Ge) Type() value.ValueType { return value.BoolType }
func (m *Ge) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for GE(lh, rh) but got %s", n)
	}
	return greatherThanOrEqualEval, nil
}
func greatherThanOrEqualEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left >= right), true
}

// Le Less Than or Equal. Must be able to convert items to Floats.
//
type Le struct{}

// Type bool
func (m *Le) Type() value.ValueType { return value.BoolType }
func (m *Le) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for Le(lh, rh) but got %s", n)
	}
	return lessThanOrEqualEval, nil
}
func lessThanOrEqualEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left <= right), true
}

// Lt Less Than Must be able to convert items to Floats
//
//     lt(5, 6)  => true
//
type Lt struct{}

// Type bool
func (m *Lt) Type() value.ValueType { return value.BoolType }

func (m *Lt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 arg for Lt(lh, rh) but got %s", n)
	}
	return lessThanEval, nil
}
func lessThanEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left < right), true
}

// Exists Answers True/False if the field exists and is non null
//
//     exists(real_field) => true
//     exists("value") => true
//     exists("") => false
//     exists(empty_field) => false
//     exists(2) => true
//     exists(todate(date_field)) => true
//
type Exists struct{}

// Type bool
func (m *Exists) Type() value.ValueType { return value.BoolType }
func (m *Exists) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected exactly 1 arg for Exists(arg) but got %s", n)
	}
	return existsEval, nil
}

func existsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	switch node := args[0].(type) {
	// case *expr.IdentityNode:
	// 	_, ok := ctx.Get(node.Text)
	// 	if ok {
	// 		return value.BoolValueTrue, true
	// 	}
	// 	return value.BoolValueFalse, true
	// case *expr.StringNode:
	// 	_, ok := ctx.Get(node.Text)
	// 	if ok {
	// 		return value.BoolValueTrue, true
	// 	}
	// 	return value.BoolValueFalse, true
	case value.StringValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.BoolValue:
		return value.BoolValueTrue, true
	case value.NumberValue:
		return value.BoolValueTrue, true
	case value.IntValue:
		return value.BoolValueTrue, true
	case value.TimeValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.StringsValue, value.SliceValue, value.MapIntValue:
		return value.BoolValueTrue, true
	case value.JsonValue, value.StructValue:
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}

// Any Answers True/False if any of the arguments evaluate to truish (javascripty)
// type definintion of true
//
// Rules for if True:
//     int != 0
//     string != ""
//     boolean natively supported true/false
//     time != time.IsZero()
//
// Examples:
//     any(item,item2)  => true, true
//     any(not_field)   => false, true
type Any struct{}

// Type bool
func (m *Any) Type() value.ValueType { return value.BoolType }

func (m *Any) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Any(arg, arg, ...) but got %s", n)
	}
	return anyEval, nil
}
func anyEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v == nil || v.Err() || v.Nil() {
			// continue
		} else {
			return value.NewBoolValue(true), true
		}
	}
	return value.NewBoolValue(false), true
}

// All Answers True/False if all of the arguments evaluate to truish (javascripty)
// type definintion of true.  Non-Nil, non-Error, values.
//
// Rules for if True:
//     int != 0
//     string != ""
//     boolean natively supported true/false
//     time != time.IsZero()
//
// Examples:
//     all("hello",2, true) => true
//     all("hello",0,true)  => false
//     all("",2, true)      => false
type All struct{}

func allEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			return value.NewBoolValue(false), true
		}
		switch vt := v.(type) {
		case value.BoolValue:
			if vt.Val() == false {
				return value.NewBoolValue(false), true
			}
		case value.NumericValue:
			if iv := vt.Int(); iv != 0 {
				return value.NewBoolValue(false), true
			}
		}
	}
	return value.NewBoolValue(true), true
}
func (m *All) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf(`Expected 1 or more args for All(true, tobool(item)) but got %s`, n)
	}
	return allEval, nil
}

// Type is BoolType for All function
func (m *All) Type() value.ValueType { return value.BoolType }
