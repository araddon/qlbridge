package builtins

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Avg average of values.  Note, this function DOES NOT persist state doesn't aggregate
// across multiple calls.  That would be responsibility of write context.
//
//    avg(1,2,3) => 2.0, true
//    avg("hello") => math.NaN, false
//
type Avg struct{}

// Type is NumberType
func (m *Avg) Type() value.ValueType { return value.NumberType }
func (m *Avg) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Avg(arg, arg, ...) but got %s", n)
	}
	return avgEval, nil
}
func (m *Avg) IsAgg() bool { return true }

func avgEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	avg := float64(0)
	ct := 0
	for _, val := range vals {
		switch v := val.(type) {
		case value.StringsValue:
			for _, sv := range v.Val() {
				if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
					avg += fv
					ct++
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.SliceValue:
			for _, sv := range v.Val() {
				if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
					avg += fv
					ct++
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.StringValue:
			if fv, ok := value.StringToFloat64(v.Val()); ok {
				avg += fv
				ct++
			}
		case value.NumericValue:
			avg += v.Float()
			ct++
		}
	}
	if ct > 0 {
		return value.NewNumberValue(avg / float64(ct)), true
	}
	return value.NumberNaNValue, false
}

// Sum function to add values. Note, this function DOES NOT persist state doesn't aggregate
// across multiple calls.  That would be responsibility of write context.
//
//   sum(1, 2, 3) => 6
//   sum(1, "horse", 3) => nan, false
//
type Sum struct{}

// Type is number
func (m *Sum) Type() value.ValueType { return value.NumberType }

// IsAgg yes sum is an agg.
func (m *Sum) IsAgg() bool { return true }

func (m *Sum) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Sum(arg, arg, ...) but got %s", n)
	}
	return sumEval, nil
}

func sumEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	sumval := float64(0)
	for _, val := range vals {
		if val == nil || val.Nil() || val.Err() {
			// we don't need to evaluate if nil or error
		} else {
			switch v := val.(type) {
			case value.StringValue:
				if fv, ok := value.StringToFloat64(v.Val()); ok && !math.IsNaN(fv) {
					sumval += fv
				}
			case value.StringsValue:
				for _, sv := range v.Val() {
					if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
						sumval += fv
					}
				}
			case value.SliceValue:
				for _, sv := range v.Val() {
					if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
						sumval += fv
					} else {
						return value.NumberNaNValue, false
					}
				}
			case value.NumericValue:
				fv := v.Float()
				if !math.IsNaN(fv) {
					sumval += fv
				}
			default:
				// Do we silently drop, or fail?
				return value.NumberNaNValue, false
			}
		}
	}
	if sumval == float64(0) {
		return value.NumberNaNValue, false
	}
	return value.NewNumberValue(sumval), true
}

// Count Return int value 1 if non-nil/zero.  This should be renamed Increment
// and in general is a horrible, horrible function that needs to be replaced
// with occurrences of value, ignores the value and ensures it is non null
//
//    count(anyvalue)     =>  1, true
//    count(not_number)   =>  -- 0, false
//
type Count struct{}

// Type is Integer
func (m *Count) Type() value.ValueType { return value.IntType }
func (m *Count) IsAgg() bool           { return true }

func (m *Count) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected max 1 arg for count(arg) but got %s", n)
	}
	return incrementEval, nil
}

func incrementEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if vals[0] == nil || vals[0].Err() || vals[0].Nil() {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(1), true
}
