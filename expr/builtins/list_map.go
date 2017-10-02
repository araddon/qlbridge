package builtins

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// len length of array types
//
//    len([1,2,3])     =>  3, true
//    len(not_a_field)   =>  -- NilInt, false
//
type Length struct{}

// Type is IntType
func (m *Length) Type() value.ValueType { return value.IntType }
func (m *Length) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Length(arg) but got %s", n)
	}
	return lenEval, nil
}
func lenEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	switch node := args[0].(type) {
	case value.StringValue:
		return value.NewIntValue(int64(len(node.Val()))), true
	case value.BoolValue:
		return value.NewIntValue(0), true
	case value.NumberValue:
		return value.NewIntValue(0), true
	case value.IntValue:
		return value.NewIntValue(0), true
	case value.TimeValue:
		return value.NewIntValue(0), true
	case value.StringsValue:
		return value.NewIntValue(int64(node.Len())), true
	case value.SliceValue:
		return value.NewIntValue(int64(node.Len())), true
	case value.ByteSliceValue:
		return value.NewIntValue(int64(node.Len())), true
	case value.MapIntValue:
		return value.NewIntValue(int64(len(node.Val()))), true
	case value.MapNumberValue:
		return value.NewIntValue(int64(len(node.Val()))), true
	case value.MapStringValue:
		return value.NewIntValue(int64(len(node.Val()))), true
	case value.MapValue:
		return value.NewIntValue(int64(len(node.Val()))), true
	case nil, value.NilValue:
		return value.NewIntNil(), false
	}
	return value.NewIntNil(), false
}

// ArrayIndex  array.index choose the nth element of an array
//
//     // given context input of
//     "items" = [1,2,3]
//
//     array.index(items, 1)     =>  1, true
//     array.index(items, 5)     =>  nil, false
//     array.index(items, -1)    =>  3, true
//
type ArrayIndex struct{}

// Type unknown - returns single value from SliceValue array
func (m *ArrayIndex) Type() value.ValueType { return value.UnknownType }
func (m *ArrayIndex) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 arg for ArrayIndex(array, index) but got %s", n)
	}
	return arrayIndexEval, nil
}
func arrayIndexEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	idx, ok := value.ValueToInt(args[1])
	if !ok {
		return nil, false
	}

	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return nil, false
	}
	switch node := args[0].(type) {
	case value.Slice:

		slvals := node.SliceValue()

		if idx < 0 {
			idx = len(slvals) + idx
		}
		if len(slvals) <= idx || idx < 0 {
			return nil, false
		}
		return slvals[idx], true
	}
	return nil, false
}

// array.slice  slice element m -> n of a slice.  First arg must be a slice.
//
//    // given context of
//    "items" = [1,2,3,4,5]
//
//    array.slice(items, 1, 3)     =>  [2,3], true
//    array.slice(items, 2)        =>  [3,4,5], true
//    array.slice(items, -2)       =>  [4,5], true
type ArraySlice struct{}

// Type Unknown for Array Slice
func (m *ArraySlice) Type() value.ValueType { return value.UnknownType }

// Validate must be at least 2 args, max of 3
func (m *ArraySlice) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 || len(n.Args) > 3 {
		return nil, fmt.Errorf("Expected 2 OR 3 args for ArraySlice(array, start, [end]) but got %s", n)
	}
	return arraySliceEval, nil
}

func arraySliceEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return nil, false
	}

	idx, ok := value.ValueToInt(args[1])
	if !ok {
		return nil, false
	}

	idx2 := 0
	if len(args) == 3 {
		idx2, ok = value.ValueToInt(args[2])
		if !ok {
			return nil, false
		}
	}

	switch node := args[0].(type) {
	case value.StringsValue:

		svals := node.Val()

		if idx < 0 {
			idx = len(svals) + idx
		}
		if idx2 < 0 {
			idx2 = len(svals) + idx2
			if idx2 < idx {
				return nil, false
			}
		}

		if len(svals) <= idx || idx < 0 {
			return nil, false
		}
		if len(svals) < idx2 || idx2 < 0 {
			return nil, false
		}
		if len(args) == 2 {
			// array.slice(item, start)
			return value.NewStringsValue(svals[idx:]), true
		} else {
			// array.slice(item, start, end)
			return value.NewStringsValue(svals[idx:idx2]), true
		}
	case value.SliceValue:

		svals := node.Val()

		if idx < 0 {
			idx = len(svals) + idx
		}
		if idx2 < 0 {
			idx2 = len(svals) + idx2
			if idx2 < idx {
				return nil, false
			}
		}

		if len(svals) <= idx || idx < 0 {
			return nil, false
		}
		if len(svals) < idx2 || idx2 < 0 {
			return nil, false
		}
		if len(args) == 2 {
			// array.slice(item, start)
			return value.NewSliceValues(svals[idx:]), true
		}
		// array.slice(item, start, end)
		return value.NewSliceValues(svals[idx:idx2]), true

	}
	return nil, false
}

// Map Create a map from two values.   If the right side value is nil
// then does not evaluate.
//
//     map(left, right)    => map[string]value{left:right}
//
type MapFunc struct{}

// Type is MapValueType
func (m *MapFunc) Type() value.ValueType { return value.MapValueType }

func (m *MapFunc) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 arg for MapFunc(key, value) but got %s", n)
	}
	return mapEval, nil
}

func mapEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Nil() || args[0].Err() {
		return value.EmptyMapValue, false
	}
	if args[0] == nil || args[1].Nil() || args[1].Nil() {
		return value.EmptyMapValue, false
	}
	// What should the map function be if lh is slice/map?
	return value.NewMapValue(map[string]interface{}{args[0].ToString(): args[1].Value()}), true
}

// MapTime()    Create a map[string]time of each key
//
//    maptime(field)    => map[string]time{field_value:message_timestamp}
//    maptime(field, timestamp) => map[string]time{field_value:timestamp}
//
type MapTime struct{}

// Type MapTime
func (m *MapTime) Type() value.ValueType { return value.MapTimeType }
func (m *MapTime) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 || len(n.Args) > 2 {
		return nil, fmt.Errorf("Expected 1 or 2 args for MapTime() but got %s", n)
	}
	return mapTimeEval, nil
}
func mapTimeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	var k string
	var ts time.Time
	switch len(args) {
	case 1:
		kitem := args[0]
		if kitem.Err() || kitem.Nil() {
			return value.EmptyMapTimeValue, false
		}
		k = strings.ToLower(kitem.ToString())
		ts = ctx.Ts()
	case 2:
		kitem := args[0]
		if kitem.Err() || kitem.Nil() {
			return value.EmptyMapTimeValue, false
		}
		k = strings.ToLower(kitem.ToString())

		var ok bool
		ts, ok = value.ValueToTime(args[1])
		if !ok {
			return value.EmptyMapTimeValue, false
		}
	}
	return value.NewMapTimeValue(map[string]time.Time{k: ts}), true
}

// Match a simple pattern match on KEYS (not values) and build a map of all matched values.
// Matched portion is replaced with empty string.
// - May pass as many match strings as you want.
// - Must match on Prefix of key.
//
//  given input context of:
//     {"score_value":24,"event_click":true, "tag_apple": "apple", "label_orange": "orange"}
//
//     match("score_") => {"value":24}
//     match("amount_") => false
//     match("event_") => {"click":true}
//     match("label_","tag_") => {"apple":"apple","orange":"orange"}
type Match struct{}

// Type is MapValueType
func (m *Match) Type() value.ValueType { return value.MapValueType }

func (m *Match) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more arg for Match(arg) but got %s", n)
	}
	return matchEval, nil
}

func matchEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]interface{})
	for _, item := range args {
		switch node := item.(type) {
		case value.StringValue:
			matchKey := node.Val()
			// Iterate through every value in Context
			for rowKey, val := range ctx.Row() {
				if strings.HasPrefix(rowKey, matchKey) && val != nil {
					newKey := strings.Replace(rowKey, matchKey, "", 1)
					if newKey != "" {
						mv[newKey] = val
					}
				}
			}
		}
	}
	if len(mv) > 0 {
		return value.NewMapValue(mv), true
	}

	return value.EmptyMapValue, false
}

// MapKeys:  Take a map and extract array of keys
//
//    //given input:
//    {"tag.1":"news","tag.2":"sports"}
//
//    mapkeys(match("tag.")) => []string{"news","sports"}
//
type MapKeys struct{}

// Type []string aka strings
func (m *MapKeys) Type() value.ValueType { return value.StringsType }
func (m *MapKeys) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 arg for MapKeys(arg) but got %s", n)
	}
	return mapKeysEval, nil
}

func mapKeysEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]bool)
	for _, item := range args {
		switch node := item.(type) {
		case value.Map:
			for key := range node.MapValue().Val() {
				mv[key] = true
			}
		}
	}
	if len(mv) == 0 {
		return value.EmptyStringsValue, false
	}
	keys := make([]string, 0, len(mv))
	for k := range mv {
		keys = append(keys, k)
	}

	return value.NewStringsValue(keys), true
}

// MapValues:  Take a map and extract array of values
//
//    // given input:
//    {"tag.1":"news","tag.2":"sports"}
//
//    mapvalue(match("tag.")) => []string{"1","2"}
//
type MapValues struct{}

// Type strings aka []string
func (m *MapValues) Type() value.ValueType { return value.StringsType }
func (m *MapValues) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for MapValues(arg) but got %s", n)
	}
	return mapValuesEval, nil
}
func mapValuesEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]bool)
	for _, item := range args {
		switch node := item.(type) {
		case value.Map:
			for _, val := range node.MapValue().Val() {
				if val != nil {
					mv[val.ToString()] = true
				}
			}
		}
	}
	if len(mv) == 0 {
		return value.EmptyStringsValue, false
	}
	result := make([]string, 0, len(mv))
	for k := range mv {
		result = append(result, k)
	}

	return value.NewStringsValue(result), true
}

// MapInvert:  Take a map and invert key/values
//
//    // given input:
//    tags = {"1":"news","2":"sports"}
//
//    mapinvert(tags) => map[string]string{"news":"1","sports":"2"}
//
type MapInvert struct{}

// Type MapValue
func (m *MapInvert) Type() value.ValueType { return value.MapValueType }

func (m *MapInvert) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for MapInvert(arg) but got %s", n)
	}
	return mapInvertEval, nil
}

func mapInvertEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]string)
	for _, val := range args {
		switch node := val.(type) {
		case value.Map:
			for key, val := range node.MapValue().Val() {
				if val != nil {
					mv[val.ToString()] = key
				}
			}
		}
	}
	if len(mv) == 0 {
		return value.EmptyMapStringValue, false
	}
	return value.NewMapStringValue(mv), true
}
