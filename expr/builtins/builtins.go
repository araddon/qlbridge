// Builtin functions are a library of functions natively available in
// qlbridge expression evaluation although adding your own is easy.
package builtins

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"math"
	"net/mail"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/leekchan/timeutil"
	"github.com/lytics/datemath"
	"github.com/mb0/glob"
	"github.com/pborman/uuid"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY
var loadOnce sync.Once

const yymmTimeLayout = "0601"

func LoadAllBuiltins() {
	loadOnce.Do(func() {

		// math
		expr.FuncAdd("sqrt", SqrtFunc)
		expr.FuncAdd("pow", PowFunc)

		// agregate ops
		expr.AggFuncAdd("count", &Count{})
		expr.AggFuncAdd("avg", &Avg{})
		expr.AggFuncAdd("sum", &Sum{})

		// logical
		expr.FuncAdd("gt", &Gt{})
		expr.FuncAdd("ge", &Ge{})
		expr.FuncAdd("ne", &Ne{})
		expr.FuncAdd("le", &Le{})
		expr.FuncAdd("lt", &Lt{})
		expr.FuncAdd("eq", &Eq{})
		expr.FuncAdd("not", &Not{})
		expr.FuncAdd("exists", &Exists{})
		expr.FuncAdd("map", &MapFunc{})

		// Date/Time functions
		expr.FuncAdd("now", &Now{})
		expr.FuncAdd("yy", &Yy{})
		expr.FuncAdd("yymm", &YyMm{})
		expr.FuncAdd("mm", &Mm{})
		expr.FuncAdd("monthofyear", &Mm{})
		expr.FuncAdd("dayofweek", &DayOfWeek{})
		expr.FuncAdd("hourofday", &HourOfDay{})
		expr.FuncAdd("hourofweek", &HourOfWeek{})
		expr.FuncAdd("totimestamp", &ToTimestamp{})
		expr.FuncAdd("todate", &ToDate{})
		expr.FuncAdd("seconds", TimeSeconds)
		expr.FuncAdd("maptime", MapTime)
		expr.FuncAdd("extract", TimeExtractFunc)
		expr.FuncAdd("strftime", TimeExtractFunc)

		// String Functions
		expr.FuncAdd("contains", &Contains{})
		expr.FuncAdd("tolower", &Lower{})
		expr.FuncAdd("tostring", &ToString{})
		expr.FuncAdd("toint", &ToInt{})
		expr.FuncAdd("tonumber", &ToNumber{})
		expr.FuncAdd("uuid", &UuidGenerate{})
		expr.FuncAdd("split", &Split{})
		expr.FuncAdd("replace", &Replace{})
		expr.FuncAdd("join", &Join{})
		expr.FuncAdd("hassuffix", &HasSuffix{})
		expr.FuncAdd("hasprefix", &HasPrefix{})

		// array, string
		expr.FuncAdd("len", &Length{})
		expr.FuncAdd("array.index", &ArrayIndex{})
		expr.FuncAdd("array.slice", &ArraySlice{})

		// selection
		expr.FuncAdd("oneof", &OneOf{})
		expr.FuncAdd("match", &Match{})
		expr.FuncAdd("mapkeys", &MapKeys{})
		expr.FuncAdd("mapvalues", &MapValues{})
		expr.FuncAdd("mapinvert", &MapInvert{})
		expr.FuncAdd("any", &Any{})
		expr.FuncAdd("all", &All{})
		expr.FuncAdd("filter", &Filter{})

		// special items}
		expr.FuncAdd("email", EmailFunc)
		expr.FuncAdd("emaildomain", EmailDomainFunc)
		expr.FuncAdd("emailname", EmailNameFunc)
		expr.FuncAdd("domain", DomainFunc)
		expr.FuncAdd("domains", DomainsFunc)
		expr.FuncAdd("host", HostFunc)
		expr.FuncAdd("hosts", HostsFunc)
		expr.FuncAdd("path", UrlPath)
		expr.FuncAdd("qs", Qs)
		expr.FuncAdd("urlmain", UrlMain)
		expr.FuncAdd("urlminusqs", UrlMinusQs)
		expr.FuncAdd("urldecode", UrlDecode)

		// Hashing functions
		expr.FuncAdd("hash.md5", HashMd5Func)
		expr.FuncAdd("hash.sha1", HashSha1Func)
		expr.FuncAdd("hash.sha256", HashSha256Func)
		expr.FuncAdd("hash.sha512", HashSha512Func)

		// MySQL Builtins
		expr.FuncAdd("cast", &Cast{})
		expr.FuncAdd("char_length", &Length{})
	})
}

type baseFunc struct{}

func (*baseFunc) Validate(n *expr.FuncNode) error { return nil }
func (*baseFunc) IsAgg() bool                     { return false }

func emptyFunc(ctx expr.EvalContext, _ value.Value) (value.Value, bool) { return nil, true }

type Avg struct{ baseFunc }

// avg:   average doesn't aggregate across calls, that would be
//        responsibility of write context, but does return number
//
//   avg(1,2,3) => 2.0, true
//   avg("hello") => math.NaN, false
//
func (m *Avg) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
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

func (*Avg) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf("Expected 1 or more args for Avg(arg, arg, ...) but got %s", n)
	}
	return nil
}

type Sum struct{ baseFunc }

// Sum  function to add values
//
//   sum(1, 2, 3) => 6
//   sum(1, "horse", 3) => nan, false
//
func (m *Sum) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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

func (*Sum) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf("Expected 1 or more args for Sum(arg, arg, ...) but got %s", n)
	}
	return nil
}

type Count struct{ baseFunc }

// Count:   This should be renamed Increment
//      and in general is a horrible, horrible function that needs to be replaced
//      with occurences of value, ignores the value and ensures it is non null
//
//      count(anyvalue)     =>  1, true
//      count(not_number)   =>  -- 0, false
//
func (m *Count) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if len(vals) == 0 {
		return value.NewIntValue(1), true
	}
	if vals[0] == nil || vals[0].Err() || vals[0].Nil() {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(1), true
}
func (*Count) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected max 1 arg for Count(arg) but got %s", n)
	}
	return nil
}
func (*Count) IsAgg() bool { return true }

// Sqrt
//
//      sqrt(4)            =>  2, true
//      sqrt(9)            =>  3, true
//      sqrt(not_number)   =>  0, false
//
func SqrtFunc(ctx expr.EvalContext, val value.Value) (value.NumberValue, bool) {
	nv, ok := val.(value.NumericValue)
	if !ok {
		return value.NewNumberNil(), false
	}
	if val.Err() || val.Nil() {
		return value.NewNumberNil(), false
	}
	fv := nv.Float()
	fv = math.Sqrt(fv)
	//u.Infof("???   vals=[%v]", val.Value())
	return value.NewNumberValue(fv), true
}

// Pow:   exponents, raise x to the power of y
//
//      pow(5,2)            =>  25, true
//      pow(3,2)            =>  9, true
//      pow(not_number,2)   =>  NilNumber, false
//
func PowFunc(ctx expr.EvalContext, val, toPower value.Value) (value.NumberValue, bool) {
	//Pow(x, y float64) float64
	//u.Infof("powFunc:  %T:%v %T:%v ", val, val.Value(), toPower, toPower.Value())
	if val.Err() || val.Nil() {
		return value.NewNumberNil(), false
	}
	if toPower.Err() || toPower.Nil() {
		return value.NewNumberNil(), false
	}
	fv, _ := value.ToFloat64(val.Rv())
	pow, _ := value.ToFloat64(toPower.Rv())
	if math.IsNaN(fv) || math.IsNaN(pow) {
		return value.NewNumberNil(), false
	}
	fv = math.Pow(fv, pow)
	//u.Infof("pow ???   vals=[%v]", fv, pow)
	return value.NewNumberValue(fv), true
}

type Not struct{ baseFunc }

//  Not:   urnary negation function
//
//      not(eq(5,5)) => false, true
//      not(eq("false")) => false, true
//
func (m *Not) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	boolVal, ok := value.ToBool(vals[0].Rv())
	if ok {
		return value.NewBoolValue(!boolVal), true
	}
	return value.BoolValueFalse, false
}
func (*Not) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected exactly 1 args for NOT(arg) but got %s", n)
	}
	return nil
}

type Eq struct{ baseFunc }

//  Equal function?  returns true if items are equal
//
//   given   {"name":"wil","event":"stuff", "int4": 4}
//
//      eq(int4,5)  => false
//
func (m *Eq) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	eq, err := value.Equal(vals[0], vals[1])
	if err == nil {
		return value.NewBoolValue(eq), true
	}
	return value.BoolValueFalse, false
}
func (*Eq) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected exactly 2 args for EQ(lh, rh) but got %s", n)
	}
	return nil
}

type Ne struct{ baseFunc }

//  Not Equal function?  returns true if items are equal
//
//   given   {"5s":"5","item4":4,"item4s":"4"}
//
//      ne(`5s`,5) => true, true
//      ne(`not_a_field`,5) => false, true
//      ne(`item4s`,5) => false, true
//      ne(`item4`,5) => false, true
//
func (m *Ne) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	eq, err := value.Equal(vals[0], vals[1])
	if err == nil {
		return value.NewBoolValue(!eq), true
	}
	return value.BoolValueFalse, false
}
func (*Ne) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected exactly 2 args for NE(lh, rh) but got %s", n)
	}
	return nil
}

type Gt struct{ baseFunc }

// > GreaterThan
//  Must be able to convert items to Floats or else not ok
//
func (m *Gt) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left > right), true
}
func (*Gt) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected exactly 2 args for Gt(lh, rh) but got %s", n)
	}
	return nil
}

type Ge struct{ baseFunc }

// >= GreaterThan or Equal
//  Must be able to convert items to Floats or else not ok
//
func (m *Ge) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left >= right), true
}
func (*Ge) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected exactly 2 args for GE(lh, rh) but got %s", n)
	}
	return nil
}

type Le struct{ baseFunc }

// <= Less Than or Equal
//  Must be able to convert items to Floats or else not ok
//
func (m *Le) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left <= right), true
}
func (*Le) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected exactly 2 args for Le(lh, rh) but got %s", n)
	}
	return nil
}

type Lt struct{ baseFunc }

// Lt   < Less Than
//  Must be able to convert items to Floats or else not ok
//
func (m *Lt) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left < right), true
}
func (*Lt) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected exactly 2 arg for Lt(lh, rh) but got %s", n)
	}
	return nil
}

type Exists struct{ baseFunc }

// Exists:  Answers True/False if the field exists and is non null
//
//     exists(real_field) => true
//     exists("value") => true
//     exists("") => false
//     exists(empty_field) => false
//     exists(2) => true
//     exists(todate(date_field)) => true
//
func (m *Exists) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	switch node := vals[0].(type) {
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
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.IntValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.TimeValue:
		if node.Nil() {
			return value.BoolValueFalse, true
		}
		return value.BoolValueTrue, true
	case value.StringsValue, value.SliceValue, value.MapIntValue:
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}
func (*Exists) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected exactly 1 arg for Exists(arg) but got %s", n)
	}
	return nil
}

type Length struct{ baseFunc }

// len:   length of array types
//
//      len([1,2,3])     =>  3, true
//      len(not_a_field)   =>  -- NilInt, false
//
func (m *Length) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	switch node := vals[0].(type) {
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

func (*Length) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 arg for Length(arg) but got %s", n)
	}
	return nil
}

type ArrayIndex struct{ baseFunc }

// array.index:   choose the nth element of an array
//
//   given: "items" = [1,2,3]
//
//      array.index(items, 1)     =>  1, true
//      array.index(items, 5)     =>  nil, false
//
func (m *ArrayIndex) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	idx, ok := value.ValueToInt(vals[1])
	if !ok {
		return nil, false
	}
	if vals[0] == nil || vals[0].Err() || vals[0].Nil() {
		return nil, false
	}
	switch node := vals[0].(type) {
	case value.Slice:
		slvals := node.SliceValue()
		if len(slvals) <= idx {
			return nil, false
		}
		return slvals[idx], true
	}
	return nil, false
}
func (*ArrayIndex) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected 2 arg for ArrayIndex(array, index) but got %s", n)
	}
	return nil
}

type ArraySlice struct{ baseFunc }

// array.slice:   slice element m -> n of a slice
//
//   given: "items" = [1,2,3,4,5]
//
//      array.slice(items, 1, 3)     =>  [2,3], true
//      array.slice(items, 2)        =>  [3,4,5], true
//
func (m *ArraySlice) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if vals[0] == nil || vals[0].Err() || vals[0].Nil() {
		return nil, false
	}

	idx, ok := value.ValueToInt(vals[1])
	if !ok || idx < 0 {
		return nil, false
	}

	idx2 := 0
	if len(vals) == 3 {
		idx2, ok = value.ValueToInt(vals[2])
		if !ok || idx2 < 0 {
			return nil, false
		}
	}

	switch node := vals[0].(type) {
	case value.StringsValue:

		svals := node.Val()
		if len(svals) <= idx {
			return nil, false
		}
		if len(svals) < idx2 {
			return nil, false
		}
		if len(vals) == 2 {
			// array.slice(item, start)
			return value.NewStringsValue(svals[idx:]), true
		} else {
			// array.slice(item, start, end)
			return value.NewStringsValue(svals[idx:idx2]), true
		}
	case value.SliceValue:

		svals := node.Val()
		if len(svals) <= idx {
			return nil, false
		}
		if len(svals) < idx2 {
			return nil, false
		}
		if len(vals) == 2 {
			// array.slice(item, start)
			return value.NewSliceValues(svals[idx:]), true
		} else {
			// array.slice(item, start, end)
			return value.NewSliceValues(svals[idx:idx2]), true
		}
	}
	return nil, false
}
func (*ArraySlice) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 2 || len(n.Args) > 3 {
		return fmt.Errorf("Expected 2 OR 3 args for ArraySlice(array, start, [end]) but got %s", n)
	}
	return nil
}

type MapFunc struct{ baseFunc }

// Map()    Create a map from two values.   If the right side value is nil
//    then does not evaluate
//
//  Map(left, right)    => map[string]value{left:right}
//
func (m *MapFunc) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return value.EmptyMapValue, false
	}
	if vals[0] == nil || vals[1].Nil() || vals[1].Nil() {
		return value.EmptyMapValue, false
	}
	// What should the map function be if lh is slice/map?
	return value.NewMapValue(map[string]interface{}{vals[0].ToString(): vals[1].Value()}), true
}
func (*MapFunc) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected 2 arg for MapFunc(key, value) but got %s", n)
	}
	return nil
}

type Match struct{ baseFunc }

// match:  Match a simple pattern match and return matched value
//
//  given input:
//      {"score_value":24,"event_click":true}
//
//     match("score_") => {"value":24}
//     match("amount_") => false
//     match("event_") => {"click":true}
//
func (m *Match) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	mv := make(map[string]interface{})
	for _, item := range vals {
		switch node := item.(type) {
		case value.StringValue:
			matchKey := node.Val()
			for rowKey, val := range ctx.Row() {
				if strings.HasPrefix(rowKey, matchKey) && val != nil {
					newKey := strings.Replace(rowKey, matchKey, "", 1)
					if newKey != "" {
						mv[newKey] = val
					}
				}
			}
		default:
			u.Warnf("unsuported key type: %T %v", item, item)
		}
	}
	if len(mv) > 0 {
		return value.NewMapValue(mv), true
	}

	return value.EmptyMapValue, false
}
func (*Match) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf("Expected 1 or more arg for Match(arg) but got %s", n)
	}
	return nil
}

type MapKeys struct{ baseFunc }

// MapKeys:  Take a map and extract array of keys
//
//  given input:
//      {"tag.1":"news","tag.2":"sports"}
//
//     mapkeys(match("tag.")) => []string{"news","sports"}
//
func (m *MapKeys) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	mv := make(map[string]bool)
	for _, item := range vals {
		switch node := item.(type) {
		case value.Map:
			for key, _ := range node.MapValue().Val() {
				mv[key] = true
			}
		default:
			u.Debugf("unsuported key type: %T %v", item, item)
		}
	}
	keys := make([]string, 0, len(mv))
	for k, _ := range mv {
		keys = append(keys, k)
	}

	return value.NewStringsValue(keys), true
}
func (*MapKeys) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf("Expected 1 arg for MapKeys(arg) but got %s", n)
	}
	return nil
}

type MapValues struct{ baseFunc }

// MapValues:  Take a map and extract array of values
//
//  given input:
//      {"tag.1":"news","tag.2":"sports"}
//
//     mapvalue(match("tag.")) => []string{"1","2"}
//
func (m *MapValues) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	mv := make(map[string]bool)
	for _, item := range vals {
		switch node := item.(type) {
		case value.Map:
			for _, val := range node.MapValue().Val() {
				if val != nil {
					mv[val.ToString()] = true
				}
			}
		case nil, value.NilValue:
			// nil, nothing to do
		default:
			u.Debugf("unsuported key type: %T %v", item, item)
		}
	}
	result := make([]string, 0, len(mv))
	for k, _ := range mv {
		result = append(result, k)
	}

	return value.NewStringsValue(result), true
}
func (*MapValues) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 arg for MapValues(arg) but got %s", n)
	}
	return nil
}

type MapInvert struct{ baseFunc }

// MapInvert:  Take a map and invert key/values
//
//  given input:
//     tags = {"1":"news","2":"sports"}
//
//     mapinvert(tags) => map[string]string{"news":"1","sports":"2"}
//
func (m *MapInvert) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	mv := make(map[string]string)
	for _, val := range vals {
		switch node := val.(type) {
		case value.Map:
			for key, val := range node.MapValue().Val() {
				if val != nil {
					mv[val.ToString()] = key
				}
			}
		default:
			u.Debugf("unsuported key type: %T %v", val, val)
		}
	}
	return value.NewMapStringValue(mv), true
}
func (*MapInvert) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf("Expected 1 arg for MapInvert(arg) but got %s", n)
	}
	return nil
}

type UuidGenerate struct{ baseFunc }

// uuid generates a new uuid
//
func (m *UuidGenerate) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	return value.NewStringValue(uuid.New()), true
}
func (*UuidGenerate) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 0 {
		return fmt.Errorf("Expected 0 arg for uuid() but got %s", n)
	}
	return nil
}

type Contains struct{ baseFunc }

// Contains does first arg string contain 2nd arg?
//   contain("alabama","red") => false
//
func (m *Contains) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, leftOk := value.ValueToString(vals[0])
	right, rightOk := value.ValueToString(vals[1])
	if !leftOk {
		// TODO:  this should be false, false?
		//        need to ensure doesn't break downstream
		return value.BoolValueFalse, true
	}
	if !rightOk {
		return value.BoolValueFalse, true
	}
	if left == "" || right == "" {
		return value.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}
func (*Contains) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf("Expected 2 args for Contains(str_value, contains_this) but got %s", n)
	}
	return nil
}

type Lower struct{ baseFunc }

// Lower string function
//   must be able to convert to string
//
func (m *Lower) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	val, ok := value.ValueToString(vals[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.ToLower(val)), true
}
func (*Lower) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 arg for Lower(arg) but got %s", n)
	}
	return nil
}

type ToString struct{ baseFunc }

// ToString cast as string
//   must be able to convert to string
//
func (m *ToString) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if vals[0] == nil || vals[0].Err() {
		return value.EmptyStringValue, false
	}
	if vals[0].Nil() {
		return value.EmptyStringValue, true
	}
	return value.NewStringValue(vals[0].ToString()), true
}
func (*ToString) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 arg for ToString(arg) but got %s", n)
	}
	return nil
}

type OneOf struct{ baseFunc }

// choose OneOf these fields, first non-null
func (m *OneOf) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			// continue, ignore
		} else if !value.IsNilIsh(v.Rv()) {
			return v, true
		}
	}
	return value.NilValueVal, true
}
func (*OneOf) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 2 {
		return fmt.Errorf("Expected 2 or more args for OneOf(arg, arg, ...) but got %s", n)
	}
	return nil
}

type Any struct{ baseFunc }

// Any:  Answers True/False if any of the arguments evaluate to truish (javascripty)
//       type definintion of true
//
//     int > 0 = true
//     string != "" = true
//
//
//     any(item,item2)  => true, true
//     any(not_field)   => false, true
//
func (m *Any) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v == nil || v.Err() || v.Nil() {
			// continue
		} else if !value.IsNilIsh(v.Rv()) {
			return value.NewBoolValue(true), true
		}
	}
	return value.NewBoolValue(false), true
}
func (*Any) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf("Expected 1 or more args for Any(arg, arg, ...) but got %s", n)
	}
	return nil
}

func FiltersFromArgs(filterVals []value.Value) []string {
	filters := make([]string, 0, len(filterVals))
	for _, fv := range filterVals {
		switch fv := fv.(type) {
		case value.SliceValue:
			for _, fv := range fv.Val() {
				matchKey := fv.ToString()
				if strings.Contains(matchKey, "%") {
					matchKey = strings.Replace(matchKey, "%", "*", -1)
				}
				filters = append(filters, matchKey)
			}
		case value.StringsValue:
			for _, fv := range fv.Val() {
				matchKey := fv
				if strings.Contains(matchKey, "%") {
					matchKey = strings.Replace(matchKey, "%", "*", -1)
				}
				filters = append(filters, matchKey)
			}
		default:
			matchKey := fv.ToString()
			if strings.Contains(matchKey, "%") {
				matchKey = strings.Replace(matchKey, "%", "*", -1)
			}
			filters = append(filters, matchKey)
		}
	}
	return filters
}

type Filter struct{ baseFunc }

// Filter  Filter out Values that match specified list of match filter criteria
//
//   - Operates on MapValue (map[string]interface{}), StringsValue ([]string), or string
//   - takes N Filter Criteria
//   - supports Matching:      "filter*" // matches  "filter_x", "filterstuff"
//
//  -- Filter a map of values by key to remove certain keys
//    filter(match("topic_"),key_to_filter, key2_to_filter)  => {"goodkey": 22}, true
//
// -- Filter out VALUES (not keys) from a list of []string{} for a specific value
//    filter(split("apples,oranges",","),"ora*")  => ["apples"], true
//
// -- Filter out values for single strings
//    filter("apples","app*")      => []string{}, true
//
func (m *Filter) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return nil, false
	}
	val := vals[0]
	filters := FiltersFromArgs(vals[1:])

	//u.Debugf("Filter():  %T:%v   filters:%v", val, val, filters)
	switch val := val.(type) {
	case value.MapValue:

		mv := make(map[string]interface{})

		for rowKey, v := range val.Val() {
			filteredOut := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, rowKey)
					if match {
						filteredOut = true
						break
					}
				} else {
					if strings.HasPrefix(rowKey, filter) && v != nil {
						filteredOut = true
						break
					}
				}
			}
			if !filteredOut {
				mv[rowKey] = v.Value()
			}
		}

		return value.NewMapValue(mv), true

	case value.StringValue:
		anyMatches := false
		for _, filter := range filters {
			if strings.Contains(filter, "*") {
				match, _ := glob.Match(filter, val.Val())
				if match {
					anyMatches = true
					break
				}
			} else {
				if strings.HasPrefix(val.Val(), filter) {
					anyMatches = true
					break
				}
			}
		}
		if anyMatches {
			return value.NilValueVal, true
		}
		return val, true
	case value.StringsValue:
		lv := make([]string, 0, val.Len())
		for _, sv := range val.Val() {
			filteredOut := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, sv)
					if match {
						filteredOut = true
						break
					}
				} else {
					if strings.HasPrefix(sv, filter) && sv != "" {
						filteredOut = true
						break
					}
				}
			}
			if !filteredOut {
				lv = append(lv, sv)
			}
		}

		return value.NewStringsValue(lv), true

	case nil, value.NilValue:
		// nothing we can do
		return nil, true
	default:
		u.Debugf("unsuported key type: %T %v", val, val)
	}

	//u.Warnf("could not find key: %T %v", item, item)
	return nil, false
}
func (*Filter) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 2 {
		return fmt.Errorf(`Expected 2 args for Filter("apples","ap") but got %s`, n)
	}
	return nil
}

type All struct{ baseFunc }

// All:  Answers True/False if all of the arguments evaluate to truish (javascripty)
//       type definintion of true
//
//     int > 0 = true
//     string != "" = true
//     boolean natively supported true/false
//
//
//     all("hello",2, true) => true
//     all("hello",0,true)  => false
//     all("",2, true)      => false
//
func (m *All) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			return value.NewBoolValue(false), true
		} else if value.IsNilIsh(v.Rv()) {
			return value.NewBoolValue(false), true
		}
		if nv, ok := v.(value.NumericValue); ok {
			if iv := nv.Int(); iv < 0 {
				return value.NewBoolValue(false), true
			}
			continue
		}
		switch vt := v.(type) {
		case value.TimeValue:
			if vt.Val().IsZero() {
				return value.NewBoolValue(false), true
			}
		case value.BoolValue:
			if vt.Val() == false {
				return value.NewBoolValue(false), true
			}
		}
	}
	return value.NewBoolValue(true), true
}
func (*All) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf(`Expected 1 or more args for All(true, tobool(item)) but got %s`, n)
	}
	return nil
}

type Split struct{ baseFunc }

// Split a string, accepts an optional with parameter
//
//     split(item, ",")
//
func (m *Split) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	sv, ok := value.ValueToString(vals[0])
	splitBy, splitByOk := value.ValueToString(vals[1])
	if !ok || !splitByOk {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if sv == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if splitBy == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	return value.NewStringsValue(strings.Split(sv, splitBy)), true
}
func (*Split) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf(`Expected 2 args for Split("apples,oranges",",") but got %s`, n)
	}
	return nil
}

type Replace struct{ baseFunc }

// Replace a string(s), accepts any number of parameters to replace
//    replaces with ""
//
//     replace("/blog/index.html", "/blog","")  =>  /index.html
//     replace("/blog/index.html", "/blog")  =>  /index.html
//     replace("/blog/index.html", "/blog/archive/","/blog")  =>  /blog/index.html
//     replace(item, "M")
//
func (m *Replace) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if len(vals) < 2 {
		return value.EmptyStringValue, false
	}
	val1 := vals[0].ToString()
	arg := vals[1]
	replaceWith := ""
	if len(vals) == 3 {
		replaceWith = vals[2].ToString()
	}
	if arg.Err() {
		return value.EmptyStringValue, false
	}
	val1 = strings.Replace(val1, arg.ToString(), replaceWith, -1)
	return value.NewStringValue(val1), true
}
func (*Replace) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 2 {
		return fmt.Errorf(`Expected 2 args for Replace("apples","ap") but got %s`, n)
	}
	return nil
}

type Join struct{ baseFunc }

// Join items together (string concatenation)
//
//   join("apples","oranges",",")   => "apples,oranges"
//   join(["apples","oranges"],",") => "apples,oranges"
//   join("apples","oranges","")    => "applesoranges"
//
func (m *Join) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) <= 1 {
		return value.EmptyStringValue, false
	}
	sep, ok := value.ValueToString(vals[len(vals)-1])
	if !ok {
		return value.EmptyStringValue, false
	}
	args := make([]string, 0)
	for i := 0; i < len(vals)-1; i++ {
		switch valTyped := vals[i].(type) {
		case value.SliceValue:
			svals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				svals[i] = sv.ToString()
			}
			args = append(args, svals...)
		case value.StringsValue:
			svals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				svals[i] = sv
			}
			args = append(args, svals...)
		case value.StringValue, value.NumberValue, value.IntValue:
			val := valTyped.ToString()
			if val == "" {
				continue
			}
			args = append(args, val)
		}
	}
	if len(args) == 0 {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.Join(args, sep)), true
}
func (*Join) Validate(n *expr.FuncNode) error {
	if len(n.Args) < 1 {
		return fmt.Errorf(`Expected 1 or more args for Join("apples","ap") but got %s`, n)
	}
	return nil
}

type HasPrefix struct{ baseFunc }

// HasPrefix string evaluation to see if string begins with
//
//   hasprefix("apples","ap")   => true
//   hasprefix("apples","o")   => false
//
func (m *HasPrefix) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	prefixStr := vals[1].ToString()
	if len(prefixStr) == 0 {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(strings.HasPrefix(vals[0].ToString(), prefixStr)), true
}
func (*HasPrefix) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf(`Expected 2 args for HasPrefix("apples","ap") but got %s`, n)
	}
	return nil
}

type HasSuffix struct{ baseFunc }

// HasSuffix string evaluation to see if string ends with
//
//   hassuffix("apples","es")   => true
//   hassuffix("apples","e")   => false
//
func (m *HasSuffix) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	suffixStr := vals[1].ToString()
	if suffixStr == "" {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(strings.HasSuffix(vals[0].ToString(), suffixStr)), true
}
func (*HasSuffix) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 2 {
		return fmt.Errorf(`Expected 2 args for HasSuffix("apples","es") but got %s`, n)
	}
	return nil
}

type Cast struct{ baseFunc }

// Cast :   type coercion
//
//   cast(identity AS <type>) => 5.0
//   cast(reg_date AS string) => "2014/01/12"
//
//  Types:  [char, string, int, float]
//
func (m *Cast) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	// identity AS identity
	//  0        1    2
	if len(vals) != 3 {
		return nil, false
	}
	if vals[0] == nil || vals[0].Type() == value.NilType {
		return nil, false
	}
	if vals[2] == nil || vals[2].Type() == value.NilType {
		return nil, false
	}
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
	//u.Debugf("cast  %#v  err=%v", val, err)
	if err != nil {
		return nil, false
	}
	return val, true
}
func (*Cast) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 3 {
		return fmt.Errorf("Expected 3 args for Cast(arg AS <type>) but got %s", n)
	}
	return nil
}

type ToInt struct{ baseFunc }

// Convert to Integer:   Best attempt at converting to integer
//
//   toint("5")          => 5, true
//   toint("5.75")       => 5, true
//   toint("5,555")      => 5555, true
//   toint("$5")         => 5, true
//   toint("5,555.00")   => 5555, true
//
func (m *ToInt) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (*ToInt) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 arg for ToInt(arg) but got %s", n)
	}
	return nil
}

type ToNumber struct{ baseFunc }

// Convert to Number:   Best attempt at converting to integer
//
//   tonumber("5") => 5.0
//   tonumber("5.75") => 5.75
//   tonumber("5,555") => 5555
//   tonumber("$5") => 5.00
//   tonumber("5,555.00") => 5555
//
func (m *ToNumber) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	fv, ok := value.ValueToFloat64(vals[0])
	if !ok {
		return value.NewNumberNil(), false
	}
	return value.NewNumberValue(fv), true
}
func (*ToNumber) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 arg for ToNumber(arg) but got %s", n)
	}
	return nil
}

type Now struct{ baseFunc }

// Get current time of Message (message time stamp) or else choose current
//   server time if none is available in message context
//
func (m *Now) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if ctx != nil && !ctx.Ts().IsZero() {
		t := ctx.Ts()
		return value.NewTimeValue(t), true
	}

	return value.NewTimeValue(time.Now().In(time.UTC)), true
}
func (*Now) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 0 {
		return fmt.Errorf("Expected 0 args for Now() but got %s", n)
	}
	return nil
}

type Yy struct{ baseFunc }

// Get year in integer from field, must be able to convert to date
//
//    yy()                 =>  15, true    // assuming it is 2015
//    yy("2014-03-01")     =>  14, true
//
func (m *Yy) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	yy := 0
	if len(vals) == 0 {
		if !ctx.Ts().IsZero() {
			yy = ctx.Ts().Year()
		} else {
			// Do we want to use Now()?
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Debugf("v=%v   %v", dateStr, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err != nil {
			return value.NewIntValue(0), false
		} else {
			yy = t.Year()
		}
	} else {
		return value.NewIntValue(0), false
	}

	if yy >= 2000 {
		yy = yy - 2000
	} else if yy >= 1900 {
		yy = yy - 1900
	}
	return value.NewIntValue(int64(yy)), true
}
func (*Yy) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected 0 or 1 args for Yy() but got %s", n)
	}
	return nil
}

type Mm struct{ baseFunc }

// Get month as integer from date
//   @optional timestamp (if not, gets from context reader)
//
//  mm()                =>  01, true  /// assuming message ts = jan 1
//  mm("2014-03-17")    =>  03, true
//
func (m *Mm) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Month())), true
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntValue(0), false
		}
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Month())), true
		}
	}

	return value.NewIntValue(0), false
}
func (*Mm) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected 0 args for Mm() but got %s", n)
	}
	return nil
}

type YyMm struct{ baseFunc }

// Get yymm in 4 digits from argument if supplied, else uses message context ts
//
func (m *YyMm) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.EmptyStringValue, false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	}

	return value.EmptyStringValue, false
}
func (*YyMm) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected 0 or 1 args for YyMm() but got %s", n)
	}
	return nil
}

type DayOfWeek struct{ baseFunc }

// day of week [0-6]
func (m *DayOfWeek) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Weekday())), true
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntNil(), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday())), true
		}
	}

	return value.NewIntNil(), false
}
func (*DayOfWeek) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected 0 or 1 args for DayOfWeek() but got %s", n)
	}
	return nil
}

type HourOfWeek struct{ baseFunc }

// hour of week [0-167]
func (m *HourOfWeek) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, vals[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}
func (*HourOfWeek) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected 0 or 1 args for HourOfWeek() but got %s", n)
	}
	return nil
}

type HourOfDay struct{ baseFunc }

// hour of day [0-23]
//  hourofday(field)
//  hourofday()  // Uses message time
func (m *HourOfDay) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if !ctx.Ts().IsZero() {
			return value.NewIntValue(int64(ctx.Ts().Hour())), true
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntValue(0), false
		}
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}
func (*HourOfDay) Validate(n *expr.FuncNode) error {
	if len(n.Args) > 1 {
		return fmt.Errorf("Expected 0 or 1 args for HourOfDay(val) but got %s", n)
	}
	return nil
}

type ToTimestamp struct{ baseFunc }

// totimestamp:   convert to date, then to unix Seconds
//
func (m *ToTimestamp) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	dateStr, ok := value.ValueToString(vals[0])
	if !ok {
		return value.NewIntValue(0), false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		//u.Infof("v=%v   %v  unix=%v", item, item.Rv(), t.Unix())
		return value.NewIntValue(int64(t.Unix())), true
	}

	return value.NewIntValue(0), false
}
func (*ToTimestamp) Validate(n *expr.FuncNode) error {
	if len(n.Args) != 1 {
		return fmt.Errorf("Expected 1 args for ToTimestamp(field) but got %s", n)
	}
	return nil
}

type ToDate struct{ baseFunc }

// todate:   convert to Date
//
//   todate("now-3m")  uses lytics/datemath
//
//   todate(field)  uses araddon/dateparse util to recognize formats
//
//   todate("01/02/2006", field )  uses golang date parse rules
//      first parameter is the layout/format
//
//
func (m *ToDate) Func(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.TimeZeroValue, false
		}

		//u.Infof("v=%v   %v  ", dateStr, items[0].Rv())
		if len(dateStr) > 3 && strings.ToLower(dateStr[:3]) == "now" {
			// Is date math
			if t, err := datemath.Eval(dateStr[3:]); err == nil {
				return value.NewTimeValue(t), true
			}
		} else {
			if t, err := dateparse.ParseAny(dateStr); err == nil {
				return value.NewTimeValue(t), true
			}
		}

	} else if len(vals) == 2 {

		dateStr, ok := value.ValueToString(vals[1])
		if !ok {
			return value.TimeZeroValue, false
		}

		formatStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.TimeZeroValue, false
		}
		//u.Infof("hello  layout=%v  time=%v", formatStr, dateStr)
		if t, err := time.Parse(formatStr, dateStr); err == nil {
			return value.NewTimeValue(t), true
		}
	}

	return value.TimeZeroValue, false
}
func (*ToDate) Validate(n *expr.FuncNode) error {
	if len(n.Args) == 0 || len(n.Args) > 2 {
		return fmt.Errorf(`Expected 1 or 2 args for ToDate(field, ["format"]) but got %s`, n)
	}
	return nil
}

// MapTime()    Create a map[string]time of each key
//
//  maptime(field)    => map[string]time{field_value:message_timestamp}
//  maptime(field, timestamp) => map[string]time{field_value:timestamp}
//
func MapTime(ctx expr.EvalContext, items ...value.Value) (value.MapTimeValue, bool) {
	var k string
	var ts time.Time
	switch len(items) {
	case 0:
		return value.EmptyMapTimeValue, false
	case 1:
		kitem := items[0]
		if kitem.Err() || kitem.Nil() {
			return value.EmptyMapTimeValue, false
		}
		k = strings.ToLower(kitem.ToString())
		ts = ctx.Ts()
	case 2:
		kitem := items[0]
		if kitem.Err() || kitem.Nil() {
			return value.EmptyMapTimeValue, false
		}
		k = strings.ToLower(kitem.ToString())
		dateStr, ok := value.ToString(items[1].Rv())
		if !ok {
			return value.EmptyMapTimeValue, false
		}
		var err error
		ts, err = dateparse.ParseAny(dateStr)
		if err != nil {
			return value.EmptyMapTimeValue, false
		}
	default:
		// incorrect number of arguments
		return value.EmptyMapTimeValue, false
	}
	return value.NewMapTimeValue(map[string]time.Time{k: ts}), true
}

// email a string, parses email
//
//     email("Bob <bob@bob.com>")  =>  bob@bob.com, true
//     email("Bob <bob>")          =>  "", false
//
func EmailFunc(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if len(val) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(val); err == nil {
		return value.NewStringValue(strings.ToLower(em.Address)), true
	}

	return value.EmptyStringValue, false
}

// emailname a string, parses email
//
//     emailname("Bob <bob@bob.com>") =>  Bob
//
func EmailNameFunc(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if len(val) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(val); err == nil {
		return value.NewStringValue(em.Name), true
	}

	return value.EmptyStringValue, false
}

// email a string, parses email
//
//     email("Bob <bob@bob.com>") =>  bob@bob.com
//
func EmailDomainFunc(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if len(val) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(strings.ToLower(val)); err == nil {
		parts := strings.SplitN(strings.ToLower(em.Address), "@", 2)
		if len(parts) == 2 {
			return value.NewStringValue(parts[1]), true
		}
	}

	return value.EmptyStringValue, false
}

// Extract Domains from a Value, or Values (must be urlish), doesn't do much/any validation
//
//     domains("http://www.lytics.io/index.html") =>  []string{"lytics.io"}
//
func DomainsFunc(ctx expr.EvalContext, items ...value.Value) (value.StringsValue, bool) {

	vals := value.NewStringsValue(make([]string, 0))
	for _, item := range items {
		switch itemT := item.(type) {
		case value.StringValue:
			vals.Append(itemT.Val())
		case value.StringsValue:
			for _, sv := range itemT.Val() {
				vals.Append(sv)
			}
		}
	}

	if vals.Len() == 0 {
		return vals, true
	}
	domains := value.NewStringsValue(make([]string, 0))
	for _, val := range vals.Val() {
		urlstr := strings.ToLower(val)
		if len(urlstr) < 8 {
			continue
		}
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
		if urlParsed, err := url.Parse(urlstr); err == nil {
			parts := strings.Split(urlParsed.Host, ".")
			if len(parts) > 2 {
				parts = parts[len(parts)-2:]
			}
			if len(parts) > 0 {
				domains.Append(strings.Join(parts, "."))
			}

		}
	}
	return domains, true
}

// Extract Domain from a Value, or Values (must be urlish), doesn't do much/any validation
//
//     domain("http://www.lytics.io/index.html") =>  "lytics.io"
//
//  if input is a list of strings, only first is evaluated, for plural see domains()
//
func DomainFunc(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {

	urlstr := ""
	switch itemT := item.(type) {
	case value.StringValue:
		urlstr = itemT.Val()
	case value.StringsValue:
		for _, sv := range itemT.Val() {
			urlstr = sv
			break
		}
	}

	urlstr = strings.ToLower(urlstr)
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		parts := strings.Split(urlParsed.Host, ".")
		if len(parts) > 2 {
			parts = parts[len(parts)-2:]
		}
		if len(parts) > 0 {
			return value.NewStringValue(strings.Join(parts, ".")), true
		}

	}
	return value.EmptyStringValue, false
}

// Extract host from a String (must be urlish), doesn't do much/any validation
//
//     host("http://www.lytics.io/index.html") =>  www.lytics.io
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
func HostFunc(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {

	val := ""
	switch itemT := item.(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0]
	case value.SliceValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0].ToString()
	}

	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return value.NewStringValue(urlParsed.Host), true
	}

	return value.EmptyStringValue, false
}

// Extract hosts from a Strings (must be urlish), doesn't do much/any validation
//
//     hosts("http://www.lytics.io", "http://www.activate.lytics.io") => www.lytics.io, www.activate.lytics.io
//
func HostsFunc(ctx expr.EvalContext, items ...value.Value) (value.StringsValue, bool) {
	vals := value.NewStringsValue(make([]string, 0))
	for _, item := range items {
		switch itemT := item.(type) {
		case value.StringValue:
			vals.Append(itemT.Val())
		case value.StringsValue:
			for _, sv := range itemT.Val() {
				vals.Append(sv)
			}
		case value.SliceValue:
			for _, sv := range itemT.Val() {
				vals.Append(sv.ToString())
			}
		}
	}

	if vals.Len() == 0 {
		return vals, true
	}
	hosts := value.NewStringsValue(make([]string, 0))
	for _, val := range vals.Val() {
		urlstr := strings.ToLower(val)
		if len(urlstr) < 8 {
			continue
		}
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
		if urlParsed, err := url.Parse(urlstr); err == nil {
			//u.Infof("url.parse: %#v", urlParsed)
			hosts.Append(urlParsed.Host)
		}
	}
	return hosts, true
}

// url decode a string
//
//     urldecode("http://www.lytics.io/index.html") =>  http://www.lytics.io
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
func UrlDecode(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {

	val := ""
	switch itemT := item.(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0]
	}

	if val == "" {
		return value.EmptyStringValue, false
	}
	val, err := url.QueryUnescape(val)
	if err != nil {
		return value.EmptyStringValue, false
	}

	return value.NewStringValue(val), true
}

// UrlPath Extract url path from a String (must be urlish), doesn't do much/any validation
//
//     path("http://www.lytics.io/blog/index.html") =>  blog/index.html
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
func UrlPath(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {
	val := ""
	switch itemT := item.(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0]
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return value.NewStringValue(urlParsed.Path), true
	}

	return value.EmptyStringValue, false
}

// Qs Extract qs param from a string (must be url valid)
//
//     qs("http://www.lytics.io/?utm_source=google","utm_source")  => "google", true
//
func Qs(ctx expr.EvalContext, urlItem, keyItem value.Value) (value.StringValue, bool) {
	val := ""
	switch itemT := urlItem.(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0]
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	keyVal, ok := value.ToString(keyItem.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if keyVal == "" {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		qsval, ok := urlParsed.Query()[keyVal]
		if !ok {
			return value.EmptyStringValue, false
		}
		if len(qsval) > 0 {
			return value.NewStringValue(qsval[0]), true
		}
	}

	return value.EmptyStringValue, false
}

// UrlMain remove the querystring and scheme from url
//
//     urlmain("http://www.lytics.io/?utm_source=google")  => "www.lytics.io/", true
//
func UrlMain(ctx expr.EvalContext, urlItem value.Value) (value.StringValue, bool) {
	val := ""
	switch itemT := urlItem.(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0]
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if up, err := url.Parse(val); err == nil {
		return value.NewStringValue(fmt.Sprintf("%s%s", up.Host, up.Path)), true
	}

	return value.EmptyStringValue, false
}

// UrlMinusQs removes a specific query parameter and its value from a url
//
//     urlminusqs("http://www.lytics.io/?q1=google&q2=123", "q1") => "http://www.lytics.io/?q2=123", true
//
func UrlMinusQs(ctx expr.EvalContext, urlItem, keyItem value.Value) (value.StringValue, bool) {
	val := ""
	switch itemT := urlItem.(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.Val()[0]
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(val, "http") {
		val = "http://" + val
	}
	keyVal, ok := value.ToString(keyItem.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if keyVal == "" {
		return value.EmptyStringValue, false
	}
	if up, err := url.Parse(val); err == nil {
		qsval := up.Query()
		_, ok := qsval[keyVal]
		if !ok {
			return value.NewStringValue(fmt.Sprintf("%s://%s%s?%s", up.Scheme, up.Host, up.Path, up.RawQuery)), true
		}
		qsval.Del(keyVal)
		up.RawQuery = qsval.Encode()
		if up.RawQuery == "" {
			return value.NewStringValue(fmt.Sprintf("%s://%s%s", up.Scheme, up.Host, up.Path)), true
		}
		return value.NewStringValue(fmt.Sprintf("%s://%s%s?%s", up.Scheme, up.Host, up.Path, up.RawQuery)), true
	}

	return value.EmptyStringValue, false
}

// TimeSeconds time in Seconds, parses a variety of formats looking for seconds
//
//   See github.com/araddon/dateparse for formats supported on date parsing
//
//    seconds("M10:30")      =>  630
//    seconds("M100:30")     =>  6030
//    seconds("00:30")       =>  30
//    seconds("30")          =>  30
//    seconds(30)            =>  30
//    seconds("2015/07/04")  =>  1435968000
//
func TimeSeconds(ctx expr.EvalContext, val value.Value) (value.NumberValue, bool) {

	switch vt := val.(type) {
	case value.StringValue:
		ts := vt.ToString()
		// First, lets try to treat it as a time/date and
		// then extract unix seconds
		if tv, err := dateparse.ParseAny(ts); err == nil {
			return value.NewNumberValue(float64(tv.In(time.UTC).Unix())), true
		}

		// Since that didn't work, lets look for a variety of seconds/minutes type
		// pseudo standards
		//    M10:30
		//     10:30
		//    100:30
		//
		if strings.HasPrefix(ts, "M") {
			ts = ts[1:]
		}
		if strings.Contains(ts, ":") {
			parts := strings.Split(ts, ":")
			switch len(parts) {
			case 1:
				if iv, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
					return value.NewNumberValue(float64(iv)), true
				}
				if fv, err := strconv.ParseFloat(parts[0], 64); err == nil {
					return value.NewNumberValue(fv), true
				}
			case 2:
				min, sec := float64(0), float64(0)
				if iv, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
					min = float64(iv)
				} else if fv, err := strconv.ParseFloat(parts[0], 64); err == nil {
					min = fv
				}
				if iv, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
					sec = float64(iv)
				} else if fv, err := strconv.ParseFloat(parts[1], 64); err == nil {
					sec = fv
				}
				if min > 0 || sec > 0 {
					return value.NewNumberValue(60*min + sec), true
				}
			case 3:

			}
		} else {
			parts := strings.Split(ts, ":")
			if iv, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				return value.NewNumberValue(float64(iv)), true
			}
			if fv, err := strconv.ParseFloat(parts[0], 64); err == nil {
				return value.NewNumberValue(fv), true
			}
		}
	case value.NumberValue:
		return vt, true
	case value.IntValue:
		return vt.NumberValue(), true
	}

	return value.NewNumberValue(0), false
}

// TimeExtractFunc extraces certain parts from a time, similar to Python's StrfTime
// See http://strftime.org/ for Strftime directives.
//
//	extract("2015/07/04", "%B") 	=> "July"
//	extract("2015/07/04", "%B:%d") 	=> "July:4"
// 	extract("1257894000", "%p")		=> "PM"

func TimeExtractFunc(ctx expr.EvalContext, items ...value.Value) (value.StringValue, bool) {
	switch len(items) {
	case 0:
		// if we have no "items", return time associated with ctx
		// This is an alias of now()
		t := ctx.Ts()
		if !t.IsZero() {
			return value.NewStringValue(t.String()), true
		}
		return value.EmptyStringValue, false

	case 1:
		// if only 1 item, convert item to time
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.EmptyStringValue, false
		}
		t, err := dateparse.ParseAny(dateStr)
		if err != nil {
			return value.EmptyStringValue, false
		}
		return value.NewStringValue(t.String()), true

	case 2:
		// if we have 2 items, the first is the time string
		// and the second is the format string.
		// Use leekchan/timeutil package
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.EmptyStringValue, false
		}

		formatStr, ok := value.ToString(items[1].Rv())
		if !ok {
			return value.EmptyStringValue, false
		}

		t, err := dateparse.ParseAny(dateStr)
		if err != nil {
			return value.EmptyStringValue, false
		}

		formatted := timeutil.Strftime(&t, formatStr)
		return value.NewStringValue(formatted), true

	default:
		return value.EmptyStringValue, false
	}
}

// HashMd5Func Hash a value to MD5 string
//
//     hash.md5("/blog/index.html")  =>  abc345xyz
//
func HashMd5Func(ctx expr.EvalContext, arg value.Value) (value.StringValue, bool) {
	if arg.Err() || arg.Nil() {
		return value.EmptyStringValue, true
	}
	hasher := md5.New()
	hasher.Write([]byte(arg.ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// HashSha1Func Hash a value to SHA256 string
//
//     hash.sha1("/blog/index.html")  =>  abc345xyz
//
func HashSha1Func(ctx expr.EvalContext, arg value.Value) (value.StringValue, bool) {
	if arg.Err() || arg.Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha1.New()
	hasher.Write([]byte(arg.ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// HashSha256Func Hash a value to SHA256 string
//
//     hash.sha256("/blog/index.html")  =>  abc345xyz
//
func HashSha256Func(ctx expr.EvalContext, arg value.Value) (value.StringValue, bool) {
	if arg.Err() || arg.Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha256.New()
	hasher.Write([]byte(arg.ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// HashSha512Func Hash a value to SHA512 string
//
//     hash.sha512("/blog/index.html")  =>  abc345xyz
//
func HashSha512Func(ctx expr.EvalContext, arg value.Value) (value.StringValue, bool) {
	if arg.Err() || arg.Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha512.New()
	hasher.Write([]byte(arg.ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
