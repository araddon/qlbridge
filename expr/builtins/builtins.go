package builtins

import (
	"fmt"
	"math"
	"net/mail"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/leekchan/timeutil"
	"github.com/lytics/datemath"
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
		expr.AggFuncAdd("count", CountFunc)
		expr.AggFuncAdd("avg", AvgFunc)
		expr.AggFuncAdd("sum", SumFunc)

		// logical
		expr.FuncAdd("gt", Gt)
		expr.FuncAdd("ge", Ge)
		expr.FuncAdd("ne", Ne)
		expr.FuncAdd("le", LeFunc)
		expr.FuncAdd("lt", LtFunc)
		expr.FuncAdd("not", NotFunc)
		expr.FuncAdd("eq", Eq)
		expr.FuncAdd("exists", Exists)
		expr.FuncAdd("map", MapFunc)

		// Date/Time functions
		expr.FuncAdd("now", Now)
		expr.FuncAdd("yy", Yy)
		expr.FuncAdd("yymm", YyMm)
		expr.FuncAdd("mm", Mm)
		expr.FuncAdd("monthofyear", Mm)
		expr.FuncAdd("dayofweek", DayOfWeek)
		expr.FuncAdd("hourofday", HourOfDay)
		expr.FuncAdd("hourofweek", HourOfWeek)
		expr.FuncAdd("totimestamp", ToTimestamp)
		expr.FuncAdd("todate", ToDate)
		expr.FuncAdd("seconds", TimeSeconds)

		// String Functions
		expr.FuncAdd("contains", ContainsFunc)
		expr.FuncAdd("tolower", Lower)
		expr.FuncAdd("toint", ToInt)
		expr.FuncAdd("tonumber", ToNumber)
		expr.FuncAdd("uuid", UuidGenerate)
		expr.FuncAdd("split", SplitFunc)
		expr.FuncAdd("replace", Replace)
		expr.FuncAdd("join", JoinFunc)

		// array, string
		expr.FuncAdd("len", LengthFunc)

		// selection
		expr.FuncAdd("oneof", OneOfFunc)
		expr.FuncAdd("match", Match)
		expr.FuncAdd("any", AnyFunc)
		expr.FuncAdd("all", AllFunc)

		// special items
		expr.FuncAdd("email", EmailFunc)
		expr.FuncAdd("emaildomain", EmailDomainFunc)
		expr.FuncAdd("emailname", EmailNameFunc)
		expr.FuncAdd("host", HostFunc)
		expr.FuncAdd("path", UrlPath)
		expr.FuncAdd("qs", Qs)
		expr.FuncAdd("urlmain", UrlMain)
		expr.FuncAdd("urlminusqs", UrlMinusQs)
		expr.FuncAdd("urldecode", UrlDecode)
		expr.FuncAdd("extract", TimeExtractFunc)

		// MySQL Builtins
		expr.FuncAdd("cast", CastFunc)
		expr.FuncAdd("char_length", LengthFunc)
	})
}

func emptyFunc(ctx expr.EvalContext, _ value.Value) (value.Value, bool) { return nil, true }

// avg:   average doesn't avg bc it doesn't have a storage, but does return number
//
func AvgFunc(ctx expr.EvalContext, val value.Value) (value.NumberValue, bool) {
	switch node := val.(type) {
	case value.StringValue:
		if fv, err := strconv.ParseFloat(node.Val(), 64); err == nil {
			return value.NewNumberValue(fv), true
		}
	case value.NumberValue:
		return node, true
	case value.IntValue:
		return value.NewNumberValue(node.Float()), true
	case nil, value.NilValue:
		return value.NewNumberValue(0), false
	}
	return value.NewNumberValue(0), false
}

// Sum
func SumFunc(ctx expr.EvalContext, vals ...value.Value) (value.NumberValue, bool) {

	//u.Debugf("Sum: %v", vals)
	sumval := float64(0)
	for _, val := range vals {
		if val == nil || val.Nil() || val.Err() {
			// we don't need to evaluate if nil or error
		} else {
			switch valValue := val.(type) {
			case value.StringsValue:
				//u.Debugf("Nice, we have strings: %v", valValue)
				for _, sv := range valValue.Value().([]string) {
					if fv, ok := value.ToFloat64(reflect.ValueOf(sv)); ok && !math.IsNaN(fv) {
						sumval += fv
					}
				}
			default:
				//u.Debugf("Sum:  %T tofloat=%v  %v", value.ToFloat64(val.Rv()), value.ToFloat64(val.Rv()), val.Rv())
				if fv, ok := value.ToFloat64(val.Rv()); ok && !math.IsNaN(fv) {
					sumval += fv
				}
			}
		}
	}
	if sumval == float64(0) {
		return value.NumberNaNValue, false
	}
	//u.Debugf("Sum() about to return?  %v  Nan?%v", sumval, sumval == math.NaN())
	return value.NewNumberValue(sumval), true
}

// len:   length of array types
//
//      len([1,2,3])     =>  3, true
//      len(not_a_field)   =>  -- NilInt, false
//
func LengthFunc(ctx expr.EvalContext, val value.Value) (value.IntValue, bool) {
	switch node := val.(type) {
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

// Count:   This should be renamed Increment
//      and in general is a horrible, horrible function that needs to be replaced
//      with occurences of value, ignores the value and ensures it is non null
//
//      count(anyvalue)     =>  1, true
//      count(not_number)   =>  -- 0, false
//
func CountFunc(ctx expr.EvalContext, val value.Value) (value.IntValue, bool) {
	if val.Err() || val.Nil() {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(1), true
}

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

//  Equal function?  returns true if items are equal
//
//   given   {"name":"wil","event":"stuff", "int4": 4}
//
//      eq(int4,5)  => false
//
func Eq(ctx expr.EvalContext, itemA, itemB value.Value) (value.BoolValue, bool) {
	eq, err := value.Equal(itemA, itemB)
	if err == nil {
		return value.NewBoolValue(eq), true
	}
	return value.BoolValueFalse, false
}

//  Not Equal function?  returns true if items are equal
//
//   given   {"5s":"5","item4":4,"item4s":"4"}
//
//      ne(`5s`,5) => true, true
//      ne(`not_a_field`,5) => false, true
//      ne(`item4s`,5) => false, true
//      ne(`item4`,5) => false, true
//
func Ne(ctx expr.EvalContext, itemA, itemB value.Value) (value.BoolValue, bool) {
	eq, err := value.Equal(itemA, itemB)
	if err == nil {
		return value.NewBoolValue(!eq), true
	}
	return value.BoolValueFalse, false
}

//  Not:   urnary negation function
//
//      not(eq(5,5)) => false, true
//      not(eq("false")) => false, true
//
func NotFunc(ctx expr.EvalContext, item value.Value) (value.BoolValue, bool) {
	boolVal, ok := value.ToBool(item.Rv())
	if ok {
		return value.NewBoolValue(!boolVal), true
	}
	return value.BoolValueFalse, false
}

// > GreaterThan
//  Must be able to convert items to Floats or else not ok
//
func Gt(ctx expr.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left, _ := value.ToFloat64(lv.Rv())
	right, _ := value.ToFloat64(rv.Rv())
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left > right), true
}

// >= GreaterThan or Equal
//  Must be able to convert items to Floats or else not ok
//
func Ge(ctx expr.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left, _ := value.ToFloat64(lv.Rv())
	right, _ := value.ToFloat64(rv.Rv())
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left >= right), true
}

// <= Less Than or Equal
//  Must be able to convert items to Floats or else not ok
//
func LeFunc(ctx expr.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left, _ := value.ToFloat64(lv.Rv())
	right, _ := value.ToFloat64(rv.Rv())
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left <= right), true
}

// < Less Than
//  Must be able to convert items to Floats or else not ok
//
func LtFunc(ctx expr.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left, _ := value.ToFloat64(lv.Rv())
	right, _ := value.ToFloat64(rv.Rv())
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left < right), true
}

// Exists:  Answers True/False if the field exists and is non null
//
//     exists(real_field) => true
//     exists("value") => true
//     exists("") => false
//     exists(empty_field) => false
//     exists(2) => true
//     exists(todate(date_field)) => true
//
func Exists(ctx expr.EvalContext, item interface{}) (value.BoolValue, bool) {

	switch node := item.(type) {
	case expr.IdentityNode:
		_, ok := ctx.Get(node.Text)
		if ok {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, true
	case expr.StringNode:
		_, ok := ctx.Get(node.Text)
		if ok {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, true
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

// Map()    Create a map from two values.   If the right side value is nil
//    then does not evaluate
//
//  Map(left, right)    => map[string]value{left:right}
//
func MapFunc(ctx expr.EvalContext, lv, rv value.Value) (value.MapValue, bool) {
	if lv.Err() || rv.Err() {
		return value.EmptyMapValue, false
	}
	if lv.Nil() || rv.Nil() {
		return value.EmptyMapValue, false
	}
	return value.NewMapValue(map[string]interface{}{lv.ToString(): rv.Value()}), true
}

// uuid generates a uuid
//
func UuidGenerate(ctx expr.EvalContext) (value.StringValue, bool) {
	return value.NewStringValue(uuid.New()), true
}

// String contains
//   Will first convert to string, so may get unexpected results
//
func ContainsFunc(ctx expr.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left, leftOk := value.ToString(lv.Rv())
	right, rightOk := value.ToString(rv.Rv())
	if !leftOk {
		return value.BoolValueFalse, true
	}
	if !rightOk {
		return value.BoolValueFalse, true
	}
	//u.Infof("Contains(%v, %v)", left, right)
	if left == "" || right == "" {
		return value.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}

// match:  Match a simple pattern match and return matched value
//
//  given input:
//      {"score_value":24,"event_click":true}
//
//     match("score_") => {"value":24}
//     match("amount_") => false
//     match("event_") => {"click":true}
//
func Match(ctx expr.EvalContext, items ...value.Value) (value.MapValue, bool) {
	//u.Debugf("Match():  %T  %v", item, item)
	mv := make(map[string]interface{})
	for _, item := range items {
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
		//u.Infof("found new: %v", mv)
		return value.NewMapValue(mv), true
	}

	//u.Warnf("could not find key: %T %v", item, item)
	return value.EmptyMapValue, false
}

func matchFind(ctx expr.EvalContext, matchKey string) (value.Value, string, bool) {

	return nil, "", false
}

// String lower function
//   must be able to convert to string
//
func Lower(ctx expr.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.ToLower(val)), true
}

// choose OneOf these fields, first non-null
func OneOfFunc(ctx expr.EvalContext, vals ...value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			// continue, ignore
		} else if !value.IsNilIsh(v.Rv()) {
			return v, true
		}
	}
	return value.NilValueVal, true
}

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
func AnyFunc(ctx expr.EvalContext, vals ...value.Value) (value.BoolValue, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			// continue
		} else if !value.IsNilIsh(v.Rv()) {
			return value.NewBoolValue(true), true
		}
	}
	return value.NewBoolValue(false), true
}

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
func AllFunc(ctx expr.EvalContext, vals ...value.Value) (value.BoolValue, bool) {
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

// Split a string, accepts an optional with parameter
//
//     split(item, ",")
//
func SplitFunc(ctx expr.EvalContext, input value.Value, splitByV value.StringValue) (value.StringsValue, bool) {

	sv, ok := value.ToString(input.Rv())
	splitBy, splitByOk := value.ToString(splitByV.Rv())
	if !ok || !splitByOk {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if sv == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if splitBy == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	vals := strings.Split(sv, splitBy)
	return value.NewStringsValue(vals), true
}

// Replace a string(s), accepts any number of parameters to replace
//    replaces with ""
//
//     replace("/blog/index.html", "/blog","")  =>  /index.html
//     replace("/blog/index.html", "/blog")  =>  /index.html
//     replace("/blog/index.html", "/blog/archive/","/blog")  =>  /blog/index.html
//     replace(item, "M")
//
func Replace(ctx expr.EvalContext, vals ...value.Value) (value.StringValue, bool) {
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

// Join items together (string concatenation)
//
//   join("apples","oranges",",")   => "apples,oranges"
//   join(["apples","oranges"],",") => "apples,oranges"
//   join("apples","oranges","")    => "applesoranges"
//
func JoinFunc(ctx expr.EvalContext, items ...value.Value) (value.StringValue, bool) {
	if len(items) <= 1 {
		return value.EmptyStringValue, false
	}
	sep, ok := value.ToString(items[len(items)-1].Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	args := make([]string, 0)
	for i := 0; i < len(items)-1; i++ {
		switch valTyped := items[i].(type) {
		case value.SliceValue:
			vals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				vals[i] = sv.ToString()
			}
			args = append(args, vals...)
		case value.StringsValue:
			vals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				vals[i] = sv
			}
			args = append(args, vals...)
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

// Cast :   type coercion
//
//   cast(identity AS <type>) => 5.0
//   cast(reg_date AS string) => "2014/01/12"
//
//  Types:  [char, string, int, float]
//
func CastFunc(ctx expr.EvalContext, items ...value.Value) (value.Value, bool) {

	// identity AS identity
	//  0        1    2
	if len(items) != 3 {
		return nil, false
	}
	if items[0] == nil || items[0].Type() == value.NilType {
		return nil, false
	}
	if items[2] == nil || items[2].Type() == value.NilType {
		return nil, false
	}
	vt := value.ValueFromString(items[2].ToString())

	// http://www.cheatography.com/davechild/cheat-sheets/mysql/
	if vt == value.UnknownType {
		switch strings.ToLower(items[2].ToString()) {
		case "char":
			vt = value.ByteSliceType
		default:
			return nil, false
		}
	}
	val, err := value.Cast(vt, items[0])
	//u.Debugf("cast  %#v  err=%v", val, err)
	if err != nil {
		return nil, false
	}
	return val, true
}

// Convert to Integer:   Best attempt at converting to integer
//
//   toint("5")          => 5, true
//   toint("5.75")       => 5, true
//   toint("5,555")      => 5555, true
//   toint("$5")         => 5, true
//   toint("5,555.00")   => 5555, true
//
func ToInt(ctx expr.EvalContext, item value.Value) (value.IntValue, bool) {

	switch itemT := item.(type) {
	case value.TimeValue:
		iv := itemT.Val().UnixNano() / 1e6 // Milliseconds
		return value.NewIntValue(iv), true
	}
	iv, ok := value.ToInt64(reflect.ValueOf(item.Value()))
	if !ok {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(iv), true
}

// Convert to Number:   Best attempt at converting to integer
//
//   tonumber("5") => 5.0
//   tonumber("5.75") => 5.75
//   tonumber("5,555") => 5555
//   tonumber("$5") => 5.00
//   tonumber("5,555.00") => 5555
//
func ToNumber(ctx expr.EvalContext, item value.Value) (value.NumberValue, bool) {
	fv, ok := value.ToFloat64(reflect.ValueOf(item.Value()))
	if !ok {
		return value.NewNumberNil(), false
	}
	return value.NewNumberValue(fv), true
}

// Get current time of Message (message time stamp) or else choose current
//   server time if none is available in message context
//
func Now(ctx expr.EvalContext, items ...value.Value) (value.TimeValue, bool) {
	if ctx != nil && !ctx.Ts().IsZero() {
		t := ctx.Ts()
		return value.NewTimeValue(t), true
	}

	return value.NewTimeValue(time.Now().In(time.UTC)), true
}

// Get year in integer from field, must be able to convert to date
//
//    yy()                 =>  15, true    // assuming it is 2015
//    yy("2014-03-01")     =>  14, true
//
func Yy(ctx expr.EvalContext, items ...value.Value) (value.IntValue, bool) {

	yy := 0
	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			yy = ctx.Ts().Year()
		} else {
			// Do we want to use Now()?
		}
	} else if len(items) == 1 {
		//u.Debugf("has 1 items? %#v", items[0].Rv())
		dateStr, ok := value.ToString(items[0].Rv())
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
	//u.Debugf("yy = %v", yy)
	return value.NewIntValue(int64(yy)), true
}

// Get month as integer from date
//   @optional timestamp (if not, gets from context reader)
//
//  mm()                =>  01, true  /// assuming message ts = jan 1
//  mm("2014-03-17")    =>  03, true
//
func Mm(ctx expr.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Month())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Month())), true
		}
	}

	return value.NewIntValue(0), false
}

// Get yymm in 4 digits from argument if supplied, else uses message context ts
//
func YyMm(ctx expr.EvalContext, items ...value.Value) (value.StringValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
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

// day of week [0-6]
func DayOfWeek(ctx expr.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Weekday())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
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

// hour of week [0-167]
func HourOfWeek(ctx expr.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}

// hour of day [0-23]
func HourOfDay(ctx expr.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			return value.NewIntValue(int64(ctx.Ts().Hour())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}

// totimestamp:   convert to date, then to unix Seconds
//
func ToTimestamp(ctx expr.EvalContext, item value.Value) (value.IntValue, bool) {

	dateStr, ok := value.ToString(item.Rv())
	if !ok {
		return value.NewIntValue(0), false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		//u.Infof("v=%v   %v  unix=%v", item, item.Rv(), t.Unix())
		return value.NewIntValue(int64(t.Unix())), true
	}

	return value.NewIntValue(0), false
}

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
func ToDate(ctx expr.EvalContext, items ...value.Value) (value.TimeValue, bool) {

	if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
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

	} else if len(items) == 2 {
		dateStr, ok := value.ToString(items[1].Rv())
		if !ok {
			return value.TimeZeroValue, false
		}
		formatStr, ok := value.ToString(items[0].Rv())
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

// Extract host from a String (must be urlish), doesn't do much/any validation
//
//     host("http://www.lytics.io/index.html") =>  http://www.lytics.io
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

// Extract url path from a String (must be urlish), doesn't do much/any validation
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

// Extract qs param from a string (must be url valid)
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

// urlmain remove the querystring and scheme from url
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

// urlminusqs removes a specific query parameter and its value from a url
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

// time Seconds, parses a variety of formats looking for seconds
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
