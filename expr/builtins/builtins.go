// Builtin functions are a library of functions natively available in
// qlbridge expression evaluation although adding your own is easy.
package builtins

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/leekchan/timeutil"
	"github.com/lytics/datemath"
	"github.com/mb0/glob"
	"github.com/mssola/user_agent"
	"github.com/pborman/uuid"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var _ = u.EMPTY
var loadOnce sync.Once

const yymmTimeLayout = "0601"

func LoadAllBuiltins() {
	loadOnce.Do(func() {

		// math
		expr.FuncAdd("sqrt", &Sqrt{})
		expr.FuncAdd("pow", &Pow{})

		// aggregate ops
		expr.FuncAdd("count", &Count{})
		expr.FuncAdd("avg", &Avg{})
		expr.FuncAdd("sum", &Sum{})

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
		expr.FuncAdd("todatein", &ToDateIn{})
		expr.FuncAdd("seconds", &TimeSeconds{})
		expr.FuncAdd("maptime", &MapTime{})
		expr.FuncAdd("extract", &StrFromTime{})
		expr.FuncAdd("strftime", &StrFromTime{})
		expr.FuncAdd("unixtrunc", &TimeTrunc{})

		// String Functions
		expr.FuncAdd("contains", &Contains{})
		expr.FuncAdd("tolower", &Lower{})
		expr.FuncAdd("tostring", &ToString{})
		expr.FuncAdd("tobool", &ToBool{})
		expr.FuncAdd("toint", &ToInt{})
		expr.FuncAdd("tonumber", &ToNumber{})
		expr.FuncAdd("uuid", &UuidGenerate{})
		expr.FuncAdd("split", &Split{})
		expr.FuncAdd("strip", &Strip{})
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
		expr.FuncAdd("filtermatch", &FilterIn{})

		// special items
		expr.FuncAdd("email", &Email{})
		expr.FuncAdd("emaildomain", &EmailDomain{})
		expr.FuncAdd("emailname", &EmailName{})
		expr.FuncAdd("domain", &Domain{})
		expr.FuncAdd("domains", &Domains{})
		expr.FuncAdd("host", &Host{})
		expr.FuncAdd("hosts", &Hosts{})
		expr.FuncAdd("path", &UrlPath{})
		expr.FuncAdd("qs", &Qs{})
		expr.FuncAdd("urlmain", &UrlMain{})
		expr.FuncAdd("urlminusqs", &UrlMinusQs{})
		expr.FuncAdd("urldecode", &UrlDecode{})
		expr.FuncAdd("url.matchqs", &UrlWithQuery{})
		expr.FuncAdd("useragent.map", &UserAgentMap{})
		expr.FuncAdd("useragent", &UserAgent{})

		// Hashing functions
		expr.FuncAdd("hash", &HashSip{})
		expr.FuncAdd("hash.sip", &HashSip{})
		expr.FuncAdd("hash.md5", &HashMd5{})
		expr.FuncAdd("hash.sha1", &HashSha1{})
		expr.FuncAdd("hash.sha256", &HashSha256{})
		expr.FuncAdd("hash.sha512", &HashSha512{})

		expr.FuncAdd("encoding.b64encode", &EncodeB64Encode{})
		expr.FuncAdd("encoding.b64decode", &EncodeB64Decode{})

		// MySQL Builtins
		expr.FuncAdd("cast", &Cast{})
		expr.FuncAdd("char_length", &Length{})
	})
}

func emptyFunc(ctx expr.EvalContext, _ value.Value) (value.Value, bool) { return nil, true }

type Avg struct{}

// avg:   average doesn't aggregate across calls, that would be
//        responsibility of write context, but does return number
//
//   avg(1,2,3) => 2.0, true
//   avg("hello") => math.NaN, false
//
func (m *Avg) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
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

func (m *Avg) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Avg(arg, arg, ...) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Avg) IsAgg() bool           { return true }
func (m *Avg) Type() value.ValueType { return value.NumberType }

type Sum struct{}

// Sum  function to add values
//
//   sum(1, 2, 3) => 6
//   sum(1, "horse", 3) => nan, false
//
func (m *Sum) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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

func (m *Sum) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Sum(arg, arg, ...) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Sum) IsAgg() bool           { return true }
func (m *Sum) Type() value.ValueType { return value.NumberType }

type Count struct{}

// Count:   This should be renamed Increment
// and in general is a horrible, horrible function that needs to be replaced
// with occurrences of value, ignores the value and ensures it is non null
//
//      count(anyvalue)     =>  1, true
//      count(not_number)   =>  -- 0, false
//
func (m *Count) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if len(vals) == 0 {
		return value.NewIntValue(1), true
	}
	if vals[0] == nil || vals[0].Err() || vals[0].Nil() {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(1), true
}
func (m *Count) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected max 1 arg for Count(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Count) IsAgg() bool           { return true }
func (m *Count) Type() value.ValueType { return value.IntType }

type Sqrt struct{}

// Sqrt
//
//      sqrt(4)            =>  2, true
//      sqrt(9)            =>  3, true
//      sqrt(not_number)   =>  0, false
//
func (m *Sqrt) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
func (m *Sqrt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected exactly 1 args for Sqrt(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Sqrt) Type() value.ValueType { return value.NumberType }

type Pow struct{}

// Pow:   exponents, raise x to the power of y
//
//      pow(5,2)            =>  25, true
//      pow(3,2)            =>  9, true
//      pow(not_number,2)   =>  NilNumber, false
//
func (m *Pow) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
func (m *Pow) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for Pow(numer, power) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Pow) Type() value.ValueType { return value.NumberType }

type Not struct{}

//  Not:   urnary negation function
//
//      not(eq(5,5)) => false, true
//      not(eq("false")) => false, true
//
func (m *Not) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	boolVal, ok := value.ValueToBool(args[0])
	if ok {
		return value.NewBoolValue(!boolVal), true
	}
	return value.BoolValueFalse, false
}
func (m *Not) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected at least 1 args for NOT(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Not) Type() value.ValueType { return value.BoolType }

type Eq struct{}

//  Equal function?  returns true if items are equal
//
//   given   {"name":"wil","event":"stuff", "int4": 4}
//
//      eq(int4,5)  => false
//
func (m *Eq) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	eq, err := value.Equal(vals[0], vals[1])
	if err == nil {
		return value.NewBoolValue(eq), true
	}
	return value.BoolValueFalse, false
}
func (m *Eq) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for EQ(lh, rh) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Eq) Type() value.ValueType { return value.BoolType }

type Ne struct{}

//  Not Equal function?  returns true if items are equal
//
//   given   {"5s":"5","item4":4,"item4s":"4"}
//
//      ne(`5s`,5) => true, true
//      ne(`not_a_field`,5) => false, true
//      ne(`item4s`,5) => false, true
//      ne(`item4`,5) => false, true
//
func (m *Ne) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	eq, err := value.Equal(vals[0], vals[1])
	if err == nil {
		return value.NewBoolValue(!eq), true
	}
	return value.BoolValueFalse, false
}
func (m *Ne) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for NE(lh, rh) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Ne) Type() value.ValueType { return value.BoolType }

type Gt struct{}

// > GreaterThan
//  Must be able to convert items to Floats or else not ok
//
func (m *Gt) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left > right), true
}
func (m *Gt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for Gt(lh, rh) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Gt) Type() value.ValueType { return value.BoolType }

type Ge struct{}

// >= GreaterThan or Equal
//  Must be able to convert items to Floats or else not ok
//
func (m *Ge) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left >= right), true
}
func (m *Ge) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for GE(lh, rh) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Ge) Type() value.ValueType { return value.BoolType }

type Le struct{}

// <= Less Than or Equal
//  Must be able to convert items to Floats or else not ok
//
func (m *Le) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(left <= right), true
}
func (m *Le) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 args for Le(lh, rh) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Le) Type() value.ValueType { return value.BoolType }

type Lt struct{}

// Lt   < Less Than
//  Must be able to convert items to Floats or else not ok
//
func (m *Lt) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	left, _ := value.ValueToFloat64(vals[0])
	right, _ := value.ValueToFloat64(vals[1])
	if math.IsNaN(left) || math.IsNaN(right) {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left < right), true
}
func (m *Lt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected exactly 2 arg for Lt(lh, rh) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Lt) Type() value.ValueType { return value.BoolType }

type Exists struct{}

// Exists:  Answers True/False if the field exists and is non null
//
//     exists(real_field) => true
//     exists("value") => true
//     exists("") => false
//     exists(empty_field) => false
//     exists(2) => true
//     exists(todate(date_field)) => true
//
func (m *Exists) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
func (m *Exists) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected exactly 1 arg for Exists(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Exists) Type() value.ValueType { return value.BoolType }

type Length struct{}

// len:   length of array types
//
//      len([1,2,3])     =>  3, true
//      len(not_a_field)   =>  -- NilInt, false
//
func (m *Length) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
func (m *Length) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Length(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Length) Type() value.ValueType { return value.IntType }

type ArrayIndex struct{}

// array.index:   choose the nth element of an array
//
//   given: "items" = [1,2,3]
//
//      array.index(items, 1)     =>  1, true
//      array.index(items, 5)     =>  nil, false
//      array.index(items, -1)    =>  3, true
//
func (m *ArrayIndex) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
func (m *ArrayIndex) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 arg for ArrayIndex(array, index) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ArrayIndex) Type() value.ValueType { return value.UnknownType }

type ArraySlice struct{}

// array.slice:   slice element m -> n of a slice
//
//   given: "items" = [1,2,3,4,5]
//
//      array.slice(items, 1, 3)     =>  [2,3], true
//      array.slice(items, 2)        =>  [3,4,5], true
//      array.slice(items, -2)       =>  [4,5], true
//
func (m *ArraySlice) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
		} else {
			// array.slice(item, start, end)
			return value.NewSliceValues(svals[idx:idx2]), true
		}
	}
	return nil, false
}
func (m *ArraySlice) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 || len(n.Args) > 3 {
		return nil, fmt.Errorf("Expected 2 OR 3 args for ArraySlice(array, start, [end]) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ArraySlice) Type() value.ValueType { return value.UnknownType }

type MapFunc struct{}

// Map()    Create a map from two values.   If the right side value is nil
//    then does not evaluate
//
//  Map(left, right)    => map[string]value{left:right}
//
func (m *MapFunc) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Nil() || args[0].Err() {
		return value.EmptyMapValue, false
	}
	if args[0] == nil || args[1].Nil() || args[1].Nil() {
		return value.EmptyMapValue, false
	}
	// What should the map function be if lh is slice/map?
	return value.NewMapValue(map[string]interface{}{args[0].ToString(): args[1].Value()}), true
}
func (m *MapFunc) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 arg for MapFunc(key, value) but got %s", n)
	}
	return m.Eval, nil
}
func (m *MapFunc) Type() value.ValueType { return value.MapValueType }

type Match struct{}

// match:  Match a simple pattern match and return matched value
//
//  given input:
//      {"score_value":24,"event_click":true}
//
//     match("score_") => {"value":24}
//     match("amount_") => false
//     match("event_") => {"click":true}
//
func (m *Match) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]interface{})
	for _, item := range args {
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
func (m *Match) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more arg for Match(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Match) Type() value.ValueType { return value.MapValueType }

type MapKeys struct{}

// MapKeys:  Take a map and extract array of keys
//
//  given input:
//      {"tag.1":"news","tag.2":"sports"}
//
//     mapkeys(match("tag.")) => []string{"news","sports"}
//
func (m *MapKeys) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]bool)
	for _, item := range args {
		switch node := item.(type) {
		case value.Map:
			for key := range node.MapValue().Val() {
				mv[key] = true
			}
		default:
			u.Debugf("unsuported key type: %T %v", item, item)
		}
	}
	keys := make([]string, 0, len(mv))
	for k := range mv {
		keys = append(keys, k)
	}

	return value.NewStringsValue(keys), true
}
func (m *MapKeys) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 arg for MapKeys(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *MapKeys) Type() value.ValueType { return value.StringsType }

type MapValues struct{}

// MapValues:  Take a map and extract array of values
//
//  given input:
//      {"tag.1":"news","tag.2":"sports"}
//
//     mapvalue(match("tag.")) => []string{"1","2"}
//
func (m *MapValues) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]bool)
	for _, item := range args {
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
	for k := range mv {
		result = append(result, k)
	}

	return value.NewStringsValue(result), true
}
func (m *MapValues) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for MapValues(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *MapValues) Type() value.ValueType { return value.StringsType }

type MapInvert struct{}

// MapInvert:  Take a map and invert key/values
//
//  given input:
//     tags = {"1":"news","2":"sports"}
//
//     mapinvert(tags) => map[string]string{"news":"1","sports":"2"}
//
func (m *MapInvert) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	mv := make(map[string]string)
	for _, val := range args {
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
func (m *MapInvert) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 arg for MapInvert(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *MapInvert) Type() value.ValueType { return value.MapValueType }

type UuidGenerate struct{}

// uuid generates a new uuid
//
func (m *UuidGenerate) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	return value.NewStringValue(uuid.New()), true
}
func (m *UuidGenerate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 arg for uuid() but got %s", n)
	}
	return m.Eval, nil
}
func (m *UuidGenerate) Type() value.ValueType { return value.StringType }

type Contains struct{}

// Contains does first arg string contain 2nd arg?
//
//   contain("alabama","red") => false
//
func (m *Contains) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	left, leftOk := value.ValueToString(args[0])
	right, rightOk := value.ValueToString(args[1])
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
func (m *Contains) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for Contains(str_value, contains_this) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Contains) Type() value.ValueType { return value.BoolType }

type Lower struct{}

// Lower string function
//   must be able to convert to string
//
func (m *Lower) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.ToLower(val)), true
}
func (m *Lower) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Lower(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Lower) Type() value.ValueType { return value.StringType }

type ToString struct{}

// ToString cast as string
//   must be able to convert to string
//
func (m *ToString) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() {
		return value.EmptyStringValue, false
	}
	if args[0].Nil() {
		return value.EmptyStringValue, true
	}
	return value.NewStringValue(args[0].ToString()), true
}
func (m *ToString) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToString(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ToString) Type() value.ValueType { return value.StringType }

type OneOf struct{}

// choose OneOf these fields, first non-null
func (m *OneOf) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	for _, v := range args {
		if v.Err() || v.Nil() {
			// continue, ignore
		} else {
			return v, true
		}
	}
	return value.NilValueVal, true
}
func (m *OneOf) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("Expected 2 or more args for OneOf(arg, arg, ...) but got %s", n)
	}
	return m.Eval, nil
}
func (m *OneOf) Type() value.ValueType { return value.UnknownType }

type Any struct{}

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
func (m *Any) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v == nil || v.Err() || v.Nil() {
			// continue
		} else {
			return value.NewBoolValue(true), true
		}
	}
	return value.NewBoolValue(false), true
}
func (m *Any) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Any(arg, arg, ...) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Any) Type() value.ValueType { return value.BoolType }

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

type Filter struct{}

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
func (m *Filter) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *Filter) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for Filter("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *Filter) Type() value.ValueType { return value.UnknownType }

type FilterIn struct{}

// FilterIn  Filter IN Values that match specified list of match filter criteria
//
// Operates on MapValue (map[string]interface{}), StringsValue ([]string), or string
// takes N Filter Criteria
//
// Wildcard Matching:      "abcd*" // matches  "abcd_x", "abcdstuff"
//
// Filter a map of values by key to only keep certain keys
//
//    filterin(match("topic_"),key_to_filter, key2_to_filter)  => {"goodkey": 22}, true
//
// Filter in VALUES (not keys) from a list of []string{} for a specific value
//
//    filterin(split("apples,oranges",","),"ora*")  => ["oranges"], true
//
// Filter in values for single strings
//
//    filterin("apples","app*")      => []string{"apple"}, true
//
func (m *FilterIn) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return nil, false
	}
	val := vals[0]
	filters := FiltersFromArgs(vals[1:])

	//u.Debugf("FilterIn():  %T:%v   filters:%v", val, val, filters)
	switch val := val.(type) {
	case value.MapValue:

		mv := make(map[string]interface{})

		for rowKey, v := range val.Val() {
			filteredIn := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, rowKey)
					if match {
						filteredIn = true
						break
					}
				} else {
					if strings.HasPrefix(rowKey, filter) && v != nil {
						filteredIn = true
						break
					}
				}
			}
			if filteredIn {
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
		if !anyMatches {
			return value.NilValueVal, true
		}
		return val, true
	case value.StringsValue:
		lv := make([]string, 0, val.Len())

		for _, sv := range val.Val() {
			filteredIn := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, sv)
					if match {
						filteredIn = true
						break
					}
				} else {
					if strings.HasPrefix(sv, filter) && sv != "" {
						filteredIn = true
						break
					}
				}
			}
			if filteredIn {
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
func (m *FilterIn) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for FilterIn("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *FilterIn) Type() value.ValueType { return value.UnknownType }

type All struct{}

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
func (m *All) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
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
func (m *All) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf(`Expected 1 or more args for All(true, tobool(item)) but got %s`, n)
	}
	return m.Eval, nil
}
func (m *All) Type() value.ValueType { return value.BoolType }

type Split struct{}

// Split a string, accepts an optional with parameter
//
//     split(item, ",")
//
func (m *Split) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *Split) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for Split("apples,oranges",",") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *Split) Type() value.ValueType { return value.StringsType }

type Strip struct{}

// Strip a string, removing leading/trailing whitespace
//
//     strip(split("apples, oranges ",",")) => {"apples", "oranges"}
//
func (m *Strip) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	switch val := vals[0].(type) {
	case value.StringValue:
		sv := strings.Trim(val.ToString(), " \n\t\r")
		return value.NewStringValue(sv), true
	case value.StringsValue:
		svs := make([]string, val.Len())
		for i, sv := range val.Val() {
			svs[i] = strings.Trim(sv, " \n\t\r")
		}
		return value.NewStringsValue(svs), true
	}
	return nil, false
}
func (m *Strip) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf(`Expected 1 args for Strip(arg) but got %s`, n)
	}
	return m.Eval, nil
}
func (m *Strip) Type() value.ValueType { return value.UnknownType }

type Replace struct{}

// Replace a string(s), accepts any number of parameters to replace
//    replaces with ""
//
//     replace("/blog/index.html", "/blog","")  =>  /index.html
//     replace("/blog/index.html", "/blog")  =>  /index.html
//     replace("/blog/index.html", "/blog/archive/","/blog")  =>  /blog/index.html
//     replace(item, "M")
//
func (m *Replace) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
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
func (m *Replace) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for Replace("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *Replace) Type() value.ValueType { return value.StringType }

type Join struct{}

// Join items together (string concatenation)
//
//   join("apples","oranges",",")   => "apples,oranges"
//   join(["apples","oranges"],",") => "apples,oranges"
//   join("apples","oranges","")    => "applesoranges"
//
func (m *Join) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *Join) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf(`Expected 1 or more args for Join("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *Join) Type() value.ValueType { return value.StringType }

type HasPrefix struct{}

// HasPrefix string evaluation to see if string begins with
//
//   hasprefix("apples","ap")   => true
//   hasprefix("apples","o")   => false
//
func (m *HasPrefix) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	prefixStr := vals[1].ToString()
	if len(prefixStr) == 0 {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(strings.HasPrefix(vals[0].ToString(), prefixStr)), true
}
func (m *HasPrefix) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for HasPrefix("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *HasPrefix) Type() value.ValueType { return value.BoolType }

type HasSuffix struct{}

// HasSuffix string evaluation to see if string ends with
//
//   hassuffix("apples","es")   => true
//   hassuffix("apples","e")   => false
//
func (m *HasSuffix) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	suffixStr := vals[1].ToString()
	if suffixStr == "" {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(strings.HasSuffix(vals[0].ToString(), suffixStr)), true
}
func (m *HasSuffix) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for HasSuffix("apples","es") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *HasSuffix) Type() value.ValueType { return value.BoolType }

type Cast struct{}

// Cast :   type coercion
//
//   cast(identity AS <type>) => 5.0
//   cast(reg_date AS string) => "2014/01/12"
//
//  Types:  [char, string, int, float]
//
func (m *Cast) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *Cast) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 3 {
		return nil, fmt.Errorf("Expected 3 args for Cast(arg AS <type>) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Cast) Type() value.ValueType { return value.UnknownType }

type ToBool struct{}

// ToBool cast as string
//   must be able to convert to string
//
func (m *ToBool) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.BoolValueFalse, false
	}
	b, ok := value.ValueToBool(args[0])
	if !ok {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(b), true
}
func (m *ToBool) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToBool(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ToBool) Type() value.ValueType { return value.BoolType }

type ToInt struct{}

// Convert to Integer:   Best attempt at converting to integer
//
//   toint("5")          => 5, true
//   toint("5.75")       => 5, true
//   toint("5,555")      => 5555, true
//   toint("$5")         => 5, true
//   toint("5,555.00")   => 5555, true
//
func (m *ToInt) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *ToInt) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToInt(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ToInt) Type() value.ValueType { return value.IntType }

type ToNumber struct{}

// Convert to Number:   Best attempt at converting to integer
//
//   tonumber("5") => 5.0
//   tonumber("5.75") => 5.75
//   tonumber("5,555") => 5555
//   tonumber("$5") => 5.00
//   tonumber("5,555.00") => 5555
//
func (m *ToNumber) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	fv, ok := value.ValueToFloat64(vals[0])
	if !ok {
		return value.NewNumberNil(), false
	}
	return value.NewNumberValue(fv), true
}
func (m *ToNumber) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for ToNumber(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ToNumber) Type() value.ValueType { return value.NumberType }

type Now struct{}

// Get current time of Message (message time stamp) or else choose current
//   server time if none is available in message context
//
func (m *Now) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if ctx != nil && !ctx.Ts().IsZero() {
		t := ctx.Ts()
		return value.NewTimeValue(t), true
	}

	return value.NewTimeValue(time.Now().In(time.UTC)), true
}
func (m *Now) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 args for Now() but got %s", n)
	}
	return m.Eval, nil
}
func (m *Now) Type() value.ValueType { return value.TimeType }

type Yy struct{}

// Get year in integer from field, must be able to convert to date
//
//    yy()                 =>  15, true    // assuming it is 2015
//    yy("2014-03-01")     =>  14, true
//
func (m *Yy) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *Yy) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for Yy() but got %s", n)
	}
	return m.Eval, nil
}
func (m *Yy) Type() value.ValueType { return value.IntType }

type Mm struct{}

// Get month as integer from date
//   @optional timestamp (if not, gets from context reader)
//
//  mm()                =>  01, true  /// assuming message ts = jan 1
//  mm("2014-03-17")    =>  03, true
//
func (m *Mm) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *Mm) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 args for Mm() but got %s", n)
	}
	return m.Eval, nil
}
func (m *Mm) Type() value.ValueType { return value.IntType }

type YyMm struct{}

// Get yymm in 4 digits from argument if supplied, else uses message context ts
//
func (m *YyMm) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	}

	return value.EmptyStringValue, false
}
func (m *YyMm) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for YyMm() but got %s", n)
	}
	return m.Eval, nil
}
func (m *YyMm) Type() value.ValueType { return value.StringType }

type DayOfWeek struct{}

// day of week [0-6]
func (m *DayOfWeek) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday())), true
		}
	}

	return value.NewIntNil(), false
}
func (m *DayOfWeek) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for DayOfWeek() but got %s", n)
	}
	return m.Eval, nil
}
func (m *DayOfWeek) Type() value.ValueType { return value.IntType }

type HourOfWeek struct{}

// hour of week [0-167]
func (m *HourOfWeek) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}
func (m *HourOfWeek) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for HourOfWeek() but got %s", n)
	}
	return m.Eval, nil
}
func (m *HourOfWeek) Type() value.ValueType { return value.IntType }

type HourOfDay struct{}

// hour of day [0-23]
//  hourofday(field)
//  hourofday()  // Uses message time
func (m *HourOfDay) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
func (m *HourOfDay) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for HourOfDay(val) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HourOfDay) Type() value.ValueType { return value.IntType }

type ToTimestamp struct{}

// totimestamp:   convert to date, then to unix Seconds
//
func (m *ToTimestamp) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	dateStr, ok := value.ValueToString(args[0])
	if !ok {
		return value.NewIntValue(0), false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewIntValue(int64(t.Unix())), true
	}

	return value.NewIntValue(0), false
}
func (m *ToTimestamp) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for ToTimestamp(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *ToTimestamp) Type() value.ValueType { return value.IntType }

type ToDate struct{}

// todate:   convert to Date
//
//    // uses lytics/datemath
//    todate("now-3m")
//
//    // uses araddon/dateparse util to recognize formats
//    todate(field)
//
//    // first parameter is the layout/format
//    todate("01/02/2006", field )
//
func (m *ToDate) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if len(args) == 1 {
		dateStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeZeroValue, false
		}

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

	} else if len(args) == 2 {

		formatStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeZeroValue, false
		}

		dateStr, ok := value.ValueToString(args[1])
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
func (m *ToDate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 || len(n.Args) > 2 {
		return nil, fmt.Errorf(`Expected 1 or 2 args for ToDate([format] , field) but got %s`, n)
	}
	return m.Eval, nil
}
func (m *ToDate) Type() value.ValueType { return value.TimeType }

// todatein:   convert to Date with timezon
//
//    // uses lytics/datemath
//    todate("now-3m", "America/Los_Angeles")
//
//    // uses araddon/dateparse util to recognize formats
//    todate(field, "America/Los_Angeles")
//
type ToDateIn struct{}

func (m *ToDateIn) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected args for todatein( (field | "now-3h" ), location) but got %s`, n)
	}

	sn, ok := n.Args[1].(*expr.StringNode)
	if !ok {
		return nil, fmt.Errorf("Expected a string literal value for location like America/Los_Angeles")
	}

	loc, err := time.LoadLocation(sn.Text)
	if err != nil {
		return nil, err
	}

	if sn, ok = n.Args[0].(*expr.StringNode); ok {
		dateStr := sn.Text
		if ok && len(dateStr) > 3 && strings.ToLower(dateStr[:3]) == "now" {
			// its possible its an field called "now_date" or s
			if _, err := datemath.Eval(dateStr); err == nil {
				// Is date math
				return func(_ expr.EvalContext, _ []value.Value) (value.Value, bool) {
					if t, err := datemath.Eval(dateStr); err == nil {
						return value.NewTimeValue(t), true
					}
					return value.TimeZeroValue, false
				}, nil
			}
		}
	}

	// Return the Evaluator
	return func(_ expr.EvalContext, args []value.Value) (value.Value, bool) {
		valueDateStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeZeroValue, false
		}
		if t, err := dateparse.ParseIn(valueDateStr, loc); err == nil {
			// We are going to correct back to UTC. so all fields are in UTC.
			return value.NewTimeValue(t.In(time.UTC)), true
		}
		return value.TimeZeroValue, false
	}, nil
}
func (m *ToDateIn) Type() value.ValueType { return value.TimeType }

type TimeSeconds struct{}

// TimeSeconds time in Seconds, parses a variety of formats looking for seconds
// See github.com/araddon/dateparse for formats supported on date parsing
//
//    seconds("M10:30")      =>  630
//    seconds("M100:30")     =>  6030
//    seconds("00:30")       =>  30
//    seconds("30")          =>  30
//    seconds(30)            =>  30
//    seconds("2015/07/04")  =>  1435968000
//
func (m *TimeSeconds) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	switch vt := args[0].(type) {
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
func (m *TimeSeconds) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for TimeSeconds(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *TimeSeconds) Type() value.ValueType { return value.NumberType }

// UnixDateTruncFunc converts a value.Value to a unix timestamp string. This is used for the BigQuery export
// since value.TimeValue returns a unix timestamp with milliseconds (ie "1438445529707") by default.
// This gets displayed in BigQuery as "47547-01-24 10:49:05 UTC", because they expect seconds instead of milliseconds.
// This function "truncates" the unix timestamp to seconds to the form "1438445529.707"
// i.e the milliseconds are placed after the decimal place.
// Inspired by the "DATE_TRUNC" function used in PostgresQL and RedShift:
// http://www.postgresql.org/docs/8.1/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
//
// unixtrunc("1438445529707") --> "1438445529"
// unixtrunc("1438445529707", "seconds") --> "1438445529.707"
//
type TimeTrunc struct{ precision string }

func (m *TimeTrunc) EvalOne(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	switch v := args[0].(type) {
	case value.TimeValue:
		// If the value is of type "TimeValue", return the Unix representation.
		return value.NewStringValue(fmt.Sprintf("%d", v.Time().Unix())), true
	default:
		// Otherwise use date parse any
		t, err := dateparse.ParseAny(v.ToString())
		if err != nil {
			return value.NewStringValue(""), false
		}
		return value.NewStringValue(fmt.Sprintf("%d", t.Unix())), true
	}
}
func (m *TimeTrunc) EvalTwo(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	valTs := int64(0)
	switch itemT := args[0].(type) {
	case value.TimeValue:
		// Get the full Unix timestamp w/ milliseconds.
		valTs = itemT.Int()
	default:
		// If not a TimeValue, convert to a TimeValue and get Unix w/ milliseconds.
		btd := &ToDate{}
		tval, ok := btd.Eval(ctx, []value.Value{itemT})
		if !ok {
			return value.NewStringValue(""), false
		}
		if tv, isTime := tval.(value.TimeValue); isTime {
			valTs = tv.Int()
		} else {
			return value.NewStringValue(""), false
		}
	}

	// Look at the seconds argument to determine the truncation.
	precision, ok := value.ValueToString(args[1])
	if !ok {
		return value.NewStringValue(""), false
	}
	format := strings.ToLower(precision)

	switch format {
	case "s", "seconds":
		// If seconds: add milliseconds after the decimal place.
		return value.NewStringValue(fmt.Sprintf("%d.%d", valTs/1000, valTs%1000)), true
	case "ms", "milliseconds":
		// Otherwise return the Unix ts w/ milliseconds.
		return value.NewStringValue(fmt.Sprintf("%d", valTs)), true
	default:
		return value.EmptyStringValue, false
	}
}
func (m *TimeTrunc) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 1 {
		return m.EvalOne, nil
	} else if len(n.Args) == 2 {
		return m.EvalTwo, nil
	}
	return nil, fmt.Errorf("Expected 1 or 2 args for unixtrunc(field) but got %s", n)
}
func (m *TimeTrunc) Type() value.ValueType { return value.StringType }

type StrFromTime struct{}

// StrFromTime extraces certain parts from a time, similar to Python's StrfTime
// See http://strftime.org/ for Strftime directives.
//
//	strftime("2015/07/04", "%B") 		=> "July"
//	strftime("2015/07/04", "%B:%d") 	=> "July:4"
// 	strftime("1257894000", "%p")		=> "PM"
func (m *StrFromTime) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	// if we have 2 items, the first is the time string
	// and the second is the format string.
	// Use leekchan/timeutil package
	dateStr, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}

	formatStr, ok := value.ValueToString(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	t, err := dateparse.ParseAny(dateStr)
	if err != nil {
		return value.EmptyStringValue, false
	}

	formatted := timeutil.Strftime(&t, formatStr)
	return value.NewStringValue(formatted), true
}
func (m *StrFromTime) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 2 {
		return nil, fmt.Errorf("Expected 2 args for strftime(field, format_pattern) but got %s", n)
	}
	return m.Eval, nil
}
func (m *StrFromTime) Type() value.ValueType { return value.StringType }

type MapTime struct{}

// MapTime()    Create a map[string]time of each key
//
//  maptime(field)    => map[string]time{field_value:message_timestamp}
//  maptime(field, timestamp) => map[string]time{field_value:timestamp}
//
func (m *MapTime) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

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
		dateStr, ok := value.ValueToString(args[1])
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
func (m *MapTime) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 || len(n.Args) > 2 {
		return nil, fmt.Errorf("Expected 1 or 2 args for MapTime() but got %s", n)
	}
	return m.Eval, nil
}
func (m *MapTime) Type() value.ValueType { return value.MapTimeType }

type Email struct{}

// email a string, parses email
//
//     email("Bob <bob@bob.com>")  =>  bob@bob.com, true
//     email("Bob <bob>")          =>  "", false
//
func (m *Email) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := args[0]
	if val == nil || val.Nil() || val.Err() {
		return nil, false
	}
	emailStr := ""
	switch v := val.(type) {
	case value.StringValue:
		emailStr = v.ToString()
	case value.Slice:
		if v.Len() == 0 {
			return nil, false
		}
		v1 := v.SliceValue()[0]
		if v1 == nil {
			return nil, false
		}
		emailStr = v1.ToString()
	}

	if emailStr == "" {
		return value.EmptyStringValue, false
	}
	if len(emailStr) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(emailStr); err == nil {
		return value.NewStringValue(strings.ToLower(em.Address)), true
	}
	return value.EmptyStringValue, false
}
func (m *Email) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for Email(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Email) Type() value.ValueType { return value.StringType }

type EmailName struct{}

// emailname a string, parses email
//
//     emailname("Bob <bob@bob.com>") =>  Bob
//
func (m *EmailName) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
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
func (m *EmailName) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailName(fieldname) but got %s", n)
	}
	return m.Eval, nil
}
func (m *EmailName) Type() value.ValueType { return value.StringType }

type EmailDomain struct{}

// email a string, parses email
//
//     email("Bob <bob@bob.com>") =>  bob@bob.com
//
func (m *EmailDomain) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
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
func (m *EmailDomain) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailDomain(fieldname) but got %s", n)
	}
	return m.Eval, nil
}
func (m *EmailDomain) Type() value.ValueType { return value.StringType }

type Domains struct{}

// Extract Domains from a Value, or Values (must be urlish), doesn't do much/any validation
//
//     domains("http://www.lytics.io/index.html") =>  []string{"lytics.io"}
//
func (m *Domains) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	svals := value.NewStringsValue(make([]string, 0))
	for _, val := range args {
		switch v := val.(type) {
		case value.StringValue:
			svals.Append(v.Val())
		case value.StringsValue:
			for _, sv := range v.Val() {
				svals.Append(sv)
			}
		}
	}

	// Since its empty, we will just re-use it
	if svals.Len() == 0 {
		return svals, true
	}

	// Now convert to domains
	domains := value.NewStringsValue(make([]string, 0))
	for _, val := range svals.Val() {
		urlstr := strings.ToLower(val)
		if len(urlstr) < 8 {
			continue
		}

		// May not have an http prefix, if not assume it
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
func (m *Domains) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("Expected 1 or more args for Domains(arg, ...) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Domains) Type() value.ValueType { return value.StringsType }

type Domain struct{}

// Extract Domain from a Value, or Values (must be urlish), doesn't do much/any validation
//
//     domain("http://www.lytics.io/index.html") =>  "lytics.io"
//
//  if input is a list of strings, only first is evaluated, for plural see domains()
//
func (m *Domain) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	urlstr := ""
	switch itemT := args[0].(type) {
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
func (m *Domain) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Domain(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Domain) Type() value.ValueType { return value.StringType }

type Host struct{}

// Extract host from a String (must be urlish), doesn't do much/any validation
//
//     host("http://www.lytics.io/index.html") =>  www.lytics.io
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
func (m *Host) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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
func (m *Host) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Host(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Host) Type() value.ValueType { return value.StringType }

type Hosts struct{}

// Extract hosts from a Strings (must be urlish), doesn't do much/any validation
//
//     hosts("http://www.lytics.io", "http://www.activate.lytics.io") => www.lytics.io, www.activate.lytics.io
//
func (m *Hosts) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	vals := value.NewStringsValue(make([]string, 0))
	for _, item := range args {
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
func (m *Hosts) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("Expected 1 or more args for Hosts() but got %s", n)
	}
	return m.Eval, nil
}
func (m *Hosts) Type() value.ValueType { return value.StringsType }

type UrlDecode struct{}

// url decode a string
//
//     urldecode("http://www.lytics.io/index.html") =>  http://www.lytics.io
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
func (m *UrlDecode) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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
func (m *UrlDecode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for UrlDecode(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *UrlDecode) Type() value.ValueType { return value.StringType }

type UrlPath struct{}

// UrlPath Extract url path from a String (must be urlish), doesn't do much/any validation
//
//     path("http://www.lytics.io/blog/index.html") =>  blog/index.html
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
func (m *UrlPath) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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
func (m *UrlPath) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for UrlPath() but got %s", n)
	}
	return m.Eval, nil
}
func (m *UrlPath) Type() value.ValueType { return value.StringType }

type Qs struct{}

// Qs Extract qs param from a string (must be url valid)
//
//     qs("http://www.lytics.io/?utm_source=google","utm_source")  => "google", true
//
func (m *Qs) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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

	keyVal, ok := value.ValueToString(args[1])
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
func (m *Qs) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for Qs(url, param) but got %s", n)
	}
	return m.Eval, nil
}
func (m *Qs) Type() value.ValueType { return value.StringType }

type UrlMain struct{}

// UrlMain remove the querystring and scheme from url
//
//     urlmain("http://www.lytics.io/?utm_source=google")  => "www.lytics.io/", true
//
func (m *UrlMain) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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
func (m *UrlMain) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for UrlMain() but got %s", n)
	}
	return m.Eval, nil
}
func (m *UrlMain) Type() value.ValueType { return value.StringType }

type UrlMinusQs struct{}

// UrlMinusQs removes a specific query parameter and its value from a url
//
//     urlminusqs("http://www.lytics.io/?q1=google&q2=123", "q1") => "http://www.lytics.io/?q2=123", true
//
func (m *UrlMinusQs) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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
	keyVal, ok := value.ValueToString(args[1])
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
func (m *UrlMinusQs) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for UrlMinusQs(url, qsparam) but got %s", n)
	}
	return m.Eval, nil
}
func (m *UrlMinusQs) Type() value.ValueType { return value.StringType }

// UrlWithQueryFunc strips a url and retains only url parameters that match the expressions in keyItems.
type UrlWithQuery struct{ include []*regexp.Regexp }

func (m *UrlWithQuery) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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

	up, err := url.Parse(val)
	if err != nil {
		return value.EmptyStringValue, false
	}

	if len(args) == 1 {
		return value.NewStringValue(fmt.Sprintf("%s%s", up.Host, up.Path)), true
	}

	oldvals := up.Query()
	newvals := make(url.Values)
	for k, v := range oldvals {
		// include fields specified as arguments
		for _, pattern := range m.include {
			if pattern.MatchString(k) {
				newvals[k] = v
				break
			}
		}
	}

	up.RawQuery = newvals.Encode()
	if up.RawQuery == "" {
		return value.NewStringValue(fmt.Sprintf("%s%s", up.Host, up.Path)), true
	}

	return value.NewStringValue(fmt.Sprintf("%s%s?%s", up.Host, up.Path, up.RawQuery)), true
}
func (*UrlWithQuery) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("Expected at least 1 args for urlwithqs(url, param, param2) but got %s", n)
	}

	m := &UrlWithQuery{}
	if len(n.Args) == 1 {
		return m.Eval, nil
	}

	// Memoize these compiled reg-expressions
	m.include = make([]*regexp.Regexp, 0)
	for _, n := range n.Args[1:] {
		keyItem, ok := vm.Eval(nil, n)
		if !ok {
			continue
		}
		keyVal, ok := value.ValueToString(keyItem)
		if !ok {
			continue
		}
		if keyVal == "" {
			continue
		}

		keyRegexp, err := regexp.Compile(keyVal)
		if err != nil {
			continue
		}
		m.include = append(m.include, keyRegexp)
	}
	return m.Eval, nil
}
func (m *UrlWithQuery) Type() value.ValueType { return value.StringType }

type UserAgent struct{}

// UserAgent Extract user agent features
//
//     tobool(useragent(user_agent_field,"mobile")  => "true", true
//
func (m *UserAgent) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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

	/*
	   fmt.Printf("%v\n", ua.Mobile())   // => false
	   fmt.Printf("%v\n", ua.Bot())      // => false
	   fmt.Printf("%v\n", ua.Mozilla())  // => "5.0"

	   fmt.Printf("%v\n", ua.Platform()) // => "X11"
	   fmt.Printf("%v\n", ua.OS())       // => "Linux x86_64"

	   name, version := ua.Engine()
	   fmt.Printf("%v\n", name)          // => "AppleWebKit"
	   fmt.Printf("%v\n", version)       // => "537.11"

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => "Chrome"
	   fmt.Printf("%v\n", version)       // => "23.0.1271.97"

	   // Let's see an example with a bot.

	   ua.Parse("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")

	   fmt.Printf("%v\n", ua.Bot())      // => true

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => Googlebot
	   fmt.Printf("%v\n", version)       // => 2.1
	*/

	method, ok := value.ValueToString(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	ua := user_agent.New(val)

	switch strings.ToLower(method) {
	case "bot":
		return value.NewStringValue(fmt.Sprintf("%v", ua.Bot())), true
	case "mobile":
		return value.NewStringValue(fmt.Sprintf("%v", ua.Mobile())), true
	case "mozilla":
		return value.NewStringValue(ua.Mozilla()), true
	case "platform":
		return value.NewStringValue(ua.Platform()), true
	case "os":
		return value.NewStringValue(ua.OS()), true
	case "engine":
		name, _ := ua.Engine()
		return value.NewStringValue(name), true
	case "engine_version":
		_, version := ua.Engine()
		return value.NewStringValue(version), true
	case "browser":
		name, _ := ua.Browser()
		return value.NewStringValue(name), true
	case "browser_version":
		_, version := ua.Browser()
		return value.NewStringValue(version), true
	}
	return value.EmptyStringValue, false
}
func (m *UserAgent) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for UserAgent(user_agent_field, feature) but got %s", n)
	}
	return m.Eval, nil
}
func (m *UserAgent) Type() value.ValueType { return value.StringType }

type UserAgentMap struct{}

// UserAgentMap Extract user agent features
//
//     useragentmap(user_agent_field)  => {"mobile": "false","platform":"X11"}, true
//
func (m *UserAgentMap) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
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

	/*
	   fmt.Printf("%v\n", ua.Mobile())   // => false
	   fmt.Printf("%v\n", ua.Bot())      // => false
	   fmt.Printf("%v\n", ua.Mozilla())  // => "5.0"

	   fmt.Printf("%v\n", ua.Platform()) // => "X11"
	   fmt.Printf("%v\n", ua.OS())       // => "Linux x86_64"

	   name, version := ua.Engine()
	   fmt.Printf("%v\n", name)          // => "AppleWebKit"
	   fmt.Printf("%v\n", version)       // => "537.11"

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => "Chrome"
	   fmt.Printf("%v\n", version)       // => "23.0.1271.97"

	   // Let's see an example with a bot.

	   ua.Parse("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")

	   fmt.Printf("%v\n", ua.Bot())      // => true

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => Googlebot
	   fmt.Printf("%v\n", version)       // => 2.1
	*/

	ua := user_agent.New(val)
	out := make(map[string]string)
	out["bot"] = fmt.Sprintf("%v", ua.Bot())
	out["mobile"] = fmt.Sprintf("%v", ua.Mobile())
	out["mozilla"] = ua.Mozilla()
	out["platform"] = ua.Platform()
	out["os"] = ua.OS()
	name, version := ua.Engine()
	out["engine"] = name
	out["engine_version"] = version
	name, version = ua.Browser()
	out["browser"] = name
	out["browser_version"] = version
	return value.NewMapStringValue(out), true
}
func (m *UserAgentMap) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for useragentmap(user_agent) but got %s", n)
	}
	return m.Eval, nil
}
func (m *UserAgentMap) Type() value.ValueType { return value.MapStringType }

type HashSip struct{}

// hash.sip() hash a value to a 64 bit int
//
//     hash.sip("/blog/index.html")  =>  1234
//
func (m *HashSip) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.NewIntValue(0), false
		}
		val = itemT.Val()[0]
	}
	if val == "" {
		return value.NewIntValue(0), false
	}

	hash := siphash.Hash(0, 1, []byte(val))

	return value.NewIntValue(int64(hash)), true
}
func (m *HashSip) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for hash.sip(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSip) Type() value.ValueType { return value.IntType }

type HashMd5 struct{}

// HashMd5Func Hash a value to MD5 string
//
//     hash.md5("/blog/index.html")  =>  abc345xyz
//
func (m *HashMd5) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := md5.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashMd5) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for hash.md5(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashMd5) Type() value.ValueType { return value.StringType }

type HashSha1 struct{}

// HashSha1Func Hash a value to SHA256 string
//
//     hash.sha1("/blog/index.html")  =>  abc345xyz
//
func (m *HashSha1) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha1.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashSha1) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha1(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSha1) Type() value.ValueType { return value.StringType }

type HashSha256 struct{}

// HashSha256Func Hash a value to SHA256 string
//
//     hash.sha256("/blog/index.html")  =>  abc345xyz
//
func (m *HashSha256) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha256.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashSha256) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha256(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSha256) Type() value.ValueType { return value.StringType }

type HashSha512 struct{}

// HashSha512Func Hash a value to SHA512 string
//
//     hash.sha512("/blog/index.html")  =>  abc345xyz
//
func (m *HashSha512) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha512.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashSha512) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha512(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSha512) Type() value.ValueType { return value.StringType }

type EncodeB64Encode struct{}

// Base 64 encoding function
//
//     encoding.b64encode("hello world=")  =>  aGVsbG8gd29ybGQ=
//
func (m *EncodeB64Encode) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	encodedString := base64.StdEncoding.EncodeToString([]byte(args[0].ToString()))
	return value.NewStringValue(encodedString), true
}
func (m *EncodeB64Encode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for encoding.b64encode(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *EncodeB64Encode) Type() value.ValueType { return value.StringType }

type EncodeB64Decode struct{}

// Base 64 encoding function
//
//     encoding.b64decode("aGVsbG8gd29ybGQ=")  =>  "hello world"
//
func (m *EncodeB64Decode) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}

	by, err := base64.StdEncoding.DecodeString(args[0].ToString())
	if err != nil {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(string(by)), true
}
func (m *EncodeB64Decode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for encoding.b64decode(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *EncodeB64Decode) Type() value.ValueType { return value.StringType }
