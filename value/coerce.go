package value

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/lytics/datemath"
)

var (
	_ = u.EMPTY

	ErrConversion             = fmt.Errorf("Error converting type")
	ErrConversionNotSupported = fmt.Errorf("Unsupported conversion")
)

// ValueTypeFromString take a string value and infer valuetype
// Will infer based on the following rules:
// - If parseable as int, will be int
// - if not above, and parse bool, is bool
// - if not above, and parse float, float
// - if not above, and parse date, date
// - else string
func ValueTypeFromString(val string) ValueType {
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return IntType
	} else if _, err := strconv.ParseBool(val); err == nil {
		return BoolType
	} else if _, err := strconv.ParseFloat(val, 64); err == nil {
		return NumberType
	} else if _, err := dateparse.ParseAny(val); err == nil {
		return TimeType
	}
	return StringType
}

// ValueTypeFromStringAll take a string value and infer valuetype
// adding the valid type JSON preferred over raw string.
//
// Will infer based on the following rules:
// - If parseable as int, will be int
// - if not above, and parse bool, is bool
// - if not above, and parse float, float
// - if not above, and parse date, date
// - if not above, and appears to be json (doesn't have to be valid)
// - else string
func ValueTypeFromStringAll(val string) ValueType {
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return IntType
	} else if _, err := strconv.ParseBool(val); err == nil {
		return BoolType
	} else if _, err := strconv.ParseFloat(val, 64); err == nil {
		return NumberType
	} else if _, err := dateparse.ParseAny(val); err == nil {
		return TimeType
	}
	by := []byte(val)
	if u.IsJson(by) {
		return JsonType
	}
	return StringType
}

// Cast a value to given value type
func Cast(valType ValueType, val Value) (Value, error) {
	switch valType {
	case ByteSliceType:
		switch valt := val.(type) {
		case ByteSliceValue:
			return valt, nil
		default:
			return NewByteSliceValue([]byte(val.ToString())), nil
		}

	case TimeType:
		t, ok := ValueToTime(val)
		if ok {
			return NewTimeValue(t), nil
		}
	case StringType:
		return NewStringValue(val.ToString()), nil
	case IntType:
		iv, ok := ValueToInt64(val)
		if ok {
			return NewIntValue(iv), nil
		}
		return nil, ErrConversion
	}
	return nil, ErrConversionNotSupported
}

// Equal function compares equality after detecting type.
// error if it could not evaluate
func Equal(l, r Value) (bool, error) {

	if l == nil && r == nil {
		return true, nil
	}

	if r == nil {
		switch l.(type) {
		case NilValue:
			return true, nil
		}
		return false, nil
	}
	if _, isNil := r.(NilValue); isNil {
		switch l.(type) {
		case nil:
			return true, nil
		case NilValue:
			return true, nil
		}
		return false, nil
	}

	switch lt := l.(type) {
	case nil, NilValue:
		return false, nil
	case StringValue:
		if lt.Val() == r.ToString() {
			return true, nil
		}
		return false, nil
	case IntValue:
		rhv, _ := ValueToInt64(r)
		return lt.Val() == rhv, nil
	case NumberValue:
		rhv, _ := ValueToFloat64(r)
		return lt.Val() == rhv, nil
	case BoolValue:
		rhv, _ := ValueToBool(r)
		return lt.Val() == rhv, nil
	case TimeValue:
		rhv, _ := ValueToTime(r)
		return lt.Val() == rhv, nil
	case Slice:
		if rhv, ok := r.(Slice); ok {
			if lt.Len() != rhv.Len() {
				return false, nil
			}
			rhslice := rhv.SliceValue()
			for i, lhval := range lt.SliceValue() {
				if eq, err := Equal(lhval, rhslice[i]); !eq || err != nil {
					return false, nil
				}
			}
			return true, nil
		}
	}
	return false, fmt.Errorf("Could not evaluate equals for %v = %v", l.Value(), r.Value())
}

// ValueToString convert all scalar values to their go string.
func ValueToString(val Value) (string, bool) {
	if val == nil || val.Err() {
		return "", false
	}
	switch v := val.(type) {
	case StringValue:
		return v.Val(), true
	case TimeValue:
		return fmt.Sprintf("%v", v.Val()), true
	case ByteSliceValue:
		return string(v.Val()), true
	case NumericValue, BoolValue, IntValue:
		return val.ToString(), true
	case Slice:
		// This is controversial, if we are demanding a "ToString"
		// should we:
		// 1)  take first?
		// 2)  append comma separated?
		// 3)  error?
		//
		// Our answer is that we are demanding a scalar string
		// and going to take first.  calling function would have had to do type detection
		// if they wanted something else.
		if v.Len() == 0 {
			return "", false
		}
		v1 := v.SliceValue()[0]
		if v1 == nil {
			return "", false
		}
		return v1.ToString(), true
	}
	return "", false
}

// ValueToStrings convert all scalar values to their go []string.
func ValueToStrings(val Value) ([]string, bool) {
	if val == nil || val.Err() {
		return nil, false
	}
	switch v := val.(type) {
	case StringValue:
		return []string{v.Val()}, true
	case TimeValue:
		return []string{fmt.Sprintf("%v", v.Val())}, true
	case ByteSliceValue:
		return []string{string(v.Val())}, true
	case NumericValue, BoolValue, IntValue:
		return []string{val.ToString()}, true
	case Slice:
		if v.Len() == 0 {
			return nil, false
		}
		vals := make([]string, v.Len())
		for i, val := range v.SliceValue() {
			if val != nil {
				vals[i] = val.ToString()
			}
		}
		return vals, true
	}
	return nil, false
}

// is this boolean string?
func IsBool(sv string) bool {
	_, err := strconv.ParseBool(sv)
	if err == nil {
		return true
	}
	return false
}

func BoolStringVal(sv string) bool {
	bv, err := strconv.ParseBool(sv)
	if err != nil {
		return false
	}
	return bv
}

// ValueToBool Convert a value type to a bool if possible
func ValueToBool(val Value) (bool, bool) {
	if val == nil || val.Nil() || val.Err() {
		return false, false
	}
	switch v := val.(type) {
	case NumericValue:
		iv := v.Int()
		if iv == 0 {
			return false, true
		} else if iv == 1 {
			return true, true
		} else {
			return false, false
		}
	case StringValue:
		bv, err := strconv.ParseBool(v.Val())
		if err != nil {
			return false, false
		}
		return bv, true
	case BoolValue:
		return v.Val(), true
	}
	return false, false
}

// ValueToFloat64 Convert a value type to a float64 if possible
func ValueToFloat64(val Value) (float64, bool) {
	if val == nil || val.Nil() || val.Err() {
		return math.NaN(), false
	}
	switch v := val.(type) {
	case NumericValue:
		return v.Float(), true
	case StringValue:
		return StringToFloat64(v.Val())
	case BoolValue:
		// Should we co-erce bools to 0/1?
		if v.Val() {
			return float64(1), true
		}
		return float64(0), true
	}
	return math.NaN(), false
}

// ValueToInt Convert a value type to a int if possible
func ValueToInt(val Value) (int, bool) {
	iv, ok := ValueToInt64(val)
	return int(iv), ok
}

// ValueToInt64 Convert a value type to a int64 if possible
func ValueToInt64(val Value) (int64, bool) {
	if val == nil || val.Nil() || val.Err() {
		return 0, false
	}
	switch v := val.(type) {
	case NumericValue:
		return v.Int(), true
	case StringValue:
		return convertStringToInt64(0, v.Val())
	case Slice:
		if v.Len() > 0 {
			return ValueToInt64(v.SliceValue()[0])
		}
	}
	return 0, false
}

// Convert a value type to a time if possible
func ValueToTime(val Value) (time.Time, bool) {
	return ValueToTimeAnchor(val, time.Now())
}

func ValueToTimeAnchor(val Value, anchor time.Time) (time.Time, bool) {
	switch v := val.(type) {
	case TimeValue:
		return v.Val(), true
	case StringValue:
		te := v.Val()
		if len(te) > 3 && strings.ToLower(te[:3]) == "now" {
			// Is date math
			t, err := datemath.EvalAnchor(anchor, te)
			if err != nil {
				return time.Time{}, false
			}
			return t, true
		}

		t, err := dateparse.ParseAny(te)
		if err == nil {
			return t, true
		}

	case IntValue, NumberValue:
		t, err := dateparse.ParseIn(v.ToString(), time.Local)
		if err != nil {
			return time.Time{}, false
		}
		if t.Year() < 1800 || t.Year() > 2120 {
			return t, false
		}
		return t, true
	default:
		//u.Warnf("un-handled type to time? %#v", val)
	}
	return time.Time{}, false
}

// StringToFloat64 converts a string to a float
// includes replacement of $ and other monetary format identifiers.
// May return math.NaN
func StringToFloat64(s string) (float64, bool) {
	if s == "" {
		return math.NaN(), false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return f, true
	}
	s = intStrReplacer.Replace(s)
	f, err = strconv.ParseFloat(s, 64)
	if err == nil {
		return f, true
	}
	return math.NaN(), false
}

// Some strings we are trying to convert into Numbers are messy
// $3.12 etc, lets replace them and retry conversion again
var intStrReplacer = strings.NewReplacer("$", "", ",", "", "£", "", "€", "", " ", "")

func convertStringToInt64(depth int, s string) (int64, bool) {
	var i int64
	var err error
	if strings.HasPrefix(s, "0x") {
		i, err = strconv.ParseInt(s, 16, 64)
	} else if strings.Contains(s, ".") {
		fv, err2 := strconv.ParseFloat(s, 64)
		if err2 == nil {
			// So, we are going to TRUNCATE, ie round down
			return int64(fv), true
		}
		err = err2
	} else {
		i, err = strconv.ParseInt(s, 10, 64)
	}
	if err == nil {
		return int64(i), true
	}
	if depth == 0 {
		s = intStrReplacer.Replace(s)
		return convertStringToInt64(1, s)
	}
	return 0, false
}

func marshalFloat(n float64) ([]byte, error) {
	if math.IsNaN(n) {
		return json.Marshal("NaN")
	} else if math.IsInf(n, 1) {
		return json.Marshal("+Inf")
	} else if math.IsInf(n, -1) {
		return json.Marshal("-Inf")
	}
	return json.Marshal(n)
}
