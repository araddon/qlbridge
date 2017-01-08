package value

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
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
		return NewByteSliceValue([]byte(val.ToString())), nil
	case TimeType:
		switch valt := val.(type) {
		case StringValue:
			if t, err := dateparse.ParseAny(valt.Val()); err == nil {
				return NewTimeValue(t), nil
			} else {
				return nil, err
			}
		case TimeValue:
			return valt, nil
		}
	case StringType:
		sv := val.ToString()
		return NewStringValue(sv), nil
	case IntType:
		iv, ok := ToInt64(val.Rv())
		if ok {
			return NewIntValue(iv), nil
		}
		return nil, ErrConversion
	}
	return nil, ErrConversionNotSupported
}

func CanCoerce(from, to reflect.Value) bool {
	if from.Kind() == reflect.Interface {
		from = from.Elem()
	}
	if to.Kind() == reflect.Interface {
		to = to.Elem()
	}

	switch from.Kind() {
	case reflect.Float32, reflect.Float64:

		switch to.Kind() {
		case reflect.Float32, reflect.Float64:
			return true
		case reflect.Int, reflect.Int32, reflect.Int64:
			return true
		case reflect.Bool:
			return false
		case reflect.String:
			return true
		}

	case reflect.Int, reflect.Int32, reflect.Int64:
		switch to.Kind() {
		case reflect.Float32, reflect.Float64:
			return true
		case reflect.Int, reflect.Int32, reflect.Int64:
			return true
		case reflect.Bool:
			return false
		case reflect.String:
			return true
		}
	case reflect.Bool:
		switch to.Kind() {
		case reflect.Float32, reflect.Float64:
			return true
		case reflect.Int, reflect.Int32, reflect.Int64:
			return true
		case reflect.Bool:
			return true
		case reflect.String:
			return true
		}
	case reflect.String:
		switch to.Kind() {
		case reflect.Float32, reflect.Float64:
			return true
		case reflect.Int, reflect.Int32, reflect.Int64:
			return true
		case reflect.Bool:
			return true
		case reflect.String:
			return true
		}
	}
	return false
}

// Given a reflect.Value coerce a 2nd into same type (so we can compare equality)
//   coerces into limited set of types
//
//   int(8,16,32,64), uint(16,32,64,8)   =>    int64
//   floats                              =>    float64
//   string                              =>    string
//   bool                                =>    bool
func CoerceTo(to, itemToConvert reflect.Value) reflect.Value {
	if to.Kind() == reflect.Interface {
		to = to.Elem()
	}
	if itemToConvert.Kind() == reflect.Interface {
		itemToConvert = itemToConvert.Elem()
	}

	switch to.Kind() {
	case reflect.Float32, reflect.Float64:
		fv, _ := ToFloat64(itemToConvert)
		return reflect.ValueOf(fv)
	case reflect.Int, reflect.Int32, reflect.Int64:
		iv, _ := ToInt64(itemToConvert)
		return reflect.ValueOf(iv)
	case reflect.Bool:
		return reflect.ValueOf(itemToConvert.Bool())
	case reflect.String:
		return reflect.ValueOf(ToStringUnchecked(itemToConvert))
	}
	return reflect.ValueOf("")
}

// Coerce interface{} values (string,int,int64, float, []byte) into appropriate
//   vm.Value type
//
//   int(8,16,32,64), uint(16,32,64,8)   =>    IntValue
//   floats                              =>    NumberValue
//   string                              =>    StringValue
//   bool                                =>    BoolValue
//
// TODO:
//    []byte, json.RawMessage,
//    struct{}
//    time.Time
func ToValue(v interface{}) (Value, error) {
	switch val := v.(type) {
	case string:
		if val == "null" || val == "NULL" {
			return NewStringValue(""), nil
		}
		return NewStringValue(val), nil
	case []string:
		if len(val) == 1 && (val[0] == "null" || val[0] == "NULL") {
			// What should this be?
		}
		return NewStringsValue(val), nil
	case int8:
		return NewIntValue(int64(val)), nil
	case *int8:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case int16:
		return NewIntValue(int64(val)), nil
	case *int16:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case int:
		return NewIntValue(int64(val)), nil
	case *int:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case int32:
		return NewIntValue(int64(val)), nil
	case *int32:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case int64:
		return NewIntValue(int64(val)), nil
	case *int64:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case uint8:
		return NewIntValue(int64(val)), nil
	case *uint8:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case uint32:
		return NewIntValue(int64(val)), nil
	case *uint32:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case uint64:
		return NewIntValue(int64(val)), nil
	case *uint64:
		if val != nil {
			return NewIntValue(int64(*val)), nil
		}
		return NewIntValue(0), nil
	case float32:
		return NewNumberValue(float64(val)), nil
	case *float32:
		if val != nil {
			return NewNumberValue(float64(*val)), nil
		}
		return NewNumberValue(0), nil
	case float64:
		return NewNumberValue(float64(val)), nil
	case *float64:
		if val != nil {
			return NewNumberValue(float64(*val)), nil
		}
		return NewNumberValue(0), nil
	case bool:
		return NewBoolValue(val), nil
		// case []byte:
		// 	if string(val) == "null" || string(val) == "NULL" {
		// 		return "", nil
		// 	}
		// 	return string(val), nil
		// case json.RawMessage:
		// 	if string(val) == "null" || string(val) == "NULL" {
		// 		return "", nil
		// 	}
		// 	return string(val), nil
	}
	return NilStructValue, fmt.Errorf("Could not coerce to Value: %T %v", v, v)
}

//  Equal function
//
//   returns bool, error
//       first bool for if they are equal
//       error if it could not evaluate
func Equal(itemA, itemB Value) (bool, error) {
	if itemA == nil && itemB == nil {
		return true, nil
	}

	if itemA == nil {
		return false, nil
	}

	if itemB == nil {
		return false, nil
	}

	//return BoolValue(itemA == itemB)
	rvb := CoerceTo(itemA.Rv(), itemB.Rv())

	switch rvb.Kind() {
	case reflect.String:
		return rvb.String() == itemA.Rv().String(), nil
	case reflect.Int64:
		return rvb.Int() == itemA.Rv().Int(), nil
	case reflect.Float64:
		return rvb.Float() == itemA.Rv().Float(), nil
	case reflect.Bool:
		//u.Infof("Equal?  %v  %v  ==? %v", itemA.Rv().Bool(), rvb.Bool(), itemA.Rv().Bool() == rvb.Bool())
		return rvb.Bool() == itemA.Rv().Bool(), nil
	default:
		u.Warnf("Unknown kind?  %v", rvb.Kind())
	}
	//u.Infof("Eq():    a:%T  b:%T     %v=%v? %v", itemA, itemB, itemA.Rv(), rvb, itemA.Rv() == rvb)
	return false, fmt.Errorf("Could not evaluate equals")
}

// ValueToString convert all values
func ValueToString(val Value) (string, bool) {
	if val == nil || val.Nil() || val.Err() {
		return "", false
	}
	switch v := val.(type) {
	case NumericValue, BoolValue:
		return val.ToString(), true
	case StringValue:
		return v.Val(), true
		// slices?   []strings?
	}
	return "", false
}

// ToString convert all reflect.Value-s into string.
func ToString(v reflect.Value) (string, bool) {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if !v.IsValid() {
		return "nil", false
	}
	switch v.Kind() {
	case reflect.String:
		return v.String(), true
	case reflect.Slice:
		if v.Len() == 0 {
			return "", false
		} else if v.Len() == 1 {
			return v.Index(0).String(), true
		} else {
			// Grab first non-nil string in slice
			for i := 0; i < v.Len(); i++ {
				if len(v.Index(i).String()) > 0 {
					return v.Index(i).String(), true
				}
			}
			// do we grab first one?   or fail?
			//u.Warnf("ToString() on slice of len=%d vals=%#v   v=%v?  What should we do?  %v", v.Len(), v.Interface(), v, v.Type())
		}
	}
	// TODO:  this sucks, fix me
	return fmt.Sprint(v.Interface()), true
}

func ToStringUnchecked(v reflect.Value) string {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() == reflect.String {
		return v.String()
	}
	if !v.IsValid() {
		return "nil"
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Slice:
		if v.Len() == 1 {
			return v.Index(0).String()
		}
		u.Warnf("ToString() on slice of len=%d vals=%v ?  What should we do?", v.Len(), v)
	}
	return fmt.Sprint(v.Interface())
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

// toBool convert all reflect.Value-s into bool.
func ToBool(v reflect.Value) (bool, bool) {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		iv := int64(v.Float())
		if iv == 0 {
			return false, true
		} else if iv == 1 {
			return true, true
		} else {
			return false, false
		}
	case reflect.Int, reflect.Int32, reflect.Int64:
		iv := v.Int()
		if iv == 0 {
			return false, true
		} else if iv == 1 {
			return true, true
		} else {
			return false, false
		}
	case reflect.Bool:
		return v.Bool(), true
	case reflect.String:
		bv, err := strconv.ParseBool(v.String())
		if err == nil {
			return bv, true
		}
		// Should we support this?
		iv, ok := ToInt64(v)
		if ok && iv == 1 {
			return true, true
		} else if ok && iv == 0 {
			return false, true
		}
	}
	return false, false
}

// Convert a value type to a bool if possible
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

// Convert a value type to a float64 if possible
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

func ValueToInt(val Value) (int, bool) {
	iv, ok := ValueToInt64(val)
	return int(iv), ok
}

// Convert a value type to a int64 if possible
func ValueToInt64(val Value) (int64, bool) {
	if val == nil || val.Nil() || val.Err() {
		return 0, false
	}
	switch v := val.(type) {
	case NumericValue:
		return v.Int(), true
	case StringValue:
		iv, err := strconv.ParseInt(v.Val(), 10, 64)
		if err == nil {
			return iv, true
		}
		fv, ok := StringToFloat64(v.Val())
		if ok {
			return int64(fv), true
		}
	}
	return 0, false
}

// Convert a value type to a time if possible
func ValueToTime(val Value) (time.Time, bool) {
	switch v := val.(type) {
	case TimeValue:
		return v.Val(), true
	case StringValue:
		te := v.Val()
		if len(te) > 3 && strings.ToLower(te[:3]) == "now" {
			// Is date math
			t, err := datemath.Eval(te[3:])
			if err != nil {
				return time.Time{}, false
			}
			return t, true
		}

		t, err := dateparse.ParseAny(te)
		if err != nil {
			return time.Time{}, false
		}
		return t, true

	default:
		//u.Warnf("un-handled type to time? %#v", val)
	}
	return time.Time{}, false
}

// toFloat64 convert all reflect.Value-s into float64.
func ToFloat64(v reflect.Value) (float64, bool) {
	return convertToFloat64(0, v)
}
func convertToFloat64(depth int, v reflect.Value) (float64, bool) {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	case reflect.Int16, reflect.Int8, reflect.Int, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.String:
		s := v.String()
		var f float64
		var err error
		if strings.HasPrefix(s, "0x") {
			f, err = strconv.ParseFloat(s, 64)
		} else {
			f, err = strconv.ParseFloat(s, 64)
		}
		if err == nil {
			return float64(f), true
		}
		if depth == 0 {
			s = intStrReplacer.Replace(s)
			rv := reflect.ValueOf(s)
			return convertToFloat64(1, rv)
		}
	case reflect.Slice:
		// Should we grab first one?  Or Error?
		//u.Warnf("ToFloat() but is slice?: %T first=%v", v, v.Index(0))
		return convertToFloat64(0, v.Index(0))
	default:
		//u.Warnf("Cannot convert type?  %v", v.Kind())
	}
	return math.NaN(), false
}
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

// IsNilish returns true
//    integers = 0 = true (is nilish)
//    floats = 0 = true
//    strings = ""
//    pointers = nil = true
func IsNilIsh(v reflect.Value) bool {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float() == float64(0)
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int() == int64(0)
	case reflect.String:
		return v.String() == ""
	default:
		return isNil(v)
	}
	return false
}

func isNil(v reflect.Value) bool {
	if !v.IsValid() || v.Kind().String() == "unsafe.Pointer" {
		return true
	}
	if (v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr) && v.IsNil() {
		return true
	}
	return false
}

func isNum(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// equal return true when lhsV and rhsV is same value.
func equal(lhsV, rhsV reflect.Value) bool {
	if isNil(lhsV) && isNil(rhsV) {
		return true
	}
	if lhsV.Kind() == reflect.Interface || lhsV.Kind() == reflect.Ptr {
		lhsV = lhsV.Elem()
	}
	if rhsV.Kind() == reflect.Interface || rhsV.Kind() == reflect.Ptr {
		rhsV = rhsV.Elem()
	}
	if !lhsV.IsValid() || !rhsV.IsValid() {
		return true
	}
	if isNum(lhsV) && isNum(rhsV) {
		if rhsV.Type().ConvertibleTo(lhsV.Type()) {
			rhsV = rhsV.Convert(lhsV.Type())
		}
	}
	if lhsV.CanInterface() && rhsV.CanInterface() {
		return reflect.DeepEqual(lhsV.Interface(), rhsV.Interface())
	}
	return reflect.DeepEqual(lhsV, rhsV)
}

// Some strings we are trying to convert into Numbers are messy
//   $3.12 etc, lets replace them and retry conversion again
var intStrReplacer = strings.NewReplacer("$", "", ",", "", "£", "", "€", "", " ", "")

// toInt64 convert all reflect.Value-s into int64.
func ToInt64(v reflect.Value) (int64, bool) {
	return convertToInt64(0, v)
}
func convertToInt64(depth int, v reflect.Value) (int64, bool) {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return int64(v.Float()), true
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.String:
		s := v.String()
		var i int64
		var err error
		if strings.HasPrefix(s, "0x") {
			i, err = strconv.ParseInt(s, 16, 64)
		} else if strings.Contains(s, ".") {
			fv, err2 := strconv.ParseFloat(s, 64)
			if err2 == nil {
				// So, we are going to TRUNCATE, ie round down
				return int64(fv), true
				// However, some people might want a round function?
				// return int64(fv + .5), true
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
			rv := reflect.ValueOf(s)
			return convertToInt64(1, rv)
		}
	case reflect.Slice:
		if v.Len() > 0 {
			return ToInt64(v.Index(0))
		}
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

func marshalBool(v Value) ([]byte, error) {
	return json.Marshal(v.Value())
}
