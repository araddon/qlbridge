package exprvm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
)

var _ = u.EMPTY

func canCoerce(from, to reflect.Value) bool {
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

func coerceTo(to, itemToConvert reflect.Value) reflect.Value {
	if to.Kind() == reflect.Interface {
		to = to.Elem()
	}
	if itemToConvert.Kind() == reflect.Interface {
		itemToConvert = itemToConvert.Elem()
	}

	switch to.Kind() {
	case reflect.Float32, reflect.Float64:
		return reflect.ValueOf(toFloat64(itemToConvert))

	case reflect.Int, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(toInt64(itemToConvert))
	case reflect.Bool:
		return reflect.ValueOf(toBool(itemToConvert))
	case reflect.String:
		return reflect.ValueOf(toString(itemToConvert))
	}
	return reflect.ValueOf("")
}

// toString convert all reflect.Value-s into string.
func toString(v reflect.Value) string {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() == reflect.String {
		return v.String()
	}
	if !v.IsValid() {
		return "nil"
	}
	return fmt.Sprint(v.Interface())
}

// toBool convert all reflect.Value-s into bool.
func toBool(v reflect.Value) bool {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float() != 0.0
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int() != 0
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		if v.String() == "true" {
			return true
		}
		if toInt64(v) != 0 {
			return true
		}
	}
	return false
}

// toFloat64 convert all reflect.Value-s into float64.
func toFloat64(v reflect.Value) float64 {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Int, reflect.Int32, reflect.Int64:
		return float64(v.Int())
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
			return float64(f)
		}
	default:
		u.Warnf("Cannot convert type?  %v", v.Kind())
	}
	return 0.0
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

// toInt64 convert all reflect.Value-s into int64.
func toInt64(v reflect.Value) int64 {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return int64(v.Float())
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.String:
		s := v.String()
		var i int64
		var err error
		if strings.HasPrefix(s, "0x") {
			i, err = strconv.ParseInt(s, 16, 64)
		} else {
			i, err = strconv.ParseInt(s, 10, 64)
		}
		if err == nil {
			return int64(i)
		}
	}
	return 0
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
