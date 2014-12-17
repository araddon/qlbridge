package builtins

import (
	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/vm"
	"math"
	"reflect"
	"strings"
)

var _ = u.EMPTY

func LoadAllBuiltins() {
	vm.FuncAdd("gt", GtFunc)
	vm.FuncAdd("ge", GeFunc)
	vm.FuncAdd("eq", Eq)
	vm.FuncAdd("contains", ContainsFunc)
	vm.FuncAdd("toint", ToInt)
	vm.FuncAdd("count", Count)
	vm.FuncAdd("yy", Yy)
}

func GtFunc(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left := vm.ToFloat64(lv.Rv())
	right := vm.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return vm.BoolValueFalse, false
	}

	return vm.NewBoolValue(left > right), true
}

// >= GreaterThan or Equal
//  Must be able to convert items to Floats or else not ok
//
func GeFunc(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left := vm.ToFloat64(lv.Rv())
	right := vm.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return vm.BoolValueFalse, false
	}

	return vm.NewBoolValue(left >= right), true
}

func ContainsFunc(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left, right := vm.ToString(lv.Rv()), vm.ToString(rv.Rv())
	if left == "" || right == "" {
		return vm.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return vm.BoolValueTrue, true
	}
	return vm.BoolValueTrue, true
}

// Count
func Count(e *vm.State, item vm.Value) (vm.IntValue, bool) {
	// TODO:  we need a numeric and Int types that accept operands (+- etc)
	v, ok := vm.ToInt64(item.Rv())
	return vm.NewIntValue(v), ok
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(e *vm.State, itemA, itemB vm.Value) (vm.BoolValue, bool) {
	//return BoolValue(itemA == itemB)
	rvb := vm.CoerceTo(itemA.Rv(), itemB.Rv())
	switch rvb.Kind() {
	case reflect.String:
		return vm.NewBoolValue(rvb.String() == itemA.Rv().String()), true
	case reflect.Int64:
		return vm.NewBoolValue(rvb.Int() == itemA.Rv().Int()), true
	case reflect.Float64:
		return vm.NewBoolValue(rvb.Float() == itemA.Rv().Float()), true
	case reflect.Bool:
		return vm.NewBoolValue(rvb.Bool() == itemA.Rv().Bool()), true
	default:
		u.Warnf("Unknown kind?  %v", rvb.Kind())
	}
	u.Infof("Eq():    a:%T  b:%T     %v=%v? %v", itemA, itemB, itemA.Rv(), rvb, itemA.Rv() == rvb)
	return vm.NewBoolValue(reflect.DeepEqual(itemA.Rv(), rvb)), true
}

func ToInt(e *vm.State, item vm.Value) (vm.IntValue, bool) {
	iv, _ := vm.ToInt64(reflect.ValueOf(item.Value()))
	return vm.NewIntValue(iv), true
	//return IntValue(2)
}
func Yy(e *vm.State, item vm.Value) (vm.IntValue, bool) {

	v := vm.ToString(item.Rv())
	//u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(v); err == nil {
		yy := t.Year()
		if yy >= 2000 {
			yy = yy - 2000
		} else if yy >= 1900 {
			yy = yy - 1900
		}
		//u.Infof("%v   yy = %v", item, yy)
		return vm.NewIntValue(int64(yy)), true
	}

	return vm.NewIntValue(0), false
	//return IntValue(2)
}
