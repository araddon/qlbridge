package vm

import (
	u "github.com/araddon/gou"
	"reflect"
)

var _ = u.EMPTY

//
func Count(e *State, item Value) IntValue {
	v := toInt64(item.Rv())
	return NewIntValue(v)
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(e *State, itemA, itemB Value) BoolValue {
	//return BoolValue(itemA == itemB)
	rvb := coerceTo(itemA.Rv(), itemB.Rv())
	a := itemA.Value()
	u.Infof("Eq():    a:%T  b:%T     %v=%v?", itemA, itemB, a, rvb)
	return NewBoolValue(reflect.DeepEqual(itemA.Rv(), rvb))
}

func ToInt(e *State, item Value) IntValue {
	return NewIntValue(toInt64(reflect.ValueOf(item.Value())))
	//return IntValue(2)
}
