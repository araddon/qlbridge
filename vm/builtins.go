package exprvm

import (
	u "github.com/araddon/gou"
	"reflect"
)

var _ = u.EMPTY

func Count(e *state, item string) int {
	return 1
}

func Eq(e *state, itemA, itemB Value) BoolValue {
	//return BoolValue(itemA == itemB)
	rvb := coerceTo(itemA.Rv(), itemB.Rv())
	a := itemA.Value()
	u.Infof("Eq():    a:%T  b:%T     %v=%v?", itemA, itemB, a, rvb)
	return NewBoolValue(reflect.DeepEqual(itemA.Rv(), rvb))
}

func ToInt(e *state, item Value) IntValue {
	return NewIntValue(toInt64(reflect.ValueOf(item.Value())))
	//return IntValue(2)
}
