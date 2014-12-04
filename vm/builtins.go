package vm

import (
	"github.com/araddon/dateparse"
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
func Yy(e *State, item Value) IntValue {
	// TODO:  date magic
	v := toString(item.Rv())
	if t, err := dateparse.ParseAny(v); err == nil {
		yy := t.Year()
		if yy >= 2000 {
			yy = yy - 2000
		} else if yy >= 1900 {
			yy = yy - 1900
		}
		//u.Infof("%v   yy = %v", item, yy)
		return NewIntValue(int64(yy))
	}

	return NewIntValue(0)
	//return IntValue(2)
}
