package vm

import (
	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"reflect"
)

var _ = u.EMPTY

//
func Count(e *State, item Value) IntValue {
	// TODO:  write to writeContext
	v := ToInt64(item.Rv())
	return NewIntValue(v)
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(e *State, itemA, itemB Value) BoolValue {
	//return BoolValue(itemA == itemB)
	rvb := CoerceTo(itemA.Rv(), itemB.Rv())
	a := itemA.Value()
	u.Infof("Eq():    a:%T  b:%T     %v=%v?", itemA, itemB, a, rvb)
	return NewBoolValue(reflect.DeepEqual(itemA.Rv(), rvb))
}

func ToInt(e *State, item Value) IntValue {
	return NewIntValue(ToInt64(reflect.ValueOf(item.Value())))
	//return IntValue(2)
}
func Yy(e *State, item Value) IntValue {

	v := ToString(item.Rv())
	u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(v); err == nil {
		yy := t.Year()
		if yy >= 2000 {
			yy = yy - 2000
		} else if yy >= 1900 {
			yy = yy - 1900
		}
		u.Infof("%v   yy = %v", item, yy)
		return NewIntValue(int64(yy))
	}

	return NewIntValue(0)
	//return IntValue(2)
}
