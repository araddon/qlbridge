package vm

import (
	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"reflect"
)

var _ = u.EMPTY

//
func Count(e *State, item Value) (IntValue, bool) {
	// TODO:  write to writeContext
	v, ok := ToInt64(item.Rv())
	return NewIntValue(v), ok
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(e *State, itemA, itemB Value) (BoolValue, bool) {
	//return BoolValue(itemA == itemB)
	rvb := CoerceTo(itemA.Rv(), itemB.Rv())
	//u.Infof("Eq():    a:%T  b:%T     %v=%v?", itemA, itemB, itemA.Value(), rvb)
	return NewBoolValue(reflect.DeepEqual(itemA.Rv(), rvb)), true
}

func ToInt(e *State, item Value) (IntValue, bool) {
	iv, _ := ToInt64(reflect.ValueOf(item.Value()))
	return NewIntValue(iv), true
	//return IntValue(2)
}
func Yy(e *State, item Value) (IntValue, bool) {

	v := ToString(item.Rv())
	//u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(v); err == nil {
		yy := t.Year()
		if yy >= 2000 {
			yy = yy - 2000
		} else if yy >= 1900 {
			yy = yy - 1900
		}
		//u.Infof("%v   yy = %v", item, yy)
		return NewIntValue(int64(yy)), true
	}

	return NewIntValue(0), false
	//return IntValue(2)
}
