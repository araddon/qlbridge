package exprvm

import (
	"reflect"
)

func Count(e *state, item string) int {
	return 1
}

func Eq(e *state, itema, itemB interface{}) BoolValue {
	return BoolValue(false)
}

func ToInt(e *state, item Value) IntValue {
	return IntValue(toInt64(reflect.ValueOf(item.Value())))
	//return IntValue(2)
}
