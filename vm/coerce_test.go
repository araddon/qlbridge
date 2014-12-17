package vm

import (
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	//"reflect"
	"testing"
)

var _ = u.EMPTY

type coerceInts struct {
	v interface{}
	i int64
}

var float3pt1 = float64(3.1)
var intTests = []coerceInts{
	{int8(3), 3},
	{int(3), 3},
	{float32(3.1), 3},
	{float64(3.2), 3},
	{&float3pt1, 3},
	{"3", 3},
	{"3.1", 3},
	{"2.9", 2},         // do we really want this = 2?
	{[]string{"3"}, 3}, // again, do we really want this?
}

func TestCoerceInts(t *testing.T) {
	for _, cv := range intTests {
		v, err := ToValue(cv.v)
		assert.Tf(t, err == nil, "Nil err? %v", err)
		intVal, ok := ToInt64(v.Rv())
		assert.Tf(t, ok, "Should be ok: %#v   %v", cv, intVal)
		assert.Tf(t, intVal == cv.i, "should be == expect %v but was: %v  %#v", cv.i, intVal, cv)
	}
}

type coerceNumber struct {
	v interface{}
	f float64
}

var numberCoerceTests = []coerceNumber{
	{int8(3), 3},
	{int(3), 3},
	{float32(3.1), 3.1},
	{float64(3.2), 3.2},
	{&float3pt1, 3.1},
	{"3", 3},
	{"3.1", 3.1},
	{"2.9", 2.9},
}

// take two floats, compare, need to be within 1%
func CloseEnuf(a, b float64) bool {
	if a == b {
		return true
	}
	c := a / b
	if c > .99 && c < 1.01 {
		return true
	}
	return false
}

func TestCoerceNumbers(t *testing.T) {
	for _, cv := range numberCoerceTests {
		v, err := ToValue(cv.v)
		assert.Tf(t, err == nil, "Nil err? %v", err)
		floatVal := ToFloat64(v.Rv())
		//assert.Tf(t, ok, "Should be ok")
		assert.Tf(t, CloseEnuf(floatVal, cv.f), "should be == expect %v but was: %v", cv.f, floatVal)
	}
}
