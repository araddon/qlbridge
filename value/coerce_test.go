package value

import (
	"testing"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
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
		assert.True(t, err == nil, "Nil err? %v", err)
		intVal, ok := ToInt64(v.Rv())
		assert.True(t, ok, "Should be ok: %#v   %v", cv, intVal)
		assert.True(t, intVal == cv.i, "should be == expect %v but was: %v  %#v", cv.i, intVal, cv)
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
	{"$3.1", 3.1},
	{"$ 3.1", 3.1},
	{"3,222.11", 3222.11},
	{"$ 3,222.11", 3222.11},
	{"$3,222.11", 3222.11},
	{"£3,222.11", 3222.11},
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
		assert.True(t, err == nil, "Nil err? %v", err)
		floatVal, _ := ToFloat64(v.Rv())
		//assert.True(t, ok, "Should be ok")
		assert.True(t, CloseEnuf(floatVal, cv.f), "should be == expect %v but was: %v", cv.f, floatVal)
	}
}

func TestCoerceStrings(t *testing.T) {
	assert.Equal(t, IntType, ValueTypeFromString("1"))
	assert.Equal(t, IntType, ValueTypeFromString("0"))
	assert.Equal(t, IntType, ValueTypeFromString("100"))
	assert.Equal(t, IntType, ValueTypeFromString("-1"))
	assert.Equal(t, NumberType, ValueTypeFromString("1.499"))
	assert.Equal(t, NumberType, ValueTypeFromString("-1.499"))
	assert.Equal(t, BoolType, ValueTypeFromString("false"))
	assert.Equal(t, BoolType, ValueTypeFromString("false"))
	assert.Equal(t, TimeType, ValueTypeFromString("2017/07/07"))
	assert.Equal(t, StringType, ValueTypeFromString("hello"))

	// All may include JSON
	assert.Equal(t, IntType, ValueTypeFromStringAll("1"))
	assert.Equal(t, IntType, ValueTypeFromStringAll("0"))
	assert.Equal(t, IntType, ValueTypeFromStringAll("100"))
	assert.Equal(t, IntType, ValueTypeFromStringAll("-1"))
	assert.Equal(t, NumberType, ValueTypeFromStringAll("1.499"))
	assert.Equal(t, NumberType, ValueTypeFromStringAll("-1.499"))
	assert.Equal(t, BoolType, ValueTypeFromStringAll("false"))
	assert.Equal(t, BoolType, ValueTypeFromStringAll("false"))
	assert.Equal(t, TimeType, ValueTypeFromStringAll("2017/07/07"))
	assert.Equal(t, StringType, ValueTypeFromStringAll("hello"))
	assert.Equal(t, JsonType, ValueTypeFromStringAll(`{"name":"world"}`))
	assert.Equal(t, JsonType, ValueTypeFromStringAll(`["hello","world",1]`))
}

func TestCast(t *testing.T) {
	castGood := func(vt ValueType, v Value) interface{} {
		val, err := Cast(vt, v)
		assert.Equal(t, nil, err)
		return val.Value()
	}
	castBad := func(vt ValueType, v Value) {
		_, err := Cast(vt, v)
		assert.NotEqual(t, nil, err)
	}

	// String conversions
	assert.Equal(t, NewStringValue("hello").Value(), castGood(StringType, NewStringValue("hello")))
	assert.Equal(t, NewStringValue("100").Value(), castGood(StringType, NewIntValue(100)))
	assert.Equal(t, NewStringValue("100").Value(), castGood(StringType, NewTimeValue(dateparse.MustParse("2016/01/01"))))

	// Convert from ... to INT
	assert.Equal(t, NewIntValue(100).Value(), castGood(IntType, NewStringValue("100")))
	assert.Equal(t, NewIntValue(1451606400000).Value(), castGood(IntType, NewTimeValue(dateparse.MustParse("2016/01/01"))))

	castBad(BoolType, NewIntValue(500))
}
