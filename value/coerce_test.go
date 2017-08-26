package value

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	"github.com/stretchr/testify/assert"
)

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
		v := NewValue(cv.v)
		intVal, ok := ValueToInt64(v)
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
		v := NewValue(cv.v)
		floatVal, ok := ValueToFloat64(v)
		assert.True(t, ok, "Should be ok")
		assert.True(t, CloseEnuf(floatVal, cv.f), "should be == expect %v but was: %v", cv.f, floatVal)
	}
}

func TestValueTypeFromString(t *testing.T) {
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
	good := func(expect interface{}, vt ValueType, v Value) {
		val, err := Cast(vt, v)
		assert.Equal(t, nil, err)
		assert.Equal(t, expect, val.Value())
	}
	castBad := func(vt ValueType, v Value) {
		_, err := Cast(vt, v)
		assert.NotEqual(t, nil, err)
	}

	// String conversions
	good("hello", StringType, NewStringValue("hello"))
	good("100", StringType, NewIntValue(100))
	good("1.5", StringType, NewNumberValue(1.5))
	good("1451606400000", StringType, NewTimeValue(dateparse.MustParse("2016/01/01")))

	// Bytes
	good([]byte("hello"), ByteSliceType, NewByteSliceValue([]byte("hello")))
	good([]byte("hello"), ByteSliceType, NewStringValue("hello"))

	// time
	time.Local = time.UTC
	t1 := dateparse.MustParse("2016/01/01")
	good(dateparse.MustParse("2016/01/01"), TimeType, NewTimeValue(dateparse.MustParse("2016/01/01")))
	good(dateparse.MustParse("2016/01/01"), TimeType, NewStringValue("2016/01/01"))
	good(t1, TimeType, NewIntValue(int64(t1.Unix())))

	// Convert from ... to INT
	good(int64(100), IntType, NewStringValue("100"))
	good(int64(100), IntType, NewNumberValue(100.01))
	good(int64(1451606400000), IntType, NewTimeValue(dateparse.MustParse("2016/01/01")))
	good(int64(100), IntType, NewStringsValue([]string{"100"}))

	castBad(BoolType, NewIntValue(500))
	castBad(TimeType, NewStringValue("hello"))
	castBad(IntType, NewStringValue("hello"))
}

func TestEqual(t *testing.T) {
	good := func(l, r Value) {
		eq, err := Equal(l, r)
		assert.Equal(t, nil, err)
		assert.True(t, eq)
	}
	notEqual := func(l, r Value) {
		eq, err := Equal(l, r)
		assert.Equal(t, nil, err)
		assert.Equal(t, false, eq)
	}
	hasErr := func(l, r Value) {
		_, err := Equal(l, r)
		assert.NotEqual(t, nil, err)
	}

	good(nil, nil)
	good(nil, NewNilValue())
	good(NewNilValue(), nil)
	good(NewNilValue(), NewNilValue())

	notEqual(NewStringValue("hello"), nil)
	notEqual(nil, NewStringValue("hello"))
	notEqual(NewStringValue("hello"), NewNilValue())

	// String conversions
	good(NewStringValue("hello"), NewStringValue("hello"))
	good(NewStringValue("hello"), NewByteSliceValue([]byte("hello")))
	notEqual(NewStringValue("hello"), NewStringValue("hellox"))
	hasErr(NewJsonValue(json.RawMessage(`{"a":"hello"}`)), NewStringValue("hello"))

	good(NewIntValue(500), NewIntValue(500))
	good(NewNumberValue(500), NewIntValue(500))
	notEqual(NewIntValue(500), NewIntValue(89))

	good(NewNumberValue(500), NewNumberValue(500))
	good(NewNumberValue(500), NewIntValue(500))
	notEqual(NewNumberValue(500), NewIntValue(89))

	good(NewBoolValue(true), NewBoolValue(true))
	good(NewBoolValue(true), NewIntValue(1))
	good(NewBoolValue(true), NewStringValue("true"))
	notEqual(NewBoolValue(true), NewBoolValue(false))

	t1, _ := dateparse.ParseIn("2016/01/01", time.UTC)
	good(NewTimeValue(t1), NewTimeValue(t1))

	good(NewStringsValue([]string{"100"}), NewStringsValue([]string{"100"}))
	good(NewStringsValue([]string{"100", "200"}), NewStringsValue([]string{"100", "200"}))
	notEqual(NewStringsValue([]string{"100", "xxxxx"}), NewStringsValue([]string{"100", "200"}))
	notEqual(NewStringsValue([]string{"100"}), NewStringsValue([]string{"100", "200"}))
}

func TestValueToString(t *testing.T) {
	good := func(expect string, v Value) {
		val, ok := ValueToString(v)
		assert.Equal(t, true, ok)
		assert.Equal(t, expect, val)
	}
	ne := func(expect string, v Value) {
		val, ok := ValueToString(v)
		assert.Equal(t, true, ok)
		assert.NotEqual(t, expect, val)
	}
	notString := func(v Value) {
		_, ok := ValueToString(v)
		assert.Equal(t, false, ok)
	}

	notString(NewStructValue(struct{ Name string }{Name: "world"}))

	// String conversions
	good("hello", NewStringValue("hello"))
	good("100", NewIntValue(100))
	good("1.5", NewNumberValue(1.5))
	notString(NewNilValue())
	notString(NewMapIntValue(nil))

	// I really hate this decision, need to find usage and
	// see if i can back it out/change it
	good("100", NewStringsValue([]string{"100"}))
	good("100", NewStringsValue([]string{"100", "200"}))
	good("100", NewStringsValue([]string{"100"}))
	ne("100", NewStringsValue([]string{"200"}))
	notString(NewStringsValue(nil))

	t1, _ := dateparse.ParseIn("2016/01/01", time.UTC)
	good("2016-01-01 00:00:00 +0000 UTC", NewTimeValue(t1))
	good("1451606400000", NewIntValue(1451606400000))

	good("hello", NewByteSliceValue([]byte("hello")))
}

func TestIsBool(t *testing.T) {
	assert.Equal(t, false, IsBool("hello"))
	assert.Equal(t, true, IsBool("true"))
	assert.Equal(t, true, BoolStringVal("true"))
	assert.Equal(t, true, BoolStringVal("t"))
	assert.Equal(t, false, BoolStringVal("not bool"))
}
