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

var nilVal = func() Value { return nil }()
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

	_, ok := ValueToInt64(NewStringValue("0x22"))
	assert.True(t, !ok, "Should not be ok")

	_, ok = ValueToInt64(NewStringValue("22.22.22"))
	assert.True(t, !ok, "Should not be ok")
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
	if c > .999 && c < 1.001 {
		return true
	}
	return false
}
func CloseEnuft(a, b time.Time) bool {
	ai, bi := a.Unix(), b.Unix()
	if ai == bi {
		return true
	}
	if ai == 0 || bi == 0 {
		return false
	}
	c := float64(ai) / float64(bi)
	if c > .999 && c < 1.001 {
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
	// note, we are not using time.Local = UTC which behaves a little weird
	// so setting time.Local to known, controlled zone
	denverLoc, err := time.LoadLocation("America/Denver")
	assert.Equal(t, nil, err)
	// Start out with mountain time US
	time.Local = denverLoc
	t1, _ := dateparse.ParseLocal("2016/01/01")
	good(dateparse.MustParse("2016/01/01"), TimeType, NewTimeValue(dateparse.MustParse("2016/01/01")))
	good(dateparse.MustParse("2016/01/01"), TimeType, NewStringValue("2016/01/01"))
	good(t1, TimeType, NewIntValue(int64(t1.Unix())))

	// Convert from ... to INT
	good(int64(100), IntType, NewStringValue("100"))
	good(int64(100), IntType, NewNumberValue(100.01))
	good(int64(1451606400000), IntType, NewTimeValue(dateparse.MustParse("2016/01/01")))
	good(int64(100), IntType, NewStringsValue([]string{"100"}))
	iv, _ := ValueToInt(NewIntValue(100))
	assert.Equal(t, int(100), iv)

	castBad(BoolType, NewIntValue(500))
	castBad(TimeType, NewStringValue("hello"))
	castBad(IntType, NewStringValue("hello"))
	castBad(IntType, NewStringValue(""))
	castBad(IntType, NewStructValue(struct{ Name string }{Name: "world"}))
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
	notString(NewSliceValues([]Value{nilVal, NewStringValue("hello")}))

	// String conversions
	good("hello", NewStringValue("hello"))
	good("100", NewIntValue(100))
	good("1.5", NewNumberValue(1.5))
	notString(NewNilValue())
	notString(NewMapIntValue(nil))
	notString(nilVal)

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

func TestValueToStrings(t *testing.T) {
	good := func(expect string, v Value) {
		vals, ok := ValueToStrings(v)
		assert.Equal(t, true, ok)
		assert.Equal(t, expect, vals[0])
	}
	ne := func(expect string, v Value) {
		vals, ok := ValueToStrings(v)
		assert.Equal(t, true, ok)
		assert.NotEqual(t, expect, vals[0])
	}
	notString := func(v Value) {
		_, ok := ValueToStrings(v)
		assert.Equal(t, false, ok)
	}

	notString(NewStructValue(struct{ Name string }{Name: "world"}))

	// String conversions
	good("hello", NewStringValue("hello"))
	good("100", NewIntValue(100))
	good("1.5", NewNumberValue(1.5))
	notString(NewNilValue())
	notString(NewMapIntValue(nil))
	notString(nilVal)

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

func TestValueToTime(t *testing.T) {

	good := func(expect time.Time, v Value) {
		val, ok := ValueToTime(v)
		assert.Equal(t, true, ok)
		assert.True(t, CloseEnuft(expect, val))
	}
	ne := func(expect time.Time, v Value) {
		val, ok := ValueToTime(v)
		assert.Equal(t, true, ok)
		assert.True(t, CloseEnuft(expect, val))
	}
	notTime := func(v Value) {
		_, ok := ValueToTime(v)
		assert.Equal(t, false, ok)
	}

	notTime(NewStructValue(struct{ Name string }{Name: "world"}))
	notTime(NewSliceValues([]Value{nilVal, NewStringValue("hello")}))
	notTime(NewStringValue("hello"))

	good(time.Now().Add(time.Hour*24), NewStringValue("now+1d"))
	good(time.Now().Add(time.Hour*24), NewStringsValue([]string{"now+1d"}))
	ne(time.Now(), NewStringValue("now-3d"))
	notTime(NewStringValue(""))
	notTime(NewStringValue("now-4x"))
	notTime(NewIntValue(-11))
	notTime(NewIntValue(9332151919))

	// good(float64(1), NewStringValue("12345"))
	// good(float64(0), NewBoolValue(false))
}

func TestValueToFloat(t *testing.T) {
	good := func(expect float64, v Value) {
		val, ok := ValueToFloat64(v)
		assert.Equal(t, true, ok)
		assert.Equal(t, expect, val)
	}
	ne := func(expect float64, v Value) {
		val, ok := ValueToFloat64(v)
		assert.Equal(t, true, ok)
		assert.NotEqual(t, expect, val)
	}
	notFloat := func(v Value) {
		_, ok := ValueToFloat64(v)
		assert.Equal(t, false, ok)
	}

	notFloat(NewStructValue(struct{ Name string }{Name: "world"}))
	notFloat(NewSliceValues([]Value{nilVal, NewStringValue("hello")}))

	good(22.5, NewNumberValue(22.5))
	ne(22.5, NewNumberValue(22))
	notFloat(NewStringValue(""))
	good(float64(1), NewBoolValue(true))
	good(float64(0), NewBoolValue(false))

	_, ok := StringToFloat64("")
	assert.Equal(t, false, ok)
	_, ok = StringToFloat64("$no")
	assert.Equal(t, false, ok)
}

func TestIsBool(t *testing.T) {
	assert.Equal(t, false, IsBool("hello"))
	assert.Equal(t, true, IsBool("true"))
	assert.Equal(t, true, BoolStringVal("true"))
	assert.Equal(t, true, BoolStringVal("t"))
	assert.Equal(t, false, BoolStringVal("not bool"))

	_, ok := ValueToBool(NewStringValue(""))
	assert.Equal(t, false, ok)
	b, ok := ValueToBool(NewIntValue(0))
	assert.Equal(t, true, ok)
	assert.Equal(t, false, b)
	b, ok = ValueToBool(NewIntValue(1))
	assert.Equal(t, true, ok)
	assert.Equal(t, true, b)
	b, ok = ValueToBool(NewIntValue(19))
	assert.Equal(t, false, ok)
	assert.Equal(t, false, b)
	b, ok = ValueToBool(NewStringValue("hello"))
	assert.Equal(t, false, ok)
	assert.Equal(t, false, b)
	b, ok = ValueToBool(NewStringValue("true"))
	assert.Equal(t, true, ok)
	assert.Equal(t, true, b)
	b, ok = ValueToBool(NewStringValue("false"))
	assert.Equal(t, true, ok)
	assert.Equal(t, false, b)
	b, ok = ValueToBool(NewStringsValue([]string{"false"}))
	assert.Equal(t, false, ok)
	assert.Equal(t, false, b)
}
