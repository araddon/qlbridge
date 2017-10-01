package value

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	"github.com/stretchr/testify/assert"
)

type vtest struct {
	in  interface{}
	out interface{}
	t   ValueType
	s   string
}

var (
	t1, _      = dateparse.ParseIn("2016/01/01", time.UTC)
	i8         = int8(5)
	i64        = int64(1500)
	nilInt64   int64
	nilFloat64 float64
	vals       = []vtest{
		{int64(1), int64(1), IntType, "1"},
		{uint8(1), int64(1), IntType, "1"},
		{&i8, int64(5), IntType, "5"},
		{&i64, int64(1500), IntType, "1500"},
		{1.55, float64(1.55), NumberType, "1.55"},
		{t1, t1, TimeType, "1451606400000"},
		{&t1, t1, TimeType, "1451606400000"},
		{"hello", "hello", StringType, "hello"},
		{[]string{"hello", "world"}, []string{"hello", "world"}, StringsType, "hello,world"},
		{[]interface{}{"hello", "world"}, []string{"hello", "world"}, StringsType, "hello,world"},
		{true, true, BoolType, "true"},
	}
	ms      = map[string]string{"k1": "hello", "k2": "world"}
	mv      = map[string]interface{}{"k1": "hello", "k2": "world"}
	mvout   = map[string]Value{"k1": NewStringValue("hello"), "k2": NewStringValue("world")}
	mi      = map[string]int64{"k1": 1, "k2": 2}
	mf      = map[string]float64{"k1": 1, "k2": 2}
	mt      = map[string]time.Time{"k1": t1, "k2": t1}
	mapVals = []vtest{
		{ms, ms, MapStringType, ""},
		{mv, mvout, MapValueType, ""},
		{mi, mi, MapIntType, ""},
		{mf, mf, MapNumberType, ""},
		{mt, mt, MapTimeType, ""},
	}
	nilvals = []vtest{
		{nil, nil, NilType, ""},
		// {&nilInt64, int64(0), IntType, ""},
		// {&nilFloat64, float64(0), NumberType, ""},
	}
)

// TestNewValue tests the conversion from Go types to Values.
func TestValues(t *testing.T) {
	for _, v := range vals {
		val := NewValue(v.in)
		t.Logf("check %v", val.ToString())
		assert.Equal(t, v.out, val.Value())
		assert.Equal(t, v.t, val.Type())
		assert.Equal(t, v.s, val.ToString())
		jb, err := json.Marshal(val)
		assert.Equal(t, nil, err)
		if len(v.s) > 0 {
			assert.True(t, len(jb) > 0)
		}
		switch v.t {
		case IntType, NumberType:
			numeric, isNumeric := val.(NumericValue)
			assert.True(t, isNumeric)
			// how to test these?
			numeric.Float()
			numeric.Int()
		}
		// this is definitely, definitely cheating
		assert.Equal(t, v.t.String(), val.Type().String())
		assert.Equal(t, ValueFromString(v.t.String()), v.t)
	}
	for _, v := range mapVals {
		val := NewValue(v.in)
		t.Logf("maps %v", val.ToString())
		mapVal, isMap := val.(Map)
		assert.True(t, isMap)
		assert.Equal(t, v.out, val.Value())
		assert.Equal(t, v.t, val.Type())
		jb, err := json.Marshal(val)
		assert.Equal(t, nil, err)
		if len(v.s) > 0 {
			assert.True(t, len(jb) > 0)
		}
		assert.Equal(t, 2, mapVal.Len())
		// Not deterministic on maps
		//assert.Equal(t, v.s, val.ToString())
		// this is definitely, definitely cheating
		assert.Equal(t, v.t.String(), val.Type().String())
		assert.Equal(t, ValueFromString(v.t.String()), v.t)
	}
	for _, v := range nilvals {
		val := NewValue(v.in)
		t.Logf("nil-check %v", val.ToString())
		assert.Equal(t, v.out, val.Value())
		assert.Equal(t, v.t, val.Type())
		assert.Equal(t, v.s, val.ToString())
		// this is definitely, definitely cheating
		assert.Equal(t, v.t.String(), val.Type().String())
		assert.Equal(t, true, val.Nil())
	}
}
