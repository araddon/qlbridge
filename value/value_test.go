package value

import (
	"encoding/json"
	"fmt"
	"math"
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
	te         = fmt.Errorf("err")
	t1, _      = dateparse.ParseIn("2016/01/01", time.UTC)
	i8         = int8(5)
	i8n        *int8
	ui8        = uint8(5)
	ui8n       *uint8
	i16n       *int16
	i16        = int16(16)
	i32n       *int32
	i32        = int32(32)
	iintn      *int
	iint       = int(15)
	ui32       = uint32(33)
	ui32n      *uint32
	i64        = int64(1500)
	i64n       *int64
	ui64       = uint64(65)
	ui64n      *uint64
	nilFloat64 *float64
	nilFloat32 *float32
	f32        = float32(1.56)
	f64        = float64(1.57)
	slv        = NewSliceValuesNative([]interface{}{"a", "b"})
	sliv       = NewSliceValuesNative([]interface{}{1, 2})
	slmv       = []interface{}{"a", 2}
	stv        = struct{ Name string }{Name: "world"}
	jval       = NewJsonValue(json.RawMessage(`{"name":"world"}`))
	vals       = []vtest{
		{uint8(5), int64(5), IntType, "5"},
		{&ui8, int64(5), IntType, "5"},
		{ui8n, int64(0), IntType, "0"},
		{&i8, int64(5), IntType, "5"},
		{i8n, int64(0), IntType, "0"},
		{i16n, int64(0), IntType, "0"},
		{int16(1), int64(1), IntType, "1"},
		{&i16, int64(16), IntType, "16"},
		{int(3), int64(3), IntType, "3"},
		{iintn, int64(0), IntType, "0"},
		{&iint, int64(15), IntType, "15"},
		{int32(32), int64(32), IntType, "32"},
		{&i32, int64(32), IntType, "32"},
		{i32n, int64(0), IntType, "0"},
		{ui32n, int64(0), IntType, "0"},
		{&ui32, int64(33), IntType, "33"},
		{uint32(33), int64(33), IntType, "33"},
		{int64(1), int64(1), IntType, "1"},
		{&i64, int64(1500), IntType, "1500"},
		{i64n, int64(0), IntType, "0"},
		{ui64n, int64(0), IntType, "0"},
		{&ui64, int64(65), IntType, "65"},
		{uint64(65), int64(65), IntType, "65"},
		{1.55, float64(1.55), NumberType, "1.55"},
		{&f32, float64(1.56), NumberType, "ignore"},
		{&f64, float64(1.57), NumberType, "1.57"},
		{nilFloat32, math.NaN(), NumberType, "NaN"},
		{nilFloat64, math.NaN(), NumberType, "NaN"},
		{t1, t1, TimeType, "1451606400000"},
		{&t1, t1, TimeType, "1451606400000"},
		{"hello", "hello", StringType, "hello"},
		{[]byte("hello"), []byte("hello"), ByteSliceType, "hello"},
		{[]string{"hello", "world"}, []string{"hello", "world"}, StringsType, "hello,world"},
		{[]interface{}{"hello", "world"}, []string{"hello", "world"}, StringsType, "hello,world"},
		{true, true, BoolType, "true"},
		{slv, slv.Val(), SliceValueType, "a,b"},
		{sliv, sliv.Val(), SliceValueType, "1,2"},
		{slmv, []Value{NewStringValue("a"), NewIntValue(2)}, SliceValueType, "a,2"},
		{[]interface{}{1, "b"}, []Value{NewIntValue(1), NewStringValue("b")}, SliceValueType, "1,b"},
		{stv, stv, StructType, "{world}"},
		{jval, jval.Val(), JsonType, `{"name":"world"}`},
	}
	ms      = map[string]string{"k1": "hello", "k2": "world"}
	mv      = map[string]interface{}{"k1": "hello", "k2": "world"}
	mvout   = map[string]Value{"k1": NewStringValue("hello"), "k2": NewStringValue("world")}
	mi      = map[string]int64{"k1": 1, "k2": 2}
	mint    = map[string]int{"k1": 1, "k2": 2}
	mf      = map[string]float64{"k1": 1, "k2": 2}
	mt      = map[string]time.Time{"k1": t1, "k2": t1}
	mb      = map[string]bool{"k1": true, "k2": false}
	mapVals = []vtest{
		{ms, ms, MapStringType, ""},
		{mv, mvout, MapValueType, ""},
		{mi, mi, MapIntType, ""},
		{mint, mi, MapIntType, ""},
		{mf, mf, MapNumberType, ""},
		{mt, mt, MapTimeType, ""},
		{mb, mb, MapBoolType, ""},
	}

	jrm       = json.RawMessage([]byte(`{"name":"world"}`))
	otherVals = []vtest{
		{nil, nil, NilType, ""},
		{te, te, ErrorType, "err"},
		{jrm, jrm, JsonType, `{"name":"world"}`},
	}
)

// TestNewValue tests the conversion from Go types to Values.
func TestValues(t *testing.T) {

	var ty ValueType
	assert.Equal(t, "nil", ty.String())
	assert.Equal(t, "unknown", UnknownType.String())
	assert.Equal(t, UnknownType, ValueFromString("unknown"))
	assert.Equal(t, UnknownType, ValueFromString("abracadabra"))
	assert.Equal(t, "invalid", ValueType(100).String())
	assert.Equal(t, "value", ValueInterfaceType.String())
	assert.Equal(t, ValueInterfaceType, ValueFromString("value"))

	for _, v := range vals {
		val := NewValue(v.in)
		t.Logf("check %T: %v", v.out, val.ToString())
		assert.Equal(t, v.t, val.Type())
		if v.s != "ignore" {
			assert.Equal(t, v.s, val.ToString())
			jb, err := json.Marshal(val)
			assert.Equal(t, nil, err)
			if len(v.s) > 0 {
				assert.True(t, len(jb) > 0)
			}
		}

		sliceVal, isSlice := val.(Slice)
		switch v.t {
		case IntType:
			numeric, isNumeric := val.(NumericValue)
			assert.True(t, isNumeric)
			assert.Equal(t, v.out, val.Value())
			assert.Equal(t, v.out, numeric.Int())
			assert.Equal(t, true, v.t.IsNumeric())
		case NumberType:
			numeric, isNumeric := val.(NumericValue)
			assert.True(t, isNumeric)
			assert.Equal(t, true, v.t.IsNumeric())
			if v.s == "NaN" {
				fv, ok := val.Value().(float64)
				assert.True(t, ok)
				assert.Equal(t, true, math.IsNaN(fv))
			} else if v.s == "ignore" {
			} else {
				assert.Equal(t, v.out, val.Value())
				assert.Equal(t, v.out, numeric.Float())
			}

		case StringsType:

			assert.True(t, isSlice)
			assert.True(t, v.t.IsSlice())
			assert.Equal(t, false, v.t.IsNumeric())
			assert.Equal(t, v.out, val.Value())
			sv := val.(StringsValue)
			assert.Equal(t, v.out, sv.Strings())
			assert.Equal(t, sv.Len(), sliceVal.Len())

		case ByteSliceType:
			byVal := val.(ByteSliceValue)
			assert.Equal(t, false, isSlice)
			// this is not a slice of []Values (because it is really a scalr?)
			assert.Equal(t, false, v.t.IsSlice())
			assert.Equal(t, false, v.t.IsNumeric())
			assert.Equal(t, v.out, val.Value())
			if v.out != nil {
				assert.True(t, byVal.Len() > 0)
				assert.Equal(t, false, byVal.Nil())
			}
		case SliceValueType:
			assert.Equal(t, true, isSlice)
			assert.Equal(t, v.out, val.Value())
			assert.Equal(t, true, v.t.IsSlice())
			assert.Equal(t, false, v.t.IsNumeric())
		case StructType:
			strval := val.(StructValue)
			assert.Equal(t, false, isSlice)
			assert.Equal(t, v.out, strval.Val())
			assert.Equal(t, v.out, strval.Value())
			assert.Equal(t, false, v.t.IsSlice())
			assert.Equal(t, false, v.t.IsNumeric())
		case JsonType:
			jval := val.(JsonValue)
			assert.Equal(t, false, jval.Nil())
			assert.Equal(t, false, jval.Err())
			assert.Equal(t, false, isSlice)
			assert.Equal(t, v.out, val.Value())
			assert.Equal(t, false, v.t.IsSlice())
			assert.Equal(t, false, v.t.IsNumeric())
		case TimeType:
			tval := val.(TimeValue)
			assert.Equal(t, false, isSlice)
			assert.Equal(t, v.out, tval.Val())
			assert.Equal(t, v.out, tval.Value())
			assert.Equal(t, v.out, tval.Time())
			assert.Equal(t, false, v.t.IsSlice())
			assert.Equal(t, false, v.t.IsNumeric())
			assert.True(t, tval.Float() > float64(1000))
		default:
			assert.Equal(t, v.out, val.Value())
		}

		// this is definitely, definitely cheating
		assert.Equal(t, v.t.String(), val.Type().String())
		assert.Equal(t, ValueFromString(v.t.String()), v.t)
		assert.Equal(t, false, v.t.IsMap())
	}
	for _, v := range mapVals {
		val := NewValue(v.in)
		t.Logf("maps %v", val.ToString())
		mapVal, isMap := val.(Map)
		assert.True(t, isMap)
		assert.Equal(t, v.out, val.Value())
		assert.Equal(t, v.t, val.Type())
		assert.Equal(t, false, v.t.IsSlice())
		assert.Equal(t, false, v.t.IsNumeric())
		jb, err := json.Marshal(val)
		assert.Equal(t, nil, err)
		if len(v.s) > 0 {
			assert.True(t, len(jb) > 0)
		}
		assert.Equal(t, 2, mapVal.Len())
		assert.Equal(t, false, val.Nil())
		assert.Equal(t, false, val.Err())
		// Not deterministic on maps
		//assert.Equal(t, v.s, val.ToString())
		// this is definitely, definitely cheating
		assert.Equal(t, v.t.String(), val.Type().String())
		assert.Equal(t, ValueFromString(v.t.String()), v.t)
		assert.True(t, v.t.IsMap())
		switch mvt := val.(type) {
		case MapValue:
			assert.Equal(t, 2, len(mvt.Val()))
		}
	}
	for _, v := range otherVals {
		val := NewValue(v.in)
		t.Logf("value-check %T: %v", v.in, val.ToString())
		assert.Equal(t, v.out, val.Value(), "for %+v", v)
		assert.Equal(t, v.t, val.Type(), "for %+v", v)
		assert.Equal(t, v.s, val.ToString(), "for %+v", v)
		// this is definitely, definitely cheating
		assert.Equal(t, v.t.String(), val.Type().String(), "for %#v", v)
		assert.Equal(t, ValueFromString(v.t.String()), v.t)
		switch v.t {
		case NilType:
			nval := val.(NilValue)
			assert.Equal(t, true, val.Nil())
			assert.Equal(t, nil, nval.Val())
			_, err := json.Marshal(nval)
			assert.Equal(t, nil, err)
		}

	}
}
func TestIntValue(t *testing.T) {
	v := NewIntNil()
	assert.True(t, v.Nil())
	assert.Equal(t, "", v.ToString())
	v = NewIntValue(32)
	nv := v.NumberValue()
	assert.Equal(t, nv.Int(), int64(32))
}
func TestValueNumber(t *testing.T) {
	v := NewNumberValue(math.NaN())
	_, err := json.Marshal(&v)
	assert.Equal(t, nil, err)
	v = NewNumberValue(math.Inf(1))
	_, err = json.Marshal(&v)
	assert.Equal(t, nil, err)
	v = NewNumberValue(math.Inf(-1))
	_, err = json.Marshal(&v)
	assert.Equal(t, nil, err)

	sv := NewStringValue("25.5")
	nv := sv.NumberValue()
	assert.True(t, CloseEnuf(nv.Float(), float64(25.5)))
}
func TestString(t *testing.T) {
	v := NewStringValue("a")
	slv := v.StringsValue()
	assert.Equal(t, 1, slv.Len())
	assert.Equal(t, "a", slv.Val()[0])

	v = NewStringValue("15.3")
	assert.Equal(t, int64(15), v.IntValue().Val())
	v = NewStringValue("15")
	assert.Equal(t, int64(15), v.IntValue().Val())
}
func TestErrValue(t *testing.T) {
	v := NewErrorValuef("%v", "damn")
	assert.Equal(t, false, v.Nil())
	assert.Equal(t, "damn", v.ToString())
	assert.Equal(t, "damn", v.Val().Error())
	assert.Equal(t, "damn", v.Error())
	_, err := v.MarshalJSON()
	assert.Equal(t, nil, err)
	assert.True(t, v.Err())
}
func TestStrings(t *testing.T) {
	v := NewStringsValue([]string{"a"})
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, "a", v.Val()[0])
	v.Append("b")
	assert.Equal(t, 2, v.Len())
	assert.Equal(t, "b", v.Val()[1])
	v = NewStringsValue([]string{"25.1"})
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, float64(25.1), v.NumberValue().Float())
	assert.Equal(t, int64(25), v.IntValue().Int())
	v.Append("b")
	assert.Equal(t, float64(25.1), v.NumberValue().Float())
	assert.Equal(t, int64(25), v.IntValue().Int())
	v = NewStringsValue(nil)
	assert.True(t, math.IsNaN(v.NumberValue().Float()))
	assert.Equal(t, int64(0), v.IntValue().Int())

	v.Append("a")
	v.Append("a")
	m := v.Set()
	assert.Equal(t, 1, len(m))
}
func TestSliceValues(t *testing.T) {
	v := NewSliceValuesNative([]interface{}{"a"})
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, "a", v.Val()[0].ToString())
	v.Append(NewStringValue("b"))
	assert.Equal(t, 2, v.Len())
	assert.Equal(t, "b", v.Val()[1].ToString())
	assert.Equal(t, 2, len(v.Values()))
}
func TestMapValue(t *testing.T) {
	mv := map[string]interface{}{"k1": 10}
	v := NewMapValue(mv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.Equal(t, int64(10), mi["k1"])
	mf := v.MapFloat()
	assert.Equal(t, 1, len(mf))
	assert.Equal(t, float64(10), mf["k1"])
	ms := v.MapString()
	assert.Equal(t, 1, len(mf))
	assert.Equal(t, "10", ms["k1"])
	v = NewMapValue(map[string]interface{}{"k1": "hello"})
	mt := v.MapTime()
	assert.Equal(t, 0, mt.Len())
	v = NewMapValue(map[string]interface{}{"k1": "now-4d"})
	mt = v.MapTime()
	assert.Equal(t, 1, mt.Len())
	assert.True(t, mt.Val()["k1"].Unix() > 10000)
}

func TestMapStringValue(t *testing.T) {
	msv := map[string]string{"k1": "10"}
	v := NewMapStringValue(msv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))
	mi := v.MapInt()
	assert.Equal(t, 1, mi.Len())
	assert.Equal(t, int64(10), mi.Val()["k1"])
	mf := v.MapNumber()
	assert.Equal(t, 1, mf.Len())
	assert.Equal(t, float64(10), mf.Val()["k1"])
	mb := v.MapBool()
	assert.Equal(t, 0, mb.Len())
	v = NewMapStringValue(map[string]string{"k1": "true"})
	mb = v.MapBool()
	assert.Equal(t, 1, mb.Len())
	assert.Equal(t, true, mb.Val()["k1"])
}

func TestMapIntValue(t *testing.T) {
	miv := map[string]int64{"k1": 10}
	v := NewMapIntValue(miv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))
	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.Equal(t, int64(10), mi["k1"])
	mf := v.MapFloat()
	assert.Equal(t, 1, len(mf))
	assert.Equal(t, float64(10), mf["k1"])

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, int64(10), mv.Val()["k1"].Value())
}
func TestMapNumberValue(t *testing.T) {
	mfv := map[string]float64{"k1": 10}
	v := NewMapNumberValue(mfv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))
	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.Equal(t, int64(10), mi["k1"])

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, float64(10), mv.Val()["k1"].Value())
}
func TestMapTimeValue(t *testing.T) {
	n := time.Now()
	mtv := map[string]time.Time{"k1": n}
	v := NewMapTimeValue(mtv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())

	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.True(t, CloseEnuf(float64(n.UnixNano()), float64(mi["k1"])))

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, n, mv.Val()["k1"].Value())
}
func TestMapBoolValue(t *testing.T) {

	mbv := map[string]bool{"k1": true}
	v := NewMapBoolValue(mbv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, true, mv.Val()["k1"].Value())
}
