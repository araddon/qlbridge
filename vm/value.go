package vm

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
)

var (
	//ReflectNilValue   = reflect.ValueOf((*interface{})(nil))
	//ReflectTrueValue  = reflect.ValueOf(true)
	//ReflectFalseValue = reflect.ValueOf(false)

	// our DataTypes we support, a limited sub-set of go
	floatRv   = reflect.ValueOf(float64(1.2))
	int64Rv   = reflect.ValueOf(int64(1))
	int32Rv   = reflect.ValueOf(int32(1))
	stringRv  = reflect.ValueOf("hello")
	stringsRv = reflect.ValueOf([]string{"hello"})
	boolRv    = reflect.ValueOf(true)
	mapIntRv  = reflect.ValueOf(map[string]int64{"hello": int64(1)})

	RV_ZERO   = reflect.Value{}
	nilStruct *emptyStruct

	BoolValueTrue    = NewBoolValue(true)
	BoolValueFalse   = NewBoolValue(false)
	NumberNaNValue   = NewNumberValue(math.NaN())
	EmptyStringValue = NewStringValue("")
	EmptyMapIntValue = NewMapIntValue(make(map[string]int64))
	NilStructValue   = NewStructValue(nilStruct)
)

type emptyStruct struct{}

type Value interface {
	//Type() reflect.Value
	Value() interface{}
	CanCoerce(rv reflect.Value) bool
	Rv() reflect.Value
	//String() string
}

type NumberValue struct {
	v  float64
	rv reflect.Value
}

func NewNumberValue(v float64) NumberValue {
	return NumberValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m NumberValue) Type() reflect.Value                 { return reflect.ValueOf(float64(0)) }
func (m NumberValue) Rv() reflect.Value                 { return m.rv }
func (m NumberValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(int64Rv, toRv) }
func (m NumberValue) Value() interface{}                { return m.v }
func (m NumberValue) MarshalJSON() ([]byte, error)      { return marshalFloat(float64(m.v)) }
func (m NumberValue) String() string                    { return strconv.FormatFloat(float64(m.v), 'f', -1, 64) }

type IntValue struct {
	v  int64
	rv reflect.Value
}

func NewIntValue(v int64) IntValue {
	return IntValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m IntValue) Type() reflect.Value                 { return int64Rv }
func (m IntValue) Rv() reflect.Value                 { return m.rv }
func (m IntValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(int64Rv, toRv) }
func (m IntValue) Value() interface{}                { return m.v }
func (m IntValue) MarshalJSON() ([]byte, error)      { return marshalFloat(float64(m.v)) }
func (m IntValue) NumberValue() NumberValue          { return NewNumberValue(float64(m.v)) }

type BoolValue struct {
	v  bool
	rv reflect.Value
}

func NewBoolValue(v bool) BoolValue {
	return BoolValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m BoolValue) Type() reflect.Value                 { return boolRv }
func (m BoolValue) Rv() reflect.Value                 { return m.rv }
func (m BoolValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(boolRv, toRv) }
func (m BoolValue) Value() interface{}                { return m.v }
func (m BoolValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }

type StringValue struct {
	v  string
	rv reflect.Value
}

func NewStringValue(v string) StringValue {
	return StringValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m StringValue) Type() reflect.Value                 { return stringRv }
func (m StringValue) Rv() reflect.Value                   { return m.rv }
func (m StringValue) CanCoerce(boolRv reflect.Value) bool { return CanCoerce(stringRv, boolRv) }
func (m StringValue) Value() interface{}                  { return m.v }
func (m StringValue) MarshalJSON() ([]byte, error)        { return json.Marshal(m.v) }
func (m StringValue) NumberValue() NumberValue            { return NewNumberValue(ToFloat64(m.Rv())) }
func (m StringValue) IntValue() IntValue {
	iv, _ := ToInt64(m.Rv())
	return NewIntValue(iv)
}

type StringsValue struct {
	v  []string
	rv reflect.Value
}

func NewStringsValue(v []string) StringsValue {
	return StringsValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m StringsValue) Type() reflect.Value                 { return stringRv }
func (m StringsValue) Rv() reflect.Value                   { return m.rv }
func (m StringsValue) CanCoerce(boolRv reflect.Value) bool { return CanCoerce(stringRv, boolRv) }
func (m StringsValue) Value() interface{}                  { return m.v }
func (m StringsValue) MarshalJSON() ([]byte, error)        { return json.Marshal(m.v) }
func (m StringsValue) NumberValue() NumberValue {
	if len(m.v) == 1 {
		if fv, err := strconv.ParseFloat(m.v[0], 64); err == nil {
			return NewNumberValue(fv)
		}
	}

	return NumberNaNValue
}
func (m StringsValue) IntValue() IntValue {
	// Im not confident this is valid?   array first element?
	iv, _ := ToInt64(m.Rv())
	return NewIntValue(iv)
}

type MapIntValue struct {
	v  map[string]int64
	rv reflect.Value
}

func NewMapIntValue(v map[string]int64) MapIntValue {
	return MapIntValue{v: v, rv: reflect.ValueOf(v)}
}

func (m MapIntValue) Rv() reflect.Value                 { return m.rv }
func (m MapIntValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapIntRv, toRv) }
func (m MapIntValue) Value() interface{}                { return m.v }
func (m MapIntValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }

//func (m MapIntValue) NumberValue() NumberValue          { return NewNumberValue(float64(m.Rv().Int())) }

type StructValue struct {
	v  interface{}
	rv reflect.Value
}

func NewStructValue(v interface{}) StructValue {
	return StructValue{v: v, rv: reflect.ValueOf(v)}
}

func (m StructValue) Rv() reflect.Value                 { return m.rv }
func (m StructValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m StructValue) Value() interface{}                { return m.v }
func (m StructValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
