package exprvm

import (
	"encoding/json"
	// "fmt"
	// "math"
	"reflect"
	// "strconv"
	// "unicode/utf16"
)

var (
	NilValue   = reflect.ValueOf((*interface{})(nil))
	TrueValue  = reflect.ValueOf(true)
	FalseValue = reflect.ValueOf(false)

	// our DataTypes we support, a limited sub-set of go
	floatRv   = reflect.ValueOf(float64(1.2))
	int64Rv   = reflect.ValueOf(int64(1))
	int32Rv   = reflect.ValueOf(int32(1))
	stringRv  = reflect.ValueOf("hello")
	stringsRv = reflect.ValueOf([]string{"hello"})
	boolRv    = reflect.ValueOf(true)

	RV_ZERO = reflect.Value{}
)

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
func (m NumberValue) Rv() reflect.Value                   { return m.rv }
func (m NumberValue) CanCoerce(fromRv reflect.Value) bool { return canCoerce(fromRv, int64Rv) }
func (m NumberValue) Value() interface{}                  { return m.v }
func (m NumberValue) MarshalJSON() ([]byte, error)        { return marshalFloat(float64(m.v)) }

type IntValue struct {
	v  int64
	rv reflect.Value
}

func NewIntValue(v int64) IntValue {
	return IntValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m IntValue) Type() reflect.Value                 { return int64Rv }
func (m IntValue) Rv() reflect.Value                   { return m.rv }
func (m IntValue) CanCoerce(fromRv reflect.Value) bool { return canCoerce(fromRv, int64Rv) }
func (m IntValue) Value() interface{}                  { return m.v }
func (m IntValue) MarshalJSON() ([]byte, error)        { return marshalFloat(float64(m.v)) }

type BoolValue struct {
	v  bool
	rv reflect.Value
}

func NewBoolValue(v bool) BoolValue {
	return BoolValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m BoolValue) Type() reflect.Value                 { return boolRv }
func (m BoolValue) Rv() reflect.Value                   { return m.rv }
func (m BoolValue) CanCoerce(fromRv reflect.Value) bool { return canCoerce(fromRv, boolRv) }
func (m BoolValue) Value() interface{}                  { return m.v }
func (m BoolValue) MarshalJSON() ([]byte, error)        { return json.Marshal(m.v) }

type StringValue struct {
	v  string
	rv reflect.Value
}

func NewStringValue(v string) StringValue {
	return StringValue{v: v, rv: reflect.ValueOf(v)}
}

//func (m StringValue) Type() reflect.Value                 { return stringRv }
func (m StringValue) Rv() reflect.Value                   { return m.rv }
func (m StringValue) CanCoerce(fromRv reflect.Value) bool { return canCoerce(fromRv, stringRv) }
func (m StringValue) Value() interface{}                  { return m.v }
func (m StringValue) MarshalJSON() ([]byte, error)        { return json.Marshal(m.v) }
