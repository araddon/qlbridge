package vm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
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
	timeRv    = reflect.ValueOf(time.Time{})
	nilRv     = reflect.ValueOf(nil)

	RV_ZERO     = reflect.Value{}
	nilStruct   *emptyStruct
	EmptyStruct = struct{}{}

	BoolValueTrue     = NewBoolValue(true)
	BoolValueFalse    = NewBoolValue(false)
	NumberNaNValue    = NewNumberValue(math.NaN())
	EmptyStringValue  = NewStringValue("")
	EmptyStringsValue = NewStringsValue(nil)
	EmptyMapIntValue  = NewMapIntValue(make(map[string]int64))
	NilStructValue    = NewStructValue(nilStruct)
	TimeZeroValue     = NewTimeValue(time.Time{})
	ErrValue          = NewErrorValue("")

	_ Value = (StringValue)(EmptyStringValue)
)

type emptyStruct struct{}

type Value interface {
	// Is this a nil?  or empty string?
	Nil() bool
	// Is this an error, or unable to evaluate from Vm?
	Err() bool
	Value() interface{}
	Rv() reflect.Value
	ToString() string
	//CanCoerce(rv reflect.Value) bool
}
type NumericValue interface {
	Float() float64
	Int() int64
}

type NumberValue struct {
	v  float64
	rv reflect.Value
}

func NewNumberValue(v float64) NumberValue {
	return NumberValue{v: v, rv: reflect.ValueOf(v)}
}

func (m NumberValue) Nil() bool                         { return false }
func (m NumberValue) Err() bool                         { return false }
func (m NumberValue) Rv() reflect.Value                 { return m.rv }
func (m NumberValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(int64Rv, toRv) }
func (m NumberValue) Value() interface{}                { return m.v }
func (m NumberValue) MarshalJSON() ([]byte, error)      { return marshalFloat(float64(m.v)) }
func (m NumberValue) ToString() string                  { return strconv.FormatFloat(float64(m.v), 'f', -1, 64) }
func (m NumberValue) Float() float64                    { return m.v }
func (m NumberValue) Int() int64                        { return int64(m.v) }

type IntValue struct {
	v  int64
	rv reflect.Value
}

func NewIntValue(v int64) IntValue {
	return IntValue{v: v, rv: reflect.ValueOf(v)}
}

func (m IntValue) Nil() bool                         { return false }
func (m IntValue) Err() bool                         { return false }
func (m IntValue) Rv() reflect.Value                 { return m.rv }
func (m IntValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(int64Rv, toRv) }
func (m IntValue) Value() interface{}                { return m.v }
func (m IntValue) MarshalJSON() ([]byte, error)      { return marshalFloat(float64(m.v)) }
func (m IntValue) NumberValue() NumberValue          { return NewNumberValue(float64(m.v)) }
func (m IntValue) ToString() string                  { return strconv.FormatInt(m.v, 10) }
func (m IntValue) Float() float64                    { return float64(m.v) }
func (m IntValue) Int() int64                        { return m.v }

type BoolValue struct {
	v  bool
	rv reflect.Value
}

func NewBoolValue(v bool) BoolValue {
	return BoolValue{v: v, rv: reflect.ValueOf(v)}
}

func (m BoolValue) Nil() bool                         { return false }
func (m BoolValue) Err() bool                         { return false }
func (m BoolValue) Rv() reflect.Value                 { return m.rv }
func (m BoolValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(boolRv, toRv) }
func (m BoolValue) Value() interface{}                { return m.v }
func (m BoolValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m BoolValue) ToString() string                  { return strconv.FormatBool(m.v) }

type StringValue struct {
	v  string
	rv reflect.Value
}

func NewStringValue(v string) StringValue {
	return StringValue{v: v, rv: reflect.ValueOf(v)}
}

func (m StringValue) Nil() bool                          { return len(m.v) == 0 }
func (m StringValue) Err() bool                          { return false }
func (m StringValue) Rv() reflect.Value                  { return m.rv }
func (m StringValue) CanCoerce(input reflect.Value) bool { return CanCoerce(stringRv, input) }
func (m StringValue) Value() interface{}                 { return m.v }
func (m StringValue) MarshalJSON() ([]byte, error)       { return json.Marshal(m.v) }
func (m StringValue) NumberValue() NumberValue           { return NewNumberValue(ToFloat64(m.Rv())) }
func (m StringValue) ToString() string                   { return m.v }

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

func (m StringsValue) Nil() bool                           { return len(m.v) == 0 }
func (m StringsValue) Err() bool                           { return false }
func (m StringsValue) Rv() reflect.Value                   { return m.rv }
func (m StringsValue) CanCoerce(boolRv reflect.Value) bool { return CanCoerce(stringRv, boolRv) }
func (m StringsValue) Value() interface{}                  { return m.v }
func (m *StringsValue) Append(sv string)                   { m.v = append(m.v, sv) }
func (m StringsValue) MarshalJSON() ([]byte, error)        { return json.Marshal(m.v) }
func (m StringsValue) Len() int                            { return len(m.v) }
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
func (m StringsValue) ToString() string  { return strings.Join(m.v, ",") }
func (m StringsValue) Strings() []string { return m.v }
func (m StringsValue) Set() map[string]struct{} {
	setvals := make(map[string]struct{})
	for _, sv := range m.v {
		// Are we sure about this ToLower?
		setvals[strings.ToLower(sv)] = EmptyStruct
	}
	return setvals
}

type MapIntValue struct {
	v  map[string]int64
	rv reflect.Value
}

func NewMapIntValue(v map[string]int64) MapIntValue {
	return MapIntValue{v: v, rv: reflect.ValueOf(v)}
}

func (m MapIntValue) Nil() bool                         { return len(m.v) == 0 }
func (m MapIntValue) Err() bool                         { return false }
func (m MapIntValue) Rv() reflect.Value                 { return m.rv }
func (m MapIntValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapIntRv, toRv) }
func (m MapIntValue) Value() interface{}                { return m.v }
func (m MapIntValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m MapIntValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }
func (m MapIntValue) MapInt() map[string]int64          { return m.v }

type StructValue struct {
	v  interface{}
	rv reflect.Value
}

func NewStructValue(v interface{}) StructValue {
	return StructValue{v: v, rv: reflect.ValueOf(v)}
}

func (m StructValue) Nil() bool                         { return false }
func (m StructValue) Err() bool                         { return false }
func (m StructValue) Rv() reflect.Value                 { return m.rv }
func (m StructValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m StructValue) Value() interface{}                { return m.v }
func (m StructValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m StructValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }

type TimeValue struct {
	t  time.Time
	rv reflect.Value
}

func NewTimeValue(t time.Time) TimeValue {
	return TimeValue{t: t, rv: reflect.ValueOf(t)}
}

func (m TimeValue) Nil() bool                         { return m.t.IsZero() }
func (m TimeValue) Err() bool                         { return false }
func (m TimeValue) Rv() reflect.Value                 { return m.rv }
func (m TimeValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(timeRv, toRv) }
func (m TimeValue) Value() interface{}                { return m.t }
func (m TimeValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.t) }
func (m TimeValue) ToString() string                  { return m.t.Format(time.RFC3339) }
func (m TimeValue) Time() time.Time                   { return m.t }

type ErrorValue struct {
	v  string
	rv reflect.Value
}

func NewErrorValue(v string) ErrorValue {
	return ErrorValue{v: v, rv: reflect.ValueOf(v)}
}

func (m ErrorValue) Nil() bool                         { return false }
func (m ErrorValue) Err() bool                         { return true }
func (m ErrorValue) Rv() reflect.Value                 { return m.rv }
func (m ErrorValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m ErrorValue) Value() interface{}                { return m.v }
func (m ErrorValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m ErrorValue) ToString() string                  { return "" }

type NilValue struct{}

func NewNilValue() NilValue {
	return NilValue{}
}

func (m NilValue) Nil() bool                         { return true }
func (m NilValue) Err() bool                         { return false }
func (m NilValue) Rv() reflect.Value                 { return nilRv }
func (m NilValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m NilValue) Value() interface{}                { return nil }
func (m NilValue) MarshalJSON() ([]byte, error)      { return nil, nil }
func (m NilValue) ToString() string                  { return "" }
