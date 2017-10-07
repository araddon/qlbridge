// Value package defines the core value types (string, int, etc) for the
// qlbridge package, mostly used to provide common interfaces instead
// of reflection for virtual machine.
package value

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
)

var (
	_ = u.EMPTY

	nilStruct   *emptyStruct
	EmptyStruct = struct{}{}

	NilValueVal         = NewNilValue()
	BoolValueTrue       = BoolValue{v: true}
	BoolValueFalse      = BoolValue{v: false}
	NumberNaNValue      = NewNumberValue(math.NaN())
	EmptyStringValue    = NewStringValue("")
	EmptyStringsValue   = NewStringsValue(nil)
	EmptyMapValue       = NewMapValue(nil)
	EmptyMapStringValue = NewMapStringValue(make(map[string]string))
	EmptyMapIntValue    = NewMapIntValue(make(map[string]int64))
	EmptyMapNumberValue = NewMapNumberValue(make(map[string]float64))
	EmptyMapTimeValue   = NewMapTimeValue(make(map[string]time.Time))
	EmptyMapBoolValue   = NewMapBoolValue(make(map[string]bool))
	NilStructValue      = NewStructValue(nilStruct)
	TimeZeroValue       = NewTimeValue(time.Time{})
	ErrValue            = NewErrorValue(fmt.Errorf(""))

	_ Value = (StringValue)(EmptyStringValue)

	// force some types to implement interfaces
	_ Slice = (*StringsValue)(nil)
	_ Slice = (*SliceValue)(nil)
	_ Map   = (MapValue)(EmptyMapValue)
	_ Map   = (MapIntValue)(EmptyMapIntValue)
	_ Map   = (MapStringValue)(EmptyMapStringValue)
	_ Map   = (MapNumberValue)(EmptyMapNumberValue)
	_ Map   = (MapTimeValue)(EmptyMapTimeValue)
	_ Map   = (MapBoolValue)(EmptyMapBoolValue)
)

// This is the DataType system, ie string, int, etc
type ValueType uint8

const (
	// Enum values for Type system, DO NOT CHANGE the numbers, do not use iota
	NilType            ValueType = 0
	ErrorType          ValueType = 1
	UnknownType        ValueType = 2
	ValueInterfaceType ValueType = 3 // Is of type Value Interface, ie unknown
	NumberType         ValueType = 10
	IntType            ValueType = 11
	BoolType           ValueType = 12
	TimeType           ValueType = 13
	ByteSliceType      ValueType = 14
	StringType         ValueType = 20
	StringsType        ValueType = 21
	MapValueType       ValueType = 30
	MapIntType         ValueType = 31
	MapStringType      ValueType = 32
	MapNumberType      ValueType = 33
	MapBoolType        ValueType = 34
	MapTimeType        ValueType = 35
	SliceValueType     ValueType = 40
	StructType         ValueType = 50
	JsonType           ValueType = 51
)

func (m ValueType) String() string {
	switch m {
	case NilType:
		return "nil"
	case ErrorType:
		return "error"
	case UnknownType:
		return "unknown"
	case ValueInterfaceType:
		return "value"
	case NumberType:
		return "number"
	case IntType:
		return "int"
	case BoolType:
		return "bool"
	case TimeType:
		return "time"
	case ByteSliceType:
		return "[]byte"
	case StringType:
		return "string"
	case StringsType:
		return "[]string"
	case MapValueType:
		return "map[string]value"
	case MapIntType:
		return "map[string]int"
	case MapStringType:
		return "map[string]string"
	case MapNumberType:
		return "map[string]number"
	case MapTimeType:
		return "map[string]time"
	case MapBoolType:
		return "map[string]bool"
	case SliceValueType:
		return "[]value"
	case StructType:
		return "struct"
	case JsonType:
		return "json"
	default:
		return "invalid"
	}
}

func (m ValueType) IsMap() bool {
	switch m {
	case MapValueType, MapIntType, MapStringType, MapNumberType, MapTimeType, MapBoolType:
		return true
	}
	return false
}

func (m ValueType) IsSlice() bool {
	switch m {
	case StringsType, SliceValueType:
		return true
	}
	return false
}

func (m ValueType) IsNumeric() bool {
	switch m {
	case NumberType, IntType:
		return true
	}
	return false
}

type emptyStruct struct{}

type (
	Value interface {
		// Is this a nil/empty?
		// empty string counts as nil, empty slices/maps, nil structs.
		Nil() bool
		// Is this an error, or unable to evaluate from Vm?
		Err() bool
		Value() interface{}
		ToString() string
		Type() ValueType
	}
	// Certain types are Numeric (Ints, Time, Number)
	NumericValue interface {
		Float() float64
		Int() int64
	}
	// Slices can always return a []Value representation and is meant to be used
	// when iterating over all items in a non-scalar value. Maps return their keys
	// as a slice.
	Slice interface {
		SliceValue() []Value
		Len() int
		json.Marshaler
	}
	// Map interface
	Map interface {
		json.Marshaler
		Len() int
		MapValue() MapValue
		Get(key string) (Value, bool)
	}
)

type (
	NumberValue struct {
		v float64
	}
	IntValue struct {
		v int64
	}
	BoolValue struct {
		v bool
	}
	StringValue struct {
		v string
	}
	TimeValue struct {
		v time.Time
	}
	StringsValue struct {
		v []string
	}
	ByteSliceValue struct {
		v []byte
	}
	SliceValue struct {
		v []Value
	}
	MapValue struct {
		v map[string]Value
	}
	MapIntValue struct {
		v map[string]int64
	}
	MapNumberValue struct {
		v map[string]float64
	}
	MapStringValue struct {
		v map[string]string
	}
	MapBoolValue struct {
		v map[string]bool
	}
	MapTimeValue struct {
		v map[string]time.Time
	}
	StructValue struct {
		v interface{}
	}
	JsonValue struct {
		v json.RawMessage
	}
	ErrorValue struct {
		v error
	}
	NilValue struct{}
)

// ValueFromString Given a string, convert to valuetype
func ValueFromString(vt string) ValueType {
	switch vt {
	case "nil", "null":
		return NilType
	case "error":
		return ErrorType
	case "unknown":
		return UnknownType
	case "value":
		return ValueInterfaceType
	case "number":
		return NumberType
	case "int":
		return IntType
	case "bool":
		return BoolType
	case "time":
		return TimeType
	case "[]byte":
		return ByteSliceType
	case "string":
		return StringType
	case "[]string":
		return StringsType
	case "map[string]value":
		return MapValueType
	case "map[string]int":
		return MapIntType
	case "map[string]string":
		return MapStringType
	case "map[string]number":
		return MapNumberType
	case "map[string]bool":
		return MapBoolType
	case "map[string]time":
		return MapTimeType
	case "[]value":
		return SliceValueType
	case "struct":
		return StructType
	case "json":
		return JsonType
	default:
		return UnknownType
	}
}

// NewValue creates a new Value type from a native Go value.
//
// Defaults to StructValue for unknown types.
func NewValue(goVal interface{}) Value {

	switch val := goVal.(type) {
	case nil:
		return NilValueVal
	case Value:
		return val
	case float64:
		return NewNumberValue(val)
	case float32:
		return NewNumberValue(float64(val))
	case *float64:
		if val == nil {
			return NewNumberNil()
		}
		return NewNumberValue(*val)
	case *float32:
		if val == nil {
			return NewNumberNil()
		}
		return NewNumberValue(float64(*val))
	case int8:
		return NewIntValue(int64(val))
	case *int8:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int16:
		return NewIntValue(int64(val))
	case *int16:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int:
		return NewIntValue(int64(val))
	case *int:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int32:
		return NewIntValue(int64(val))
	case *int32:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int64:
		return NewIntValue(int64(val))
	case *int64:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case uint8:
		return NewIntValue(int64(val))
	case *uint8:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case uint32:
		return NewIntValue(int64(val))
	case *uint32:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case uint64:
		return NewIntValue(int64(val))
	case *uint64:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case string:
		// should we return Nil?
		// if val == "null" || val == "NULL" {}
		return NewStringValue(val)
	case []string:
		return NewStringsValue(val)
	// case []uint8:
	// 	return NewByteSliceValue([]byte(val))
	case []byte:
		return NewByteSliceValue(val)
	case json.RawMessage:
		return NewJsonValue(val)
	case bool:
		return NewBoolValue(val)
	case time.Time:
		return NewTimeValue(val)
	case *time.Time:
		return NewTimeValue(*val)
	case map[string]interface{}:
		return NewMapValue(val)
	case map[string]string:
		return NewMapStringValue(val)
	case map[string]float64:
		return NewMapNumberValue(val)
	case map[string]int64:
		return NewMapIntValue(val)
	case map[string]bool:
		return NewMapBoolValue(val)
	case map[string]int:
		nm := make(map[string]int64, len(val))
		for k, v := range val {
			nm[k] = int64(v)
		}
		return NewMapIntValue(nm)
	case map[string]time.Time:
		return NewMapTimeValue(val)
	case []interface{}:
		if len(val) > 0 {
			switch val[0].(type) {
			case string:
				vals := make([]string, len(val))
				for i, v := range val {
					if sv, ok := v.(string); ok {
						vals[i] = sv
					} else {
						vs := make([]Value, len(val))
						for i, v := range val {
							vs[i] = NewValue(v)
						}
						return NewSliceValues(vs)
					}
				}
				return NewStringsValue(vals)
			}
		}
		vals := make([]Value, len(val))
		for i, v := range val {
			vals[i] = NewValue(v)
		}
		return NewSliceValues(vals)
	default:
		if err, isErr := val.(error); isErr {
			return NewErrorValue(err)
		}
		return NewStructValue(val)
	}
}

func NewNumberValue(v float64) NumberValue {
	return NumberValue{v: v}
}
func NewNumberNil() NumberValue {
	v := NumberValue{v: math.NaN()}
	return v
}
func (m NumberValue) Nil() bool                    { return math.IsNaN(m.v) }
func (m NumberValue) Err() bool                    { return math.IsNaN(m.v) }
func (m NumberValue) Type() ValueType              { return NumberType }
func (m NumberValue) Value() interface{}           { return m.v }
func (m NumberValue) Val() float64                 { return m.v }
func (m NumberValue) MarshalJSON() ([]byte, error) { return marshalFloat(float64(m.v)) }
func (m NumberValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m NumberValue) Float() float64               { return m.v }
func (m NumberValue) Int() int64                   { return int64(m.v) }

func NewIntValue(v int64) IntValue {
	return IntValue{v: v}
}

func NewIntNil() IntValue {
	v := IntValue{v: math.MinInt32}
	return v
}

func (m IntValue) Nil() bool                    { return m.v == math.MinInt32 }
func (m IntValue) Err() bool                    { return m.v == math.MinInt32 }
func (m IntValue) Type() ValueType              { return IntType }
func (m IntValue) Value() interface{}           { return m.v }
func (m IntValue) Val() int64                   { return m.v }
func (m IntValue) MarshalJSON() ([]byte, error) { return marshalFloat(float64(m.v)) }
func (m IntValue) NumberValue() NumberValue     { return NewNumberValue(float64(m.v)) }
func (m IntValue) ToString() string {
	if m.v == math.MinInt32 {
		return ""
	}
	return strconv.FormatInt(m.v, 10)
}
func (m IntValue) Float() float64 { return float64(m.v) }
func (m IntValue) Int() int64     { return m.v }

func NewBoolValue(v bool) BoolValue {
	if v {
		return BoolValueTrue
	}
	return BoolValueFalse
}

func (m BoolValue) Nil() bool                    { return false }
func (m BoolValue) Err() bool                    { return false }
func (m BoolValue) Type() ValueType              { return BoolType }
func (m BoolValue) Value() interface{}           { return m.v }
func (m BoolValue) Val() bool                    { return m.v }
func (m BoolValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m BoolValue) ToString() string             { return strconv.FormatBool(m.v) }

func NewStringValue(v string) StringValue {
	return StringValue{v: v}
}

func (m StringValue) Nil() bool                    { return len(m.v) == 0 }
func (m StringValue) Err() bool                    { return false }
func (m StringValue) Type() ValueType              { return StringType }
func (m StringValue) Value() interface{}           { return m.v }
func (m StringValue) Val() string                  { return m.v }
func (m StringValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m StringValue) NumberValue() NumberValue {
	fv, _ := StringToFloat64(m.v)
	return NewNumberValue(fv)
}
func (m StringValue) StringsValue() StringsValue { return NewStringsValue([]string{m.v}) }
func (m StringValue) ToString() string           { return m.v }

func (m StringValue) IntValue() IntValue {
	iv, _ := ValueToInt64(m)
	return NewIntValue(iv)
}

func NewStringsValue(v []string) StringsValue {
	return StringsValue{v: v}
}

func (m StringsValue) Nil() bool                    { return len(m.v) == 0 }
func (m StringsValue) Err() bool                    { return false }
func (m StringsValue) Type() ValueType              { return StringsType }
func (m StringsValue) Value() interface{}           { return m.v }
func (m StringsValue) Val() []string                { return m.v }
func (m *StringsValue) Append(sv string)            { m.v = append(m.v, sv) }
func (m StringsValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m StringsValue) Len() int                     { return len(m.v) }
func (m StringsValue) NumberValue() NumberValue {
	if len(m.v) > 0 {
		if fv, err := strconv.ParseFloat(m.v[0], 64); err == nil {
			return NewNumberValue(fv)
		}
	}
	return NumberNaNValue
}
func (m StringsValue) IntValue() IntValue {
	// Im not confident this is valid?   array first element?
	if len(m.v) > 0 {
		iv, _ := convertStringToInt64(0, m.v[0])
		return NewIntValue(iv)
	}
	return NewIntValue(0)
}
func (m StringsValue) ToString() string  { return strings.Join(m.v, ",") }
func (m StringsValue) Strings() []string { return m.v }
func (m StringsValue) Set() map[string]struct{} {
	setvals := make(map[string]struct{})
	for _, sv := range m.v {
		setvals[sv] = EmptyStruct
	}
	return setvals
}
func (m StringsValue) SliceValue() []Value {
	vs := make([]Value, len(m.v))
	for i, v := range m.v {
		vs[i] = NewStringValue(v)
	}
	return vs
}

func NewByteSliceValue(v []byte) ByteSliceValue {
	return ByteSliceValue{v: v}
}

func (m ByteSliceValue) Nil() bool                    { return len(m.v) == 0 }
func (m ByteSliceValue) Err() bool                    { return false }
func (m ByteSliceValue) Type() ValueType              { return ByteSliceType }
func (m ByteSliceValue) Value() interface{}           { return m.v }
func (m ByteSliceValue) Val() []byte                  { return m.v }
func (m ByteSliceValue) ToString() string             { return string(m.v) }
func (m ByteSliceValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m ByteSliceValue) Len() int                     { return len(m.v) }

func NewSliceValues(v []Value) SliceValue {
	return SliceValue{v: v}
}
func NewSliceValuesNative(iv []interface{}) SliceValue {
	vs := make([]Value, len(iv))
	for i, v := range iv {
		vs[i] = NewValue(v)
	}
	return SliceValue{v: vs}
}

func (m SliceValue) Nil() bool          { return len(m.v) == 0 }
func (m SliceValue) Err() bool          { return false }
func (m SliceValue) Type() ValueType    { return SliceValueType }
func (m SliceValue) Value() interface{} { return m.v }
func (m SliceValue) Val() []Value       { return m.v }
func (m SliceValue) ToString() string {
	sv := make([]string, len(m.Val()))
	for i, val := range m.v {
		sv[i] = val.ToString()
	}
	return strings.Join(sv, ",")
}

func (m *SliceValue) Append(v Value)              { m.v = append(m.v, v) }
func (m SliceValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m SliceValue) Len() int                     { return len(m.v) }
func (m SliceValue) SliceValue() []Value          { return m.v }
func (m SliceValue) Values() []interface{} {
	vals := make([]interface{}, len(m.v))
	for i, v := range m.v {
		vals[i] = v.Value()
	}
	return vals
}

func NewMapValue(v map[string]interface{}) MapValue {
	mv := make(map[string]Value)
	for n, val := range v {
		mv[n] = NewValue(val)
	}
	return MapValue{v: mv}
}

func (m MapValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapValue) Err() bool                    { return false }
func (m MapValue) Type() ValueType              { return MapValueType }
func (m MapValue) Value() interface{}           { return m.v }
func (m MapValue) Val() map[string]Value        { return m.v }
func (m MapValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapValue) Len() int                     { return len(m.v) }
func (m MapValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		intVal, ok := ValueToInt64(v)
		if ok {
			mv[n] = intVal
		}
	}
	return mv
}
func (m MapValue) MapFloat() map[string]float64 {
	mv := make(map[string]float64, len(m.v))
	for n, v := range m.v {
		fv, _ := ValueToFloat64(v)
		if !math.IsNaN(fv) {
			mv[n] = fv
		}
	}
	return mv
}
func (m MapValue) MapString() map[string]string {
	mv := make(map[string]string, len(m.v))
	for n, v := range m.v {
		mv[n] = v.ToString()
	}
	return mv
}
func (m MapValue) MapValue() MapValue {
	return m
}
func (m MapValue) MapTime() MapTimeValue {
	mv := make(map[string]time.Time, len(m.v))
	for k, v := range m.v {
		t, ok := ValueToTime(v)
		if ok && !t.IsZero() {
			mv[k] = t
		}
	}
	return NewMapTimeValue(mv)
}
func (m MapValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	return v, ok
}
func NewMapStringValue(v map[string]string) MapStringValue {
	return MapStringValue{v: v}
}

func (m MapStringValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapStringValue) Err() bool                    { return false }
func (m MapStringValue) Type() ValueType              { return MapStringType }
func (m MapStringValue) Value() interface{}           { return m.v }
func (m MapStringValue) Val() map[string]string       { return m.v }
func (m MapStringValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapStringValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapStringValue) Len() int                     { return len(m.v) }
func (m MapStringValue) MapBool() MapBoolValue {
	mb := make(map[string]bool)
	for n, sv := range m.Val() {
		b, err := strconv.ParseBool(sv)
		if err == nil {
			mb[n] = b
		}
	}
	return NewMapBoolValue(mb)
}
func (m MapStringValue) MapInt() MapIntValue {
	mi := make(map[string]int64)
	for n, sv := range m.Val() {
		iv, err := strconv.ParseInt(sv, 10, 64)
		if err == nil {
			mi[n] = iv
		}
	}
	return NewMapIntValue(mi)
}
func (m MapStringValue) MapNumber() MapNumberValue {
	mn := make(map[string]float64)
	for n, sv := range m.Val() {
		fv, err := strconv.ParseFloat(sv, 64)
		if err == nil {
			mn[n] = fv
		}
	}
	return NewMapNumberValue(mn)
}
func (m MapStringValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewStringValue(val)
	}
	return MapValue{v: mv}
}
func (m MapStringValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewStringValue(v), ok
	}
	return nil, ok
}
func (m MapStringValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapIntValue(v map[string]int64) MapIntValue {
	return MapIntValue{v: v}
}

func (m MapIntValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapIntValue) Err() bool                    { return false }
func (m MapIntValue) Type() ValueType              { return MapIntType }
func (m MapIntValue) Value() interface{}           { return m.v }
func (m MapIntValue) Val() map[string]int64        { return m.v }
func (m MapIntValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapIntValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapIntValue) Len() int                     { return len(m.v) }
func (m MapIntValue) MapInt() map[string]int64     { return m.v }
func (m MapIntValue) MapFloat() map[string]float64 {
	mv := make(map[string]float64, len(m.v))
	for n, iv := range m.v {
		mv[n] = float64(iv)
	}
	return mv
}
func (m MapIntValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewIntValue(val)
	}
	return MapValue{v: mv}
}
func (m MapIntValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewIntValue(v), ok
	}
	return nil, ok
}
func (m MapIntValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapNumberValue(v map[string]float64) MapNumberValue {
	return MapNumberValue{v: v}
}

func (m MapNumberValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapNumberValue) Err() bool                    { return false }
func (m MapNumberValue) Type() ValueType              { return MapNumberType }
func (m MapNumberValue) Value() interface{}           { return m.v }
func (m MapNumberValue) Val() map[string]float64      { return m.v }
func (m MapNumberValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapNumberValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapNumberValue) Len() int                     { return len(m.v) }
func (m MapNumberValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		mv[n] = int64(v)
	}
	return mv
}
func (m MapNumberValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewNumberValue(val)
	}
	return MapValue{v: mv}
}
func (m MapNumberValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewNumberValue(v), ok
	}
	return nil, ok
}
func (m MapNumberValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapTimeValue(v map[string]time.Time) MapTimeValue {
	return MapTimeValue{v: v}
}

func (m MapTimeValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapTimeValue) Err() bool                    { return false }
func (m MapTimeValue) Type() ValueType              { return MapTimeType }
func (m MapTimeValue) Value() interface{}           { return m.v }
func (m MapTimeValue) Val() map[string]time.Time    { return m.v }
func (m MapTimeValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapTimeValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapTimeValue) Len() int                     { return len(m.v) }
func (m MapTimeValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		mv[n] = v.UnixNano()
	}
	return mv
}
func (m MapTimeValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewTimeValue(val)
	}
	return MapValue{v: mv}
}
func (m MapTimeValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewTimeValue(v), ok
	}
	return nil, ok
}

func NewMapBoolValue(v map[string]bool) MapBoolValue {
	return MapBoolValue{v: v}
}

func (m MapBoolValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapBoolValue) Err() bool                    { return false }
func (m MapBoolValue) Type() ValueType              { return MapBoolType }
func (m MapBoolValue) Value() interface{}           { return m.v }
func (m MapBoolValue) Val() map[string]bool         { return m.v }
func (m MapBoolValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapBoolValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapBoolValue) Len() int                     { return len(m.v) }
func (m MapBoolValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewBoolValue(val)
	}
	return MapValue{v: mv}
}
func (m MapBoolValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewBoolValue(v), ok
	}
	return nil, ok
}
func (m MapBoolValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewStructValue(v interface{}) StructValue {
	return StructValue{v: v}
}

func (m StructValue) Nil() bool                    { return m.v == nil }
func (m StructValue) Err() bool                    { return false }
func (m StructValue) Type() ValueType              { return StructType }
func (m StructValue) Value() interface{}           { return m.v }
func (m StructValue) Val() interface{}             { return m.v }
func (m StructValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m StructValue) ToString() string             { return fmt.Sprintf("%v", m.v) }

func NewJsonValue(v json.RawMessage) JsonValue {
	return JsonValue{v: v}
}

func (m JsonValue) Nil() bool                    { return m.v == nil }
func (m JsonValue) Err() bool                    { return false }
func (m JsonValue) Type() ValueType              { return JsonType }
func (m JsonValue) Value() interface{}           { return m.v }
func (m JsonValue) Val() interface{}             { return m.v }
func (m JsonValue) MarshalJSON() ([]byte, error) { return []byte(m.v), nil }
func (m JsonValue) ToString() string             { return string(m.v) }

func NewTimeValue(v time.Time) TimeValue {
	return TimeValue{v: v}
}

func (m TimeValue) Nil() bool                    { return m.v.IsZero() }
func (m TimeValue) Err() bool                    { return false }
func (m TimeValue) Type() ValueType              { return TimeType }
func (m TimeValue) Value() interface{}           { return m.v }
func (m TimeValue) Val() time.Time               { return m.v }
func (m TimeValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m TimeValue) ToString() string             { return strconv.FormatInt(m.Int(), 10) }
func (m TimeValue) Float() float64               { return float64(m.v.In(time.UTC).UnixNano() / 1e6) }
func (m TimeValue) Int() int64                   { return m.v.In(time.UTC).UnixNano() / 1e6 }
func (m TimeValue) Time() time.Time              { return m.v }

func NewErrorValue(v error) ErrorValue {
	return ErrorValue{v: v}
}

func NewErrorValuef(v string, args ...interface{}) ErrorValue {
	return ErrorValue{v: fmt.Errorf(v, args...)}
}

func (m ErrorValue) Nil() bool                    { return false }
func (m ErrorValue) Err() bool                    { return true }
func (m ErrorValue) Type() ValueType              { return ErrorType }
func (m ErrorValue) Value() interface{}           { return m.v }
func (m ErrorValue) Val() error                   { return m.v }
func (m ErrorValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m ErrorValue) ToString() string             { return m.v.Error() }

// ErrorValues implement Go's error interface so they can easily cross the
// VM/Go boundary.
func (m ErrorValue) Error() string { return m.v.Error() }

func NewNilValue() NilValue {
	return NilValue{}
}

func (m NilValue) Nil() bool                    { return true }
func (m NilValue) Err() bool                    { return false }
func (m NilValue) Type() ValueType              { return NilType }
func (m NilValue) Value() interface{}           { return nil }
func (m NilValue) Val() interface{}             { return nil }
func (m NilValue) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (m NilValue) ToString() string             { return "" }
