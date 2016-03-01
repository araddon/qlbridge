package value

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
)

var (
	_ = u.EMPTY

	// our DataTypes we support, a limited sub-set of go
	floatRv     = reflect.ValueOf(float64(1.2))
	int64Rv     = reflect.ValueOf(int64(1))
	int32Rv     = reflect.ValueOf(int32(1))
	stringRv    = reflect.ValueOf("hello")
	stringsRv   = reflect.ValueOf([]string{"hello"})
	byteSliceRv = reflect.ValueOf([]byte("hello"))
	boolRv      = reflect.ValueOf(true)
	mapValueRv  = reflect.ValueOf(map[string]Value{"hello": NewValue(1)})
	mapStringRv = reflect.ValueOf(map[string]string{"hello": "world"})
	mapIntRv    = reflect.ValueOf(map[string]int64{"hello": int64(1)})
	mapFloatRv  = reflect.ValueOf(map[string]float64{"hello": float64(1.1)})
	mapBoolRv   = reflect.ValueOf(map[string]bool{"hello": true})
	timeRv      = reflect.ValueOf(time.Time{})
	nilRv       = reflect.ValueOf(nil)

	RV_ZERO     = reflect.Value{}
	nilStruct   *emptyStruct
	EmptyStruct = struct{}{}

	NilValueVal         = NewNilValue()
	BoolValueTrue       = BoolValue{true, reflect.ValueOf(true)}
	BoolValueFalse      = BoolValue{false, reflect.ValueOf(false)}
	NumberNaNValue      = NewNumberValue(math.NaN())
	EmptyStringValue    = NewStringValue("")
	EmptyStringsValue   = NewStringsValue(nil)
	EmptyMapValue       = NewMapValue(nil)
	EmptyMapStringValue = NewMapStringValue(make(map[string]string))
	EmptyMapIntValue    = NewMapIntValue(make(map[string]int64))
	EmptyMapNumberValue = NewMapNumberValue(make(map[string]float64))
	EmptyMapBoolValue   = NewMapBoolValue(make(map[string]bool))
	NilStructValue      = NewStructValue(nilStruct)
	TimeZeroValue       = NewTimeValue(time.Time{})
	ErrValue            = NewErrorValue("")

	_ Value = (StringValue)(EmptyStringValue)

	// force some types to implement interfaces
	_ Map = (MapIntValue)(EmptyMapIntValue)
	_ Map = (MapStringValue)(EmptyMapStringValue)
	_ Map = (MapNumberValue)(EmptyMapNumberValue)
	_ Map = (MapBoolValue)(EmptyMapBoolValue)
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
	SliceValueType     ValueType = 40
	StructType         ValueType = 50
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
	case MapBoolType:
		return "map[string]bool"
	case SliceValueType:
		return "[]value"
	case StructType:
		return "struct"
	default:
		return "invalid"
	}
}

type emptyStruct struct{}

type (
	Value interface {
		// Is this a nil/empty?  ie empty string?  or nil struct, etc
		Nil() bool
		// Is this an error, or unable to evaluate from Vm?
		Err() bool
		Value() interface{}
		Rv() reflect.Value
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
	}
	Map interface {
		MapValue() MapValue
	}
)

type (
	NumberValue struct {
		v  float64
		rv reflect.Value
	}
	IntValue struct {
		v  int64
		rv reflect.Value
	}
	BoolValue struct {
		v  bool
		rv reflect.Value
	}
	StringValue struct {
		v  string
		rv reflect.Value
	}
	TimeValue struct {
		v  time.Time
		rv reflect.Value
	}
	StringsValue struct {
		v  []string
		rv reflect.Value
	}
	ByteSliceValue struct {
		v  []byte
		rv reflect.Value
	}
	SliceValue struct {
		v  []Value
		rv reflect.Value
	}
	MapValue struct {
		v  map[string]Value
		rv reflect.Value
	}
	MapIntValue struct {
		v  map[string]int64
		rv reflect.Value
	}
	MapNumberValue struct {
		v  map[string]float64
		rv reflect.Value
	}
	MapStringValue struct {
		v  map[string]string
		rv reflect.Value
	}
	MapBoolValue struct {
		v  map[string]bool
		rv reflect.Value
	}
	StructValue struct {
		v  interface{}
		rv reflect.Value
	}
	ErrorValue struct {
		v  string
		rv reflect.Value
	}
	NilValue struct{}
)

// Given a string, convert to valuetype
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
	case "[]value":
		return SliceValueType
	case "struct":
		return StructType
	default:
		return UnknownType
	}
}

// Create a new Value type with native go value
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
	case int:
		return NewIntValue(int64(val))
	case int32:
		return NewIntValue(int64(val))
	case int64:
		return NewIntValue(val)
	case string:
		return NewStringValue(val)
	case []string:
		return NewStringsValue(val)
	// case []uint8:
	// 	return NewByteSliceValue([]byte(val))
	case []byte:
		return NewByteSliceValue(val)
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
	default:
		if valValue, ok := goVal.(Value); ok {
			return valValue
		}
		u.LogTracef(u.WARN, "hello")
		u.Errorf("invalud value type %T.", val)
	}
	return NilValueVal
}

func NewValueReflect(rv reflect.Value) Value {
	switch rv.Kind() {
	case reflect.String:
		return NewStringValue(rv.String())
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		return NewIntValue(rv.Int())
	case reflect.Float32, reflect.Float64:
		return NewNumberValue(rv.Float())
	case reflect.Bool:
		return NewBoolValue(rv.Bool())
	//case reflect.ValueOf(time.Time{}).Kind():
	default:
		return NewValue(rv.Interface())
		//u.Warnf("not implemented %v", rv.Kind())
	}
	return nil
}

func ValueTypeFromRT(rt reflect.Type) ValueType {
	switch rt {
	case reflect.TypeOf(NilValue{}):
		return NilType
	case reflect.TypeOf(NumberValue{}):
		return NumberType
	case reflect.TypeOf(IntValue{}):
		return IntType
	case reflect.TypeOf(TimeValue{}):
		return TimeType
	case reflect.TypeOf(BoolValue{}):
		return BoolType
	case reflect.TypeOf(StringValue{}):
		return StringType
	case reflect.TypeOf(ByteSliceValue{}):
		return ByteSliceType
	case reflect.TypeOf(StringsValue{}):
		return StringsType
	case reflect.TypeOf(MapIntValue{}):
		return MapIntType
	case reflect.TypeOf(SliceValue{}):
		return SliceValueType
	case reflect.TypeOf(MapValue{}):
		return MapValueType
	case reflect.TypeOf(MapStringValue{}):
		return MapStringType
	case reflect.TypeOf(MapIntValue{}):
		return MapIntType
	case reflect.TypeOf(MapNumberValue{}):
		return MapNumberType
	case reflect.TypeOf(MapBoolValue{}):
		return MapBoolType
	case reflect.TypeOf(StructValue{}):
		return StructType
	case reflect.TypeOf(ErrorValue{}):
		return ErrorType
	default:
		// If type == Value, then it is not telling us what type
		// we should probably just allow it, as it is not telling us much
		// info but isn't wrong
		if "value.Value" == fmt.Sprintf("%v", rt) {
			//fmt.Printf("no type? %#v\n\n", rt)
			return ValueInterfaceType
		} else {
			u.Warnf("Unrecognized Value Type Kind?  %v %T ", rt, rt)
		}
	}
	return NilType
}

func NewNumberValue(v float64) NumberValue {
	return NumberValue{v: v, rv: reflect.ValueOf(v)}
}
func NewNumberNil() NumberValue {
	v := NumberValue{v: math.NaN()}
	v.rv = reflect.ValueOf(v.v)
	return v
}
func (m NumberValue) Nil() bool                         { return math.IsNaN(m.v) }
func (m NumberValue) Err() bool                         { return math.IsNaN(m.v) }
func (m NumberValue) Type() ValueType                   { return NumberType }
func (m NumberValue) Rv() reflect.Value                 { return m.rv }
func (m NumberValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(int64Rv, toRv) }
func (m NumberValue) Value() interface{}                { return m.v }
func (m NumberValue) Val() float64                      { return m.v }
func (m NumberValue) MarshalJSON() ([]byte, error)      { return marshalFloat(float64(m.v)) }
func (m NumberValue) ToString() string                  { return strconv.FormatFloat(float64(m.v), 'f', -1, 64) }
func (m NumberValue) Float() float64                    { return m.v }
func (m NumberValue) Int() int64                        { return int64(m.v) }

func NewIntValue(v int64) IntValue {
	return IntValue{v: v, rv: reflect.ValueOf(v)}
}
func NewIntNil() IntValue {
	v := IntValue{v: math.MinInt32}
	v.rv = reflect.ValueOf(v.v)
	return v
}

func (m IntValue) Nil() bool                         { return m.v == math.MinInt32 }
func (m IntValue) Err() bool                         { return m.v == math.MinInt32 }
func (m IntValue) Type() ValueType                   { return IntType }
func (m IntValue) Rv() reflect.Value                 { return m.rv }
func (m IntValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(int64Rv, toRv) }
func (m IntValue) Value() interface{}                { return m.v }
func (m IntValue) Val() int64                        { return m.v }
func (m IntValue) MarshalJSON() ([]byte, error)      { return marshalFloat(float64(m.v)) }
func (m IntValue) NumberValue() NumberValue          { return NewNumberValue(float64(m.v)) }
func (m IntValue) ToString() string                  { return strconv.FormatInt(m.v, 10) }
func (m IntValue) Float() float64                    { return float64(m.v) }
func (m IntValue) Int() int64                        { return m.v }

func NewBoolValue(v bool) BoolValue {
	if v {
		return BoolValueTrue
	}
	return BoolValueFalse
}

func (m BoolValue) Nil() bool                         { return false }
func (m BoolValue) Err() bool                         { return false }
func (m BoolValue) Type() ValueType                   { return BoolType }
func (m BoolValue) Rv() reflect.Value                 { return m.rv }
func (m BoolValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(boolRv, toRv) }
func (m BoolValue) Value() interface{}                { return m.v }
func (m BoolValue) Val() bool                         { return m.v }
func (m BoolValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m BoolValue) ToString() string                  { return strconv.FormatBool(m.v) }

func NewStringValue(v string) StringValue {
	return StringValue{v: v, rv: reflect.ValueOf(v)}
}

func (m StringValue) Nil() bool                          { return len(m.v) == 0 }
func (m StringValue) Err() bool                          { return false }
func (m StringValue) Type() ValueType                    { return StringType }
func (m StringValue) Rv() reflect.Value                  { return m.rv }
func (m StringValue) CanCoerce(input reflect.Value) bool { return CanCoerce(stringRv, input) }
func (m StringValue) Value() interface{}                 { return m.v }
func (m StringValue) Val() string                        { return m.v }
func (m StringValue) MarshalJSON() ([]byte, error)       { return json.Marshal(m.v) }
func (m StringValue) NumberValue() NumberValue           { fv, _ := ToFloat64(m.Rv()); return NewNumberValue(fv) }
func (m StringValue) StringsValue() StringsValue         { return NewStringsValue([]string{m.v}) }
func (m StringValue) ToString() string                   { return m.v }

func (m StringValue) IntValue() IntValue {
	iv, _ := ToInt64(m.Rv())
	return NewIntValue(iv)
}

func NewStringsValue(v []string) StringsValue {
	return StringsValue{v: v, rv: reflect.ValueOf(v)}
}

func (m StringsValue) Nil() bool                           { return len(m.v) == 0 }
func (m StringsValue) Err() bool                           { return false }
func (m StringsValue) Type() ValueType                     { return StringsType }
func (m StringsValue) Rv() reflect.Value                   { return m.rv }
func (m StringsValue) CanCoerce(boolRv reflect.Value) bool { return CanCoerce(stringRv, boolRv) }
func (m StringsValue) Value() interface{}                  { return m.v }
func (m StringsValue) Val() []string                       { return m.v }
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
		//setvals[strings.ToLower(sv)] = EmptyStruct
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
	return ByteSliceValue{v: v, rv: reflect.ValueOf(v)}
}

func (m ByteSliceValue) Nil() bool                    { return len(m.v) == 0 }
func (m ByteSliceValue) Err() bool                    { return false }
func (m ByteSliceValue) Type() ValueType              { return ByteSliceType }
func (m ByteSliceValue) Rv() reflect.Value            { return m.rv }
func (m ByteSliceValue) Value() interface{}           { return m.v }
func (m ByteSliceValue) Val() []byte                  { return m.v }
func (m ByteSliceValue) ToString() string             { return string(m.v) }
func (m ByteSliceValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m ByteSliceValue) Len() int                     { return len(m.v) }

func NewSliceValues(v []Value) SliceValue {
	return SliceValue{v: v, rv: reflect.ValueOf(v)}
}

func (m SliceValue) Nil() bool          { return len(m.v) == 0 }
func (m SliceValue) Err() bool          { return false }
func (m SliceValue) Type() ValueType    { return SliceValueType }
func (m SliceValue) Rv() reflect.Value  { return m.rv }
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
	return MapValue{v: mv, rv: reflect.ValueOf(mv)}
}

func (m MapValue) Nil() bool                         { return len(m.v) == 0 }
func (m MapValue) Err() bool                         { return false }
func (m MapValue) Type() ValueType                   { return MapValueType }
func (m MapValue) Rv() reflect.Value                 { return m.rv }
func (m MapValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapValueRv, toRv) }
func (m MapValue) Value() interface{}                { return m.v }
func (m MapValue) Val() map[string]Value             { return m.v }
func (m MapValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m MapValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }
func (m MapValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		intVal, ok := ToInt64(v.Rv())
		if ok {
			mv[n] = intVal
		}
	}
	return mv
}
func (m MapValue) MapFloat() map[string]float64 {
	mv := make(map[string]float64, len(m.v))
	for n, v := range m.v {
		fv, _ := ToFloat64(v.Rv())
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

func NewMapStringValue(v map[string]string) MapStringValue {
	return MapStringValue{v: v, rv: reflect.ValueOf(v)}
}

func (m MapStringValue) Nil() bool                         { return len(m.v) == 0 }
func (m MapStringValue) Err() bool                         { return false }
func (m MapStringValue) Type() ValueType                   { return MapStringType }
func (m MapStringValue) Rv() reflect.Value                 { return m.rv }
func (m MapStringValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapStringRv, toRv) }
func (m MapStringValue) Value() interface{}                { return m.v }
func (m MapStringValue) Val() map[string]string            { return m.v }
func (m MapStringValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m MapStringValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }
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
	return MapValue{v: mv, rv: reflect.ValueOf(mv)}
}
func (m MapStringValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapIntValue(v map[string]int64) MapIntValue {
	return MapIntValue{v: v, rv: reflect.ValueOf(v)}
}

func (m MapIntValue) Nil() bool                         { return len(m.v) == 0 }
func (m MapIntValue) Err() bool                         { return false }
func (m MapIntValue) Type() ValueType                   { return MapIntType }
func (m MapIntValue) Rv() reflect.Value                 { return m.rv }
func (m MapIntValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapIntRv, toRv) }
func (m MapIntValue) Value() interface{}                { return m.v }
func (m MapIntValue) Val() map[string]int64             { return m.v }
func (m MapIntValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m MapIntValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }
func (m MapIntValue) MapInt() map[string]int64          { return m.v }
func (m MapIntValue) MapFloat() map[string]float64 {
	mv := make(map[string]float64, len(m.v))
	for n, v := range m.v {
		fv, _ := ToFloat64(reflect.ValueOf(v))
		if !math.IsNaN(fv) {
			mv[n] = fv
		}
	}
	return mv
}
func (m MapIntValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewIntValue(val)
	}
	return MapValue{v: mv, rv: reflect.ValueOf(mv)}
}
func (m MapIntValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapNumberValue(v map[string]float64) MapNumberValue {
	return MapNumberValue{v: v, rv: reflect.ValueOf(v)}
}

func (m MapNumberValue) Nil() bool                         { return len(m.v) == 0 }
func (m MapNumberValue) Err() bool                         { return false }
func (m MapNumberValue) Type() ValueType                   { return MapNumberType }
func (m MapNumberValue) Rv() reflect.Value                 { return m.rv }
func (m MapNumberValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapFloatRv, toRv) }
func (m MapNumberValue) Value() interface{}                { return m.v }
func (m MapNumberValue) Val() map[string]float64           { return m.v }
func (m MapNumberValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m MapNumberValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }
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
	return MapValue{v: mv, rv: reflect.ValueOf(mv)}
}
func (m MapNumberValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapBoolValue(v map[string]bool) MapBoolValue {
	return MapBoolValue{v: v, rv: reflect.ValueOf(v)}
}

func (m MapBoolValue) Nil() bool                         { return len(m.v) == 0 }
func (m MapBoolValue) Err() bool                         { return false }
func (m MapBoolValue) Type() ValueType                   { return MapBoolType }
func (m MapBoolValue) Rv() reflect.Value                 { return m.rv }
func (m MapBoolValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(mapBoolRv, toRv) }
func (m MapBoolValue) Value() interface{}                { return m.v }
func (m MapBoolValue) Val() map[string]bool              { return m.v }
func (m MapBoolValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m MapBoolValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }
func (m MapBoolValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewBoolValue(val)
	}
	return MapValue{v: mv, rv: reflect.ValueOf(mv)}
}
func (m MapBoolValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewStructValue(v interface{}) StructValue {
	return StructValue{v: v, rv: reflect.ValueOf(v)}
}

func (m StructValue) Nil() bool                         { return m.v == nil }
func (m StructValue) Err() bool                         { return false }
func (m StructValue) Type() ValueType                   { return StructType }
func (m StructValue) Rv() reflect.Value                 { return m.rv }
func (m StructValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m StructValue) Value() interface{}                { return m.v }
func (m StructValue) Val() interface{}                  { return m.v }
func (m StructValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m StructValue) ToString() string                  { return fmt.Sprintf("%v", m.v) }

func NewTimeValue(t time.Time) TimeValue {
	return TimeValue{v: t, rv: reflect.ValueOf(t)}
}

func (m TimeValue) Nil() bool                         { return m.v.IsZero() }
func (m TimeValue) Err() bool                         { return false }
func (m TimeValue) Type() ValueType                   { return TimeType }
func (m TimeValue) Rv() reflect.Value                 { return m.rv }
func (m TimeValue) CanCoerce(toRv reflect.Value) bool { return CanCoerce(timeRv, toRv) }
func (m TimeValue) Value() interface{}                { return m.v }
func (m TimeValue) Val() time.Time                    { return m.v }
func (m TimeValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m TimeValue) ToString() string                  { return strconv.FormatInt(m.Int(), 10) }
func (m TimeValue) Float() float64                    { return float64(m.v.UnixNano() / 1e6) }
func (m TimeValue) Int() int64                        { return m.v.UnixNano() / 1e6 }
func (m TimeValue) Time() time.Time                   { return m.v }

func NewErrorValue(v string) ErrorValue {
	return ErrorValue{v: v, rv: reflect.ValueOf(v)}
}

func NewErrorValuef(v string, args ...interface{}) ErrorValue {
	return ErrorValue{v: fmt.Sprintf(v, args...), rv: reflect.ValueOf(v)}
}

func (m ErrorValue) Nil() bool                         { return false }
func (m ErrorValue) Err() bool                         { return true }
func (m ErrorValue) Type() ValueType                   { return ErrorType }
func (m ErrorValue) Rv() reflect.Value                 { return m.rv }
func (m ErrorValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m ErrorValue) Value() interface{}                { return m.v }
func (m ErrorValue) Val() string                       { return m.v }
func (m ErrorValue) MarshalJSON() ([]byte, error)      { return json.Marshal(m.v) }
func (m ErrorValue) ToString() string                  { return m.v }

// ErrorValues implement Go's error interface so they can easily cross the
// VM/Go boundary.
func (m ErrorValue) Error() string { return m.v }

func NewNilValue() NilValue {
	return NilValue{}
}

func (m NilValue) Nil() bool                         { return true }
func (m NilValue) Err() bool                         { return false }
func (m NilValue) Type() ValueType                   { return NilType }
func (m NilValue) Rv() reflect.Value                 { return nilRv }
func (m NilValue) CanCoerce(toRv reflect.Value) bool { return false }
func (m NilValue) Value() interface{}                { return nil }
func (m NilValue) Val() interface{}                  { return nil }
func (m NilValue) MarshalJSON() ([]byte, error)      { return nil, nil }
func (m NilValue) ToString() string                  { return "" }
