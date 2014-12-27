package vm

import (
	u "github.com/araddon/gou"
	"net/url"
	"time"
)

var (
	_ ContextWriter = (*ContextSimple)(nil)
	_ ContextReader = (*ContextSimple)(nil)
	_ ContextWriter = (*ContextUrlValues)(nil)
	_ ContextReader = (*ContextUrlValues)(nil)
	_               = u.EMPTY
)

// Context Reader is interface to read the context of message/row/command
//  being evaluated
type ContextReader interface {
	Get(key string) (Value, bool)
	Row() map[string]Value
	Ts() time.Time
}

type ContextWriter interface {
	Put(col SchemaInfo, readCtx ContextReader, v Value) error
}

// for commiting row ops (insert, update)
type RowWriter interface {
	Commit(rowInfo []SchemaInfo, row ContextWriter) error
	Put(col SchemaInfo, readCtx ContextReader, v Value) error
	//Rows() []map[string]Value
}

type ContextSimple struct {
	Data map[string]Value
	Rows []map[string]Value
	ts   time.Time
}

func NewContextSimple() *ContextSimple {
	return &ContextSimple{Data: make(map[string]Value), ts: time.Now()}
}
func NewContextSimpleData(data map[string]Value) *ContextSimple {
	return &ContextSimple{Data: data, ts: time.Now()}
}
func NewContextSimpleTs(data map[string]Value, ts time.Time) *ContextSimple {
	return &ContextSimple{Data: data, ts: ts}
}

func (m ContextSimple) All() map[string]Value {
	return m.Data
}
func (m ContextSimple) Row() map[string]Value {
	return m.Data
}

func (m ContextSimple) Get(key string) (Value, bool) {
	val, ok := m.Data[key]
	return val, ok
}
func (m ContextSimple) Ts() time.Time {
	return m.ts
}

func (m *ContextSimple) Put(col SchemaInfo, rctx ContextReader, v Value) error {
	//u.Infof("put context:  %v %T:%v", col.Key(), v, v)
	m.Data[col.Key()] = v
	return nil
}
func (m *ContextSimple) Commit(rowInfo []SchemaInfo, row ContextWriter) error {
	m.Rows = append(m.Rows, m.Data)
	m.Data = make(map[string]Value)
	return nil
}

type ContextUrlValues struct {
	Data url.Values
	ts   time.Time
}

func NewContextUrlValues(uv url.Values) *ContextUrlValues {
	return &ContextUrlValues{uv, time.Now()}
}
func NewContextUrlValuesTs(uv url.Values, ts time.Time) *ContextUrlValues {
	return &ContextUrlValues{uv, ts}
}
func (m ContextUrlValues) Get(key string) (Value, bool) {
	vals, ok := m.Data[key]
	if ok {
		if len(vals) == 1 {
			return NewStringValue(vals[0]), true
		}
		return NewStringsValue(vals), true
	}
	return EmptyStringValue, false
}
func (m ContextUrlValues) Row() map[string]Value {
	mi := make(map[string]Value)
	for k, v := range m.Data {
		if len(v) == 1 {
			mi[k] = NewStringValue(v[0])
		} else if len(v) > 1 {
			mi[k] = NewStringsValue(v)
		}
	}
	return mi
}

func (m ContextUrlValues) Ts() time.Time {
	return m.ts
}

func (m ContextUrlValues) Put(col SchemaInfo, rctx ContextReader, v Value) error {
	key := col.Key()
	switch typedValue := v.(type) {
	case StringValue:
		m.Data.Set(key, typedValue.v)
	case NumberValue:
		m.Data.Set(key, typedValue.ToString())
	}
	return nil
}
