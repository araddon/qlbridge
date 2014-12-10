package vm

import (
	"net/url"
)

var (
	_ ContextWriter = (*ContextSimple)(nil)
	_ ContextReader = (*ContextSimple)(nil)
	_ ContextWriter = (*ContextUrlValues)(nil)
	_ ContextReader = (*ContextUrlValues)(nil)
)

type ContextReader interface {
	Get(key string) Value
}

type ContextWriter interface {
	Put(key string, v Value) error
}

type ContextSimple struct {
	data map[string]Value
}

func NewContextSimple() ContextSimple {
	return ContextSimple{data: make(map[string]Value)}
}

func (m ContextSimple) All() map[string]Value {
	return m.data
}

func (m ContextSimple) Get(key string) Value {
	return m.data[key]
}

func (m ContextSimple) Put(key string, v Value) error {
	// switch typedValue := v.(type) {
	// case StringValue:
	// 	m.data[key] = typedValue.v
	// case NumberValue:
	// 	m.data[key] = typedValue.String()
	// }
	m.data[key] = v
	return nil
}

type ContextUrlValues struct {
	data url.Values
}

func NewContextUrlValues(uv url.Values) ContextUrlValues {
	return ContextUrlValues{uv}
}

func (m ContextUrlValues) Get(key string) Value {
	v := m.data.Get(key)
	return NewStringValue(v)
}

func (m ContextUrlValues) Put(key string, v Value) error {
	switch typedValue := v.(type) {
	case StringValue:
		m.data.Set(key, typedValue.v)
	case NumberValue:
		m.data.Set(key, typedValue.String())
	}
	return nil
}
