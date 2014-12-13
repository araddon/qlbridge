package vm

import (
	u "github.com/araddon/gou"
	"net/url"
)

var (
	_ ContextWriter = (*ContextSimple)(nil)
	_ ContextReader = (*ContextSimple)(nil)
	_ ContextWriter = (*ContextUrlValues)(nil)
	_ ContextReader = (*ContextUrlValues)(nil)
	_               = u.EMPTY
)

type ContextReader interface {
	Get(key string) Value
}

type ContextWriter interface {
	Put(key string, v Value) error
}

type ContextSimple struct {
	Data map[string]Value
}

func NewContextSimple() ContextSimple {
	return ContextSimple{Data: make(map[string]Value)}
}

func (m ContextSimple) All() map[string]Value {
	return m.Data
}

func (m ContextSimple) Get(key string) Value {
	return m.Data[key]
}

func (m ContextSimple) Put(key string, v Value) error {
	// switch typedValue := v.(type) {
	// case StringValue:
	// 	m.data[key] = typedValue.v
	// case NumberValue:
	// 	m.data[key] = typedValue.String()
	// }
	u.Infof("put context:  %v %T:%v", key, v, v)
	m.Data[key] = v
	return nil
}

type ContextUrlValues struct {
	Data url.Values
}

func NewContextUrlValues(uv url.Values) ContextUrlValues {
	return ContextUrlValues{uv}
}

func (m ContextUrlValues) Get(key string) Value {
	v := m.Data.Get(key)
	return NewStringValue(v)
}

func (m ContextUrlValues) Put(key string, v Value) error {
	switch typedValue := v.(type) {
	case StringValue:
		m.Data.Set(key, typedValue.v)
	case NumberValue:
		m.Data.Set(key, typedValue.String())
	}
	return nil
}
