package vm

import (
	"net/url"
)

type Context interface {
	Get(key string) Value
}

type ContextSimple struct {
	data map[string]Value
}

func (m ContextSimple) Get(key string) Value {
	return m.data[key]
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
