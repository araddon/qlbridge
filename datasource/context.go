package datasource

import (
	"database/sql/driver"
	"fmt"
	"net/url"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var (
	_ expr.ContextWriter = (*ContextSimple)(nil)
	_ expr.ContextReader = (*ContextSimple)(nil)
	_ expr.ContextWriter = (*ContextUrlValues)(nil)
	_ expr.ContextReader = (*ContextUrlValues)(nil)
	_                    = u.EMPTY
)

// represents a message routable by the topology. The Key() method
// is used to route the message in certain topologies. Body() is used
// to express something user specific.
// see  "https://github.com/mdmarek/topo" AND http://github.com/lytics/grid
type Message interface {
	Key() uint64
	Body() interface{}
}

type SqlDriverMessage struct {
	vals []driver.Value
	id   uint64
}

func (m *SqlDriverMessage) Key() uint64       { return m.id }
func (m *SqlDriverMessage) Body() interface{} { return m.vals }

type UrlValuesMsg struct {
	body *ContextUrlValues
	id   uint64
}

func (m *UrlValuesMsg) Key() uint64       { return m.id }
func (m *UrlValuesMsg) Body() interface{} { return m.body }
func (m *UrlValuesMsg) String() string    { return m.body.String() }

// type RowScanner interface {
// 	NextXX() map[string]value.Value
// }
type ContextSimple struct {
	Data map[string]value.Value
	//Rows   []map[string]value.Value
	ts     time.Time
	cursor int
	keyval uint64
}

func NewContextSimple() *ContextSimple {
	return &ContextSimple{Data: make(map[string]value.Value), ts: time.Now(), cursor: 0}
}
func NewContextSimpleData(data map[string]value.Value) *ContextSimple {
	return &ContextSimple{Data: data, ts: time.Now(), cursor: 0}
}
func NewContextSimpleTs(data map[string]value.Value, ts time.Time) *ContextSimple {
	return &ContextSimple{Data: data, ts: ts, cursor: 0}
}

func (m *ContextSimple) All() map[string]value.Value { return m.Data }
func (m *ContextSimple) Row() map[string]value.Value { return m.Data }
func (m *ContextSimple) Body() interface{}           { return m }
func (m *ContextSimple) Key() uint64                 { return m.keyval }
func (m *ContextSimple) Ts() time.Time               { return m.ts }
func (m ContextSimple) Get(key string) (value.Value, bool) {
	val, ok := m.Data[key]
	return val, ok
}

func (m *ContextSimple) Put(col expr.SchemaInfo, rctx expr.ContextReader, v value.Value) error {
	//u.Infof("put context:  %v %T:%v", col.Key(), v, v)
	m.Data[col.Key()] = v
	return nil
}
func (m *ContextSimple) Commit(rowInfo []expr.SchemaInfo, row expr.RowWriter) error {
	//m.Rows = append(m.Rows, m.Data)
	//m.Data = make(map[string]value.Value)
	return nil
}
func (m *ContextSimple) Delete(row map[string]value.Value) error {
	return nil
}

type ContextWriterEmpty struct{}

func (m *ContextWriterEmpty) Put(col expr.SchemaInfo, rctx expr.ContextReader, v value.Value) error {
	return nil
}
func (m *ContextWriterEmpty) Delete(delRow map[string]value.Value) error { return nil }

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
func (m *ContextUrlValues) String() string {
	if m == nil || len(m.Data) == 0 {
		return ""
	}
	return m.Data.Encode()
}
func (m ContextUrlValues) Get(key string) (value.Value, bool) {
	vals, ok := m.Data[key]
	if ok {
		if len(vals) == 1 {
			return value.NewValue(vals[0]), true
		}
		return value.NewValue(vals), true
	}
	return value.EmptyStringValue, false
}
func (m ContextUrlValues) Row() map[string]value.Value {
	mi := make(map[string]value.Value)
	for k, v := range m.Data {
		if len(v) == 1 {
			mi[k] = value.NewValue(v[0])
		} else if len(v) > 1 {
			mi[k] = value.NewStringsValue(v)
		}
	}
	return mi
}
func (m *ContextUrlValues) Delete(delRow map[string]value.Value) error {
	return fmt.Errorf("Not implemented")
}
func (m ContextUrlValues) Ts() time.Time {
	return m.ts
}

func (m ContextUrlValues) Put(col expr.SchemaInfo, rctx expr.ContextReader, v value.Value) error {
	key := col.Key()
	switch typedValue := v.(type) {
	case value.StringValue:
		m.Data.Set(key, typedValue.ToString())
	case value.NumberValue:
		m.Data.Set(key, typedValue.ToString())
	}
	return nil
}
