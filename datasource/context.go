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
	_ expr.ContextReader = (*SqlDriverMessageMap)(nil)
	_ Message            = (*SqlDriverMessageMap)(nil)
	_ expr.ContextWriter = (*ContextUrlValues)(nil)
	_ expr.ContextReader = (*ContextUrlValues)(nil)
	_ Message            = (*ContextUrlValues)(nil)
	_                    = u.EMPTY
)

// represents a message, the Key() method provides a consistent uint64 which
// can be used by consistent-hash algorithms for topologies that split messages
// up amongst multiple machines
//
// Body()  returns interface allowing this to be generic structure for routing
//
// see  "https://github.com/mdmarek/topo" AND http://github.com/lytics/grid
//
type Message interface {
	Key() uint64
	Body() interface{}
}

type SqlDriverMessage struct {
	Vals []driver.Value
	Id   uint64
}

func (m *SqlDriverMessage) Key() uint64       { return m.Id }
func (m *SqlDriverMessage) Body() interface{} { return m.Vals }

type SqlDriverMessageMap struct {
	row      []driver.Value
	colindex map[string]int
	Id       uint64
}

func NewSqlDriverMessageMapEmpty() *SqlDriverMessageMap {
	return &SqlDriverMessageMap{}
}
func NewSqlDriverMessageMap(id uint64, row []driver.Value, colindex map[string]int) *SqlDriverMessageMap {
	return &SqlDriverMessageMap{Id: id, colindex: colindex, row: row}
}
func NewSqlDriverMessageMapVals(id uint64, row []driver.Value, cols []string) *SqlDriverMessageMap {
	if len(row) != len(cols) {
		u.Errorf("Wrong row/col count: %v  vs %v", cols, row)
	}
	colindex := make(map[string]int, len(row))
	for i, _ := range row {
		colindex[cols[i]] = i
	}
	return &SqlDriverMessageMap{Id: id, colindex: colindex, row: row}
}

func (m *SqlDriverMessageMap) Key() uint64               { return m.Id }
func (m *SqlDriverMessageMap) Body() interface{}         { return m }
func (m *SqlDriverMessageMap) Values() []driver.Value    { return m.row }
func (m *SqlDriverMessageMap) SetRow(row []driver.Value) { m.row = row }
func (m *SqlDriverMessageMap) Ts() time.Time             { return time.Time{} }
func (m *SqlDriverMessageMap) Get(key string) (value.Value, bool) {
	if idx, ok := m.colindex[key]; ok {
		return value.NewValue(m.row[idx]), true
	}
	u.Warnf("could not find: %v in %#v", key, m.colindex)
	return value.ErrValue, false
}
func (m *SqlDriverMessageMap) Row() map[string]value.Value {
	row := make(map[string]value.Value)
	for k, idx := range m.colindex {
		row[k] = value.NewValue(m.row[idx])
	}
	return row
}

type ValueContextWrapper struct {
	*SqlDriverMessage
	cols map[string]*expr.Column
}

func NewValueContextWrapper(msg *SqlDriverMessage, cols map[string]*expr.Column) *ValueContextWrapper {
	return &ValueContextWrapper{msg, cols}
}
func (m *ValueContextWrapper) Get(key string) (value.Value, bool) {
	if col, ok := m.cols[key]; ok {
		if col.Index < len(m.Vals) {
			return value.NewValue(m.Vals[col.Index]), true
		}
		//u.Debugf("could not find index?: %v col.idx:%v   len(vals)=%v", key, col.Index, len(m.Vals))
	} else {
		//u.Debugf("could not find key: %v", key)
	}
	return value.ErrValue, false
}
func (m *ValueContextWrapper) Row() map[string]value.Value {
	row := make(map[string]value.Value)
	for _, col := range m.cols {
		if col.Index <= len(m.Vals) {
			row[col.Key()] = value.NewValue(m.Vals[col.Index])
		}
	}
	return row
}
func (m *ValueContextWrapper) Ts() time.Time { return time.Time{} }

type UrlValuesMsg struct {
	id   uint64
	body *ContextUrlValues
}

func NewUrlValuesMsg(id uint64, body *ContextUrlValues) *UrlValuesMsg {
	return &UrlValuesMsg{id, body}
}

func (m *UrlValuesMsg) Key() uint64       { return m.id }
func (m *UrlValuesMsg) Body() interface{} { return m.body }
func (m *UrlValuesMsg) String() string    { return m.body.String() }

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
	id   uint64
	Data url.Values
	ts   time.Time
}

func NewContextUrlValues(uv url.Values) *ContextUrlValues {
	return &ContextUrlValues{0, uv, time.Now()}
}
func NewContextUrlValuesTs(uv url.Values, ts time.Time) *ContextUrlValues {
	return &ContextUrlValues{0, uv, ts}
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
func (m *ContextUrlValues) Key() uint64       { return m.id }
func (m *ContextUrlValues) Body() interface{} { return m }

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
