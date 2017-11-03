package datasource

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"hash/fnv"
	"net/url"
	"strings"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure our ContextReaders implement interface
	// context-readers hold "State" for evaluation in vm.
	_ expr.ContextReader = (*ContextSimple)(nil)
	_ expr.ContextReader = (*SqlDriverMessageMap)(nil)
	_ expr.ContextReader = (*ContextUrlValues)(nil)
	// Context Writers hold write state of vm
	_ expr.ContextWriter = (*ContextUrlValues)(nil)
	_ expr.ContextWriter = (*ContextSimple)(nil)
	// Message is passed between tasks/actors across distributed
	// boundaries.   May be context reader/writer.
	_ schema.Message = (*ContextSimple)(nil)
	_ schema.Message = (*SqlDriverMessage)(nil)
	_ schema.Message = (*SqlDriverMessageMap)(nil)
	_ schema.Message = (*ContextUrlValues)(nil)
)

func MessageConversion(vals []interface{}) []schema.Message {
	msgs := make([]schema.Message, len(vals))
	for i, v := range vals {
		msgs[i] = v.(schema.Message)
	}
	return msgs
}

type (
	SqlDriverMessage struct {
		Vals  []driver.Value
		IdVal uint64
	}
	SqlDriverMessageMap struct {
		Vals     []driver.Value // Values
		ColIndex map[string]int // Map of column names to ordinal position in vals
		IdVal    uint64         // id()
		keyVal   string         // key   Non Hashed Key Value
	}
	MessageArray struct {
		Idv   uint64
		Items []*SqlDriverMessageMap
	}
	ValueContextWrapper struct {
		*SqlDriverMessage
		cols map[string]*rel.Column
	}
	UrlValuesMsg struct {
		id   uint64
		body *ContextUrlValues
	}
	ContextSimple struct {
		Data        map[string]value.Value
		ts          time.Time
		cursor      int
		keyval      uint64
		namespacing bool
	}
	ContextUrlValues struct {
		id   uint64
		Data url.Values
		ts   time.Time
	}
	ContextWriterEmpty  struct{}
	NestedContextReader struct {
		readers []expr.ContextReader
		writer  expr.ContextWriter
		ts      time.Time
	}
	NamespacedContextReader struct {
		basereader expr.ContextReader
		namespace  string
	}
)

func NewSqlDriverMessage(id uint64, row []driver.Value) *SqlDriverMessage {
	return &SqlDriverMessage{IdVal: id, Vals: row}
}
func (m *SqlDriverMessage) Id() uint64        { return m.IdVal }
func (m *SqlDriverMessage) Body() interface{} { return m.Vals }
func (m *SqlDriverMessage) ToMsgMap(colidx map[string]int) *SqlDriverMessageMap {
	return NewSqlDriverMessageMap(m.IdVal, m.Vals, colidx)
}

func NewSqlDriverMessageMapEmpty() *SqlDriverMessageMap {
	return &SqlDriverMessageMap{}
}
func NewSqlDriverMessageMap(id uint64, row []driver.Value, colindex map[string]int) *SqlDriverMessageMap {
	return &SqlDriverMessageMap{IdVal: id, ColIndex: colindex, Vals: row}
}
func NewSqlDriverMessageMapVals(id uint64, row []driver.Value, cols []string) *SqlDriverMessageMap {
	if len(row) != len(cols) {
		return &SqlDriverMessageMap{}
	}
	colindex := make(map[string]int, len(row))
	for i := range row {
		colindex[cols[i]] = i
	}
	return &SqlDriverMessageMap{IdVal: id, ColIndex: colindex, Vals: row}
}
func NewSqlDriverMessageMapCtx(id uint64, ctx expr.ContextReader, colindex map[string]int) *SqlDriverMessageMap {
	row := make([]driver.Value, len(colindex))
	for key, idx := range colindex {
		val, ok := ctx.Get(key)
		if ok {
			row[idx] = val.Value()
		}
	}
	return &SqlDriverMessageMap{IdVal: id, ColIndex: colindex, Vals: row}
}

func (m *SqlDriverMessageMap) Id() uint64        { return m.IdVal }
func (m *SqlDriverMessageMap) Key() string       { return m.keyVal }
func (m *SqlDriverMessageMap) SetKey(key string) { m.keyVal = key }
func (m *SqlDriverMessageMap) SetKeyHashed(key string) {
	m.keyVal = key
	// Do we want to use SipHash here
	hasher64 := fnv.New64()
	hasher64.Write([]byte(key))
	//idOld := m.IdVal
	m.IdVal = hasher64.Sum64()
	//u.Warnf("old:%v new:%v  set key hashed: %v", idOld, m.IdVal, m.row)
}
func (m *SqlDriverMessageMap) Body() interface{}         { return m }
func (m *SqlDriverMessageMap) Values() []driver.Value    { return m.Vals }
func (m *SqlDriverMessageMap) SetRow(row []driver.Value) { m.Vals = row }
func (m *SqlDriverMessageMap) Ts() time.Time             { return time.Time{} }
func (m *SqlDriverMessageMap) Get(key string) (value.Value, bool) {
	if idx, ok := m.ColIndex[key]; ok {
		return value.NewValue(m.Vals[idx]), true
	}
	_, right, hasLeft := expr.LeftRight(key)
	//u.Debugf("could not find: %q  right=%q hasLeftRight?%v", key, right, hasLeft)
	if hasLeft {
		if idx, ok := m.ColIndex[right]; ok {
			return value.NewValue(m.Vals[idx]), true
		}
	}
	return nil, false
}
func (m *SqlDriverMessageMap) Row() map[string]value.Value {
	row := make(map[string]value.Value)
	for k, idx := range m.ColIndex {
		row[k] = value.NewValue(m.Vals[idx])
	}
	return row
}
func (m *SqlDriverMessageMap) Copy() *SqlDriverMessageMap {
	nm := SqlDriverMessageMap{}
	nm.Vals = m.Vals // we assume? that values are immutable anyways
	nm.ColIndex = m.ColIndex
	nm.IdVal = m.IdVal
	nm.keyVal = m.keyVal
	return &nm
}

func (m *MessageArray) Id() uint64        { return m.Idv }
func (m *MessageArray) Body() interface{} { return m.Items }

func NewValueContextWrapper(msg *SqlDriverMessage, cols map[string]*rel.Column) *ValueContextWrapper {
	return &ValueContextWrapper{msg, cols}
}
func (m *ValueContextWrapper) Get(key string) (value.Value, bool) {
	if col, ok := m.cols[key]; ok {
		if col.Index < len(m.Vals) {
			return value.NewValue(m.Vals[col.Index]), true
		}
	}
	return nil, false
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

func NewUrlValuesMsg(id uint64, body *ContextUrlValues) *UrlValuesMsg {
	return &UrlValuesMsg{id, body}
}

func (m *UrlValuesMsg) Id() uint64        { return m.id }
func (m *UrlValuesMsg) Body() interface{} { return m.body }
func (m *UrlValuesMsg) String() string    { return m.body.String() }

func NewContextSimple() *ContextSimple {
	return &ContextSimple{Data: make(map[string]value.Value), ts: time.Now(), cursor: 0}
}
func NewContextSimpleData(data map[string]value.Value) *ContextSimple {
	return &ContextSimple{Data: data, ts: time.Now(), cursor: 0}
}
func NewContextSimpleNative(data map[string]interface{}) *ContextSimple {
	vals := make(map[string]value.Value)
	for k, v := range data {
		vals[k] = value.NewValue(v)
	}
	return &ContextSimple{Data: vals, ts: time.Now(), cursor: 0}
}
func NewContextMap(data map[string]interface{}, namespacing bool) *ContextSimple {
	vals := make(map[string]value.Value)
	for k, v := range data {
		vals[k] = value.NewValue(v)
	}
	return &ContextSimple{Data: vals, ts: time.Now(), cursor: 0, namespacing: namespacing}
}
func NewContextMapTs(data map[string]interface{}, namespacing bool, ts time.Time) *ContextSimple {
	vals := make(map[string]value.Value)
	for k, v := range data {
		vals[k] = value.NewValue(v)
	}
	return &ContextSimple{Data: vals, ts: ts, cursor: 0, namespacing: namespacing}
}
func NewContextSimpleTs(data map[string]value.Value, ts time.Time) *ContextSimple {
	return &ContextSimple{Data: data, ts: ts, cursor: 0}
}

func (m *ContextSimple) SupportNamespacing()         { m.namespacing = true }
func (m *ContextSimple) All() map[string]value.Value { return m.Data }
func (m *ContextSimple) Row() map[string]value.Value { return m.Data }
func (m *ContextSimple) Body() interface{}           { return m }
func (m *ContextSimple) Id() uint64                  { return m.keyval }
func (m *ContextSimple) Ts() time.Time               { return m.ts }
func (m ContextSimple) Get(key string) (value.Value, bool) {
	val, ok := m.Data[key]
	if !ok && m.namespacing {
		// We don't support namespacing by default?
		left, right, hasNamespace := expr.LeftRight(key)
		if !hasNamespace {
			return nil, false
		}
		val, ok = m.Data[left]
		if !ok {
			return nil, false
		}
		if mv, isMap := val.(value.Map); isMap {
			return mv.Get(right)
		}
		return nil, false
	}
	return val, ok
}

func (m *ContextSimple) Put(col expr.SchemaInfo, rctx expr.ContextReader, v value.Value) error {
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

func (m *ContextWriterEmpty) Put(col expr.SchemaInfo, rctx expr.ContextReader, v value.Value) error {
	return nil
}
func (m *ContextWriterEmpty) Delete(delRow map[string]value.Value) error { return nil }

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
	return nil, false
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
func (m *ContextUrlValues) Id() uint64        { return m.id }
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

// NewNestedContextReader provides a context reader which is a composite of ordered child readers
// the first reader with a key will be used
func NewNestedContextReader(readers []expr.ContextReader, ts time.Time) expr.ContextReader {
	return &NestedContextReader{readers, nil, ts}
}

// NewNestedContextReader provides a context reader which is a composite of ordered child readers
// the first reader with a key will be used
func NewNestedContextReadWriter(readers []expr.ContextReader, writer expr.ContextWriter, ts time.Time) expr.ContextReadWriter {
	if rw, ok := writer.(expr.ContextReader); ok {
		readers = append(readers, rw)
	}
	return &NestedContextReader{readers, writer, ts}
}

func (n *NestedContextReader) Get(key string) (value.Value, bool) {
	for _, r := range n.readers {
		val, ok := r.Get(key)
		if ok && val != nil {
			return val, ok
		}
	}
	return nil, false
}

func (n *NestedContextReader) Row() map[string]value.Value {
	current := make(map[string]value.Value)
	for _, r := range n.readers {
		for k, v := range r.Row() {
			// already added this key from a "higher priority" reader
			if _, ok := current[k]; ok {
				continue
			}

			current[k] = v
		}
	}

	return current
}

func (n *NestedContextReader) Ts() time.Time {
	return n.ts
}

func (n *NestedContextReader) Put(col expr.SchemaInfo, readCtx expr.ContextReader, v value.Value) error {
	if n.writer != nil {
		return n.writer.Put(col, readCtx, v)
	}
	return nil
}
func (n *NestedContextReader) Delete(delRow map[string]value.Value) error {
	if n.writer != nil {
		return n.writer.Delete(delRow)
	}
	return nil
}

// NewNestedContextReader provides a context reader which prefixes
// all keys with a name space.  This is useful if you have overlapping
// field names between ContextReaders within a NestedContextReader.
//
//      msg.Get("foo.key")
//
func NewNamespacedContextReader(basereader expr.ContextReader, namespace string) expr.ContextReader {
	if namespace == "" {
		return basereader
	}
	return &NamespacedContextReader{basereader, strings.ToLower(namespace)}
}

func (n *NamespacedContextReader) Get(key string) (value.Value, bool) {
	left, right, has := expr.LeftRight(key)
	if !has || strings.ToLower(left) != n.namespace {
		return nil, false
	}

	return n.basereader.Get(right)
}

func (n *NamespacedContextReader) Row() map[string]value.Value {
	wraprow := make(map[string]value.Value)
	sb := bytes.Buffer{}

	for k, v := range n.basereader.Row() {
		sb.Reset()
		sb.WriteString(n.namespace)
		sb.WriteString(".")
		sb.WriteString(k)
		wraprow[sb.String()] = v
	}

	return wraprow
}

func (n *NamespacedContextReader) Ts() time.Time {
	return n.basereader.Ts()
}
