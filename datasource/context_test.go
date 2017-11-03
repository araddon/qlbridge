package datasource_test

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

type col struct {
	k string
}

func (m *col) Key() string {
	return m.k
}

var (
	vals   = []driver.Value{1, "name", time.Now()}
	cols   = []string{"id", "name", "time"}
	colidx = map[string]int{"id": 0, "name": 1, "time": 2}
)

func allContexts() []expr.ContextReader {
	ctx := make([]expr.ContextReader, 3)
	ctx[0] = datasource.NewSqlDriverMessageMap(0, vals, colidx)
	ctx[1] = datasource.NewSqlDriverMessageMapVals(0, vals, cols)
	ctx[2] = datasource.NewSqlDriverMessageMapCtx(0, ctx[0], colidx)
	return ctx
}
func allMessages() []schema.Message {
	msg := make([]schema.Message, 4)
	ctx := datasource.NewSqlDriverMessageMapVals(0, vals, cols)
	msg[0] = datasource.NewSqlDriverMessageMap(0, vals, colidx)
	msg[1] = datasource.NewSqlDriverMessageMapVals(0, vals, cols)
	msg[2] = datasource.NewSqlDriverMessageMapCtx(0, ctx, colidx)
	msg[3] = datasource.NewSqlDriverMessage(0, vals)
	return msg
}
func TestContext(t *testing.T) {

	msgs := allMessages()
	for _, msg := range msgs {
		t.Run("message", MessageInterface(msg))
	}

	ec := datasource.NewSqlDriverMessageMapEmpty()
	t.Run("sqldriverempty", MessageInterface(ec))

	ec = datasource.NewSqlDriverMessageMapVals(0, nil, cols)
	assert.Equal(t, 0, len(ec.Values()))

	ctxall := allContexts()
	for _, ctx := range ctxall {
		t.Run("test.context", ContextReader(ctx))
	}

	vals := make([]interface{}, len(ctxall))
	for i, ctx := range ctxall {
		vals[i] = ctx
	}
	// make sure it doesn't panic
	datasource.MessageConversion(vals)
}
func ContextReader(ctx expr.ContextReader) func(t *testing.T) {
	return func(t *testing.T) {
		keyok := func(key string) {
			v, ok := ctx.Get(key)
			assert.Equal(t, true, ok, "expected ok for %v %#v", key, ctx)
			assert.NotEqual(t, nil, v)
		}
		if msg, ok := ctx.(schema.Message); ok {
			assert.Equal(t, uint64(0), msg.Id())
			assert.NotEqual(t, nil, msg.Body())
		}
		for _, key := range cols {
			keyok(key)
		}
	}
}
func MessageInterface(msg schema.Message) func(t *testing.T) {
	return func(t *testing.T) {
		if msg, ok := msg.(schema.Message); ok {
			assert.Equal(t, uint64(0), msg.Id())
			assert.NotEqual(t, nil, msg.Body())
		}
	}
}
func TestNested(t *testing.T) {

	a1 := value.NewStringValue("a1")
	b1 := value.NewStringValue("b1")
	c1 := value.NewStringValue("c1")
	d1 := value.NewStringValue("d1")
	readers := []expr.ContextReader{
		datasource.NewContextSimpleData(map[string]value.Value{
			"a": a1,
			"b": b1,
		}),
		datasource.NewContextSimpleData(map[string]value.Value{
			"b": value.NewStringValue("b2"),
			"c": c1,
		}),
		datasource.NewContextSimpleData(map[string]value.Value{
			"c": value.NewStringValue("b2"),
			"d": d1,
		}),
	}

	w := datasource.NewContextSimple()
	nc := datasource.NewNestedContextReadWriter(readers, w, time.Now())
	expected := map[string]value.Value{
		"a": a1,
		"b": b1,
		"c": c1,
		"d": d1,
	}

	for k, v := range expected {
		checkval(t, nc, k, v)
	}
	r := nc.Row()
	assert.Equal(t, len(expected), len(r))
	for k, v := range expected {
		assert.Equal(t, v, r[k])
	}

	nc.Put(&col{k: "e"}, nil, value.NewStringValue("e1"))
	v, ok := nc.Get("e")
	assert.Equal(t, true, ok)
	assert.Equal(t, v.ToString(), "e1")
}

func TestNamespaces(t *testing.T) {

	a1 := value.NewStringValue("a1")
	b1 := value.NewStringValue("b1")
	b2 := value.NewStringValue("b2")
	c1 := value.NewStringValue("c1")
	d1 := value.NewStringValue("d1")
	readers := []expr.ContextReader{
		datasource.NewNamespacedContextReader(datasource.NewContextSimpleData(map[string]value.Value{
			"a": a1,
			"b": b1,
			"d": d1,
		}), "foo"),
		datasource.NewNamespacedContextReader(datasource.NewContextSimpleData(map[string]value.Value{
			"b": b2,
			"c": c1,
		}), "BAR"),
		datasource.NewContextSimpleData(map[string]value.Value{
			"a": a1,
		}),
	}

	nc := datasource.NewNestedContextReader(readers, time.Now())
	expected := map[string]value.Value{
		"foo.a": a1,
		"foo.b": b1,
		"foo.d": d1,
		"bar.b": b2,
		"bar.c": c1,
		"a":     a1,
	}

	for k, v := range expected {
		checkval(t, nc, k, v)
	}
	r := nc.Row()
	assert.Equal(t, len(expected), len(r))
	for k, v := range expected {
		assert.Equal(t, v, r[k])
	}

	// _, ok := nc.Get("no")
	// assert.Equal(t, false, ok)
}

func checkval(t *testing.T, r expr.ContextReader, key string, expected value.Value) {
	val, ok := r.Get(key)
	if val == nil {
		if expected == nil || expected.Type() == value.NilType {
			// ok
		} else {
			t.Errorf("not value for %v expected: %#v actual: %#v", key, expected, val)
		}
	} else {
		assert.True(t, ok, "expected key:%s =%v", key, expected.Value())
		assert.Equal(t, expected.Value(), val.Value(), "%s expected: %v  got:%v", key, expected.Value(), val.Value())
	}
}
