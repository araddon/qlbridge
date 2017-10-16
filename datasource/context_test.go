package datasource_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type col struct {
	k string
}

func (m *col) Key() string {
	return m.k
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
	assert.True(t, ok, "expected key:%s =%v", key, expected.Value())
	if val == nil {
		t.Errorf("not value for %v", key)
	} else {
		assert.Equal(t, expected.Value(), val.Value(), "%s expected: %v  got:%v", key, expected.Value(), val.Value())
	}
}
