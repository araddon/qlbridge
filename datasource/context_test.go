package datasource

import (
	"testing"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/bmizerany/assert"
)

func TestNested(t *testing.T) {

	a1 := value.NewStringValue("a1")
	b1 := value.NewStringValue("b1")
	c1 := value.NewStringValue("c1")
	d1 := value.NewStringValue("d1")
	readers := []expr.ContextReader{
		NewContextSimpleData(map[string]value.Value{
			"a": a1,
			"b": b1,
		}),
		NewContextSimpleData(map[string]value.Value{
			"b": value.NewStringValue("b2"),
			"c": c1,
		}),
		NewContextSimpleData(map[string]value.Value{
			"c": value.NewStringValue("b2"),
			"d": d1,
		}),
	}

	nc := NewNestedContextReader(readers, time.Now())
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

	// _, ok := nc.Get("no")
	// assert.Equal(t, false, ok)
}

func TestNamespaces(t *testing.T) {

	a1 := value.NewStringValue("a1")
	b1 := value.NewStringValue("b1")
	b2 := value.NewStringValue("b2")
	c1 := value.NewStringValue("c1")
	d1 := value.NewStringValue("d1")
	readers := []expr.ContextReader{
		NewNamespacedContextReader(NewContextSimpleData(map[string]value.Value{
			"a": a1,
			"b": b1,
			"d": d1,
		}), "foo"),
		NewNamespacedContextReader(NewContextSimpleData(map[string]value.Value{
			"b": b2,
			"c": c1,
		}), "bar"),
	}

	nc := NewNestedContextReader(readers, time.Now())
	expected := map[string]value.Value{
		"foo.a": a1,
		"foo.b": b1,
		"foo.d": d1,
		"bar.b": b2,
		"bar.c": c1,
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
	assert.Tf(t, ok, "expected key:%s =%v", key, expected.Value())
	if val == nil {
		t.Errorf("not value for %v", key)
	} else {
		assert.Equalf(t, expected.Value(), val.Value(), "%s expected: %v  got:%v", key, expected.Value(), val.Value())
	}
}
