package value_test

import (
	"testing"
	"time"

	. "github.com/araddon/qlbridge/value"
)

// TestNewValue tests the conversion from Go types to Values.
func TestNewValue(t *testing.T) {
	v := NewValue(int(1))
	if _, ok := v.(IntValue); !ok {
		t.Errorf("Expected IntValue but received %T", v)
	}

	v = NewValue(float64(1.0))
	if _, ok := v.(NumberValue); !ok {
		t.Errorf("Expected NumberValue but received %T", v)
	}

	v = NewValue(true)
	if v, ok := v.(BoolValue); !ok {
		t.Errorf("Expected BoolValue but received %T", v)
	}

	v = NewValue(map[string]bool{"foo": false})
	if _, ok := v.(MapBoolValue); !ok {
		t.Errorf("Expected MapBoolValue but received %T", v)
	}

	// Make sure an unknown type is converted to a StructValue
	v = NewValue(struct{ Foo string }{})
	if _, ok := v.(StructValue); !ok {
		t.Errorf("Expected StructValue but received %T", v)
	}

	// Make sure Values are just roundtripped
	v2 := NewTimeValue(time.Now())
	if v3 := NewValue(v2); v2 != v3 {
		t.Errorf("Expected %T but received %T", v2, v3)
	}
}
