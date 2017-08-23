package expr

import (
	"encoding/json"
	"testing"

	"github.com/araddon/dateparse"
	"github.com/araddon/qlbridge/value"
	"github.com/stretchr/testify/assert"
)

func TestDialectWriting(t *testing.T) {

	for _, tc := range [][]string{
		{"name", "name"},
		{`has.period`, "`has.period`"},
		{"has space", "`has space`"},
	} {
		dw := NewDefaultWriter()
		dw.WriteIdentity(tc[0])
		assert.Equal(t, tc[1], dw.String())
	}
	for _, tc := range [][]string{
		{"name", "name"},
		{`has.period`, "'has.period'"},
		{"has space", "'has space'"},
	} {
		dw := NewDialectWriter('"', '\'')
		dw.WriteIdentity(tc[0])
		assert.Equal(t, tc[1], dw.String())
	}
	for _, tc := range [][]string{
		{"name", "name"},
		{`has.period`, "[has.period]"},
		{"has space", "[has space]"},
	} {
		dw := NewDialectWriter('"', '[')
		dw.WriteIdentity(tc[0])
		assert.Equal(t, tc[1], dw.String())
	}

	// Test value writing
	for _, tc := range []struct {
		in  value.Value
		out string
	}{
		{value.NewValue(true), "true"},
		{value.NewValue(22.2), "22.2"},
		{value.NewValue(dateparse.MustParse("2017/08/08")), "\"2017-08-08 00:00:00 +0000 UTC\""},
		{value.NewValue(22), "22"},
		{value.NewValue("world"), `"world"`},
		{value.NewValue(json.RawMessage(`{"name":"world"}`)), `{"name":"world"}`},
	} {
		dw := NewDialectWriter('"', '[')
		dw.WriteValue(tc.in)
		assert.Equal(t, tc.out, dw.String())
	}
}
