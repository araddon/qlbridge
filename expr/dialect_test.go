package expr

import (
	"encoding/json"
	"testing"

	"github.com/araddon/dateparse"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/value"
)

type testDialect struct {
	t      lex.Token
	expect string
}

func TestDialectIdentityWriting(t *testing.T) {

	for _, td := range []testDialect{
		{lex.Token{V: "name"}, "name"},
		{lex.Token{Quote: '`', V: "has.period"}, "`has.period`"},
		{lex.Token{Quote: '`', V: "has`.`period"}, "has.period"},
		{lex.Token{V: "has space"}, "`has space`"},
	} {
		dw := NewDefaultWriter()
		in := NewIdentityNode(&td.t)
		in.WriteDialect(dw)
		assert.Equal(t, td.expect, dw.String())
	}

	for _, td := range []testDialect{
		{lex.Token{V: "name"}, "name"},
		{lex.Token{Quote: '`', V: "has.period"}, "'has.period'"},
		{lex.Token{V: "has space"}, "'has space'"},
	} {
		dw := NewDialectWriter('"', '\'')
		in := NewIdentityNode(&td.t)
		in.WriteDialect(dw)
		assert.Equal(t, td.expect, dw.String())
	}

	for _, td := range []testDialect{
		{lex.Token{V: "name"}, "name"},
		{lex.Token{Quote: '`', V: "has.period"}, "[has.period]"},
		{lex.Token{V: "has space"}, "[has space]"},
	} {
		dw := NewDialectWriter('"', '[')
		in := NewIdentityNode(&td.t)
		in.WriteDialect(dw)
		assert.Equal(t, td.expect, dw.String())
	}
	// strip Namespaces
	for _, td := range []testDialect{
		{lex.Token{V: "name"}, "name"},
		{lex.Token{Quote: '`', V: "table_name`.`fieldname"}, "fieldname"},
		{lex.Token{V: "has space"}, "`has space`"},
	} {
		dw := NewDefaultNoNamspaceWriter()
		in := NewIdentityNode(&td.t)
		in.WriteDialect(dw)
		assert.Equal(t, td.expect, dw.String())
	}
}
func TestDialectValueWriting(t *testing.T) {
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
