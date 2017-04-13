package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdentityQuoting(t *testing.T) {
	t.Parallel()

	assert.Equal(t, IdentityMaybeQuote('"', `na"me`), `"na""me"`)
	assert.Equal(t, IdentityMaybeQuote('"', "1name"), `"1name"`)
	assert.Equal(t, IdentityMaybeQuote('"', "name"), `name`) // don't escape
	assert.Equal(t, IdentityMaybeQuote('"', "na me"), `"na me"`)
	assert.Equal(t, IdentityMaybeQuote('"', "na#me"), `"na#me"`)
	// already escaped
	assert.Equal(t, IdentityMaybeQuote('"', `"name"`), `"name"`)  // don't escape
	assert.Equal(t, IdentityMaybeQuote('\'', "'name'"), `'name'`) // don't escape
	assert.Equal(t, IdentityMaybeQuote('`', "`name`"), "`name`")  // don't escape

	assert.Equal(t, IdentityMaybeQuote('`', "namex"), "namex")
	assert.Equal(t, IdentityMaybeQuote('`', "1name"), "`1name`")
	assert.Equal(t, IdentityMaybeQuote('`', "na`me"), "`na``me`")

	assert.Equal(t, IdentityMaybeQuote('`', "space name"), "`space name`")
	assert.Equal(t, IdentityMaybeQuote('`', "space name"), "`space name`")

	assert.Equal(t, IdentityMaybeQuoteStrict('`', "_uid"), "`_uid`")

	assert.Equal(t, IdentityMaybeQuote('`', "\xe2\x00"), "`\xe2\x00`")
	assert.Equal(t, IdentityMaybeQuote('`', "a\xe2\x00"), "`a\xe2\x00`")
}

func TestLiteralEscaping(t *testing.T) {
	t.Parallel()

	assert.Equal(t, LiteralQuoteEscape('"', `na"me`), `"na""me"`)
	assert.Equal(t, LiteralQuoteEscape('"', "1name"), `"1name"`)
	assert.Equal(t, LiteralQuoteEscape('"', "na me"), `"na me"`)
	assert.Equal(t, LiteralQuoteEscape('"', "na#me"), `"na#me"`)
	// already escaped
	assert.Equal(t, LiteralQuoteEscape('"', `"name"`), `"name"`)  // don't escape
	assert.Equal(t, LiteralQuoteEscape('\'', "'name'"), `'name'`) // don't escape

	assert.Equal(t, LiteralQuoteEscape('\'', "namex"), "'namex'")
	assert.Equal(t, LiteralQuoteEscape('\'', "1name"), "'1name'")
	assert.Equal(t, LiteralQuoteEscape('\'', "na'me"), "'na''me'")

	assert.Equal(t, LiteralQuoteEscape('\'', "space name"), "'space name'")
	assert.Equal(t, LiteralQuoteEscape('\'', "space name"), "'space name'")

	newVal, wasUnEscaped := StringUnEscape('"', `Toys R\" Us`)
	assert.Equal(t, true, wasUnEscaped)
	assert.Equal(t, newVal, `Toys R" Us`)
	newVal, wasUnEscaped = StringUnEscape('"', `Toys R"" Us`)
	assert.Equal(t, true, wasUnEscaped)
	assert.Equal(t, newVal, `Toys R" Us`)
}

func TestLeftRight(t *testing.T) {
	t.Parallel()
	l, r, hasLeft := LeftRight("`table`.`column`")
	assert.True(t, l == "table" && hasLeft, "left, right, w quote: %s", l)
	assert.True(t, r == "column", "left, right, w quote & ns %s", l)

	l, r, hasLeft = LeftRight("`table.column`")
	assert.True(t, l == "" && !hasLeft, "no left bc escaped %s", l)
	assert.True(t, r == "table.column", "%s", l)

	// Un-escaped
	l, r, hasLeft = LeftRight("table.column")
	assert.True(t, l == "table" && hasLeft, "left, right no quote: %s", l)
	assert.True(t, r == "column", "left,right, no quote: %s", l)

	// Not sure i want to support this, legacy reasons we stupidly
	// allowed the left most part before the first period to be the
	// left, and the rest to be right.  Now we should ??? no left?
	l, r, hasLeft = LeftRight("table.col.with.periods")
	assert.True(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.True(t, r == "col.with.periods", "no quote: %s", l)

	l, r, hasLeft = LeftRight("`table.name`.`has.period`")
	assert.True(t, l == "table.name" && hasLeft, "recognize `left`.`right`: %s", l)
	assert.True(t, r == "has.period", "no quote: %s", l)
}
