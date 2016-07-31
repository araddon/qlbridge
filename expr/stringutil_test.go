package expr

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestIdentityQuoting(t *testing.T) {
	t.Parallel()

	assert.Equal(t, IdentityMaybeQuote('"', `na"me`), `"na""me"`)
	assert.Equal(t, IdentityMaybeQuote('"', "1name"), `"1name"`)
	assert.Equal(t, IdentityMaybeQuote('"', "name"), `name`) // don't escape
	assert.Equal(t, IdentityMaybeQuote('"', "na me"), `"na me"`)
	assert.Equal(t, IdentityMaybeQuote('"', "na#me"), `"na#me"`)

	assert.Equal(t, IdentityMaybeQuote('`', "namex"), "namex")
	assert.Equal(t, IdentityMaybeQuote('`', "1name"), "`1name`")
	assert.Equal(t, IdentityMaybeQuote('`', "na`me"), "`na``me`")

	assert.Equal(t, IdentityMaybeQuote('`', "space name"), "`space name`")
	assert.Equal(t, IdentityMaybeQuote('`', "space name"), "`space name`")

	assert.Equal(t, IdentityMaybeQuoteStrict('`', "_uid"), "`_uid`")
}

func TestLeftRight(t *testing.T) {
	t.Parallel()
	l, r, hasLeft := LeftRight("`table`.`column`")
	assert.Tf(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.Tf(t, r == "column", "no quote: %s", l)
	l, r, hasLeft = LeftRight("`table.column`")
	assert.Tf(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.Tf(t, r == "column", "no quote: %s", l)
}
