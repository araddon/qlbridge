package expr

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestIdentityQuoting(t *testing.T) {
	t.Parallel()

	out := IdentityMaybeQuote('`', "namex")
	assert.Tf(t, out == "namex", "no quote: %s", out)

	out = IdentityMaybeQuote('`', "space name")
	assert.Tf(t, out == "`space name`", "no quote: %s", out)

	out = IdentityMaybeQuoteStrict('`', "_uid")
	assert.Tf(t, out == "`_uid`", "no quote: %s", out)

}

func TestLeftRight(t *testing.T) {
	t.Parallel()
	l, r, hasLeft := LeftRight("`table`.`column`")
	assert.Tf(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.Tf(t, r == "column", "no quote: %s", l)

	l, r, hasLeft = LeftRight("`table.column`")
	assert.Tf(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.Tf(t, r == "column", "no quote: %s", l)

	// Un-escaped
	l, r, hasLeft = LeftRight("table.column")
	assert.Tf(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.Tf(t, r == "column", "no quote: %s", l)

	l, r, hasLeft = LeftRight("table.col.with.periods")
	assert.Tf(t, l == "table" && hasLeft, "no quote: %s", l)
	assert.Tf(t, r == "col.with.periods", "no quote: %s", l)

	l, r, hasLeft = LeftRight("`table.name`.`has.period`")
	assert.Tf(t, l == "table.name" && hasLeft, "recognize `left`.`right`: %s", l)
	assert.Tf(t, r == "has.period", "no quote: %s", l)
}
