package lex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToken(t *testing.T) {
	l := NewSqlLexer("SELECT X FROM Y;")
	tok := l.NextToken()
	err := tok.Err(l)
	assert.NotEqual(t, "", err.Error())
	err = tok.ErrMsg(l, "not now")
	assert.NotEqual(t, "", err.Error())

	tok = TokenFromOp("select")
	assert.Equal(t, TokenSelect, tok.T)
	tok = TokenFromOp("noway")
	assert.Equal(t, TokenNil, tok.T)
}
