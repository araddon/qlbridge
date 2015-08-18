package lex

import (
	//u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
)

func verifyJsonTokenTypes(t *testing.T, expString string, tokens []TokenType) {
	l := NewJsonLexer(expString)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok.T, goodToken, "want='%v' has %v ", goodToken, tok)
	}
}

func verifyJsonTokens(t *testing.T, expString string, tokens []Token) {
	l := NewJsonLexer(expString)
	for i, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok.T, goodToken.T, "%d want token type ='%v' has %v ", i, goodToken.T, tok.T)
		assert.Equalf(t, tok.V, goodToken.V, "%d want token value='%v' has %v ", i, goodToken.V, tok.V)
	}
}

func TestLexJsonTokens(t *testing.T) {
	verifyJsonTokens(t, `["a",2,"b",true,{"name":"world"}]`,
		[]Token{
			tv(TokenLeftBracket, "["),
			tv(TokenValue, "a"),
			tv(TokenComma, ","),
			tv(TokenInteger, "2"),
			tv(TokenComma, ","),
			tv(TokenValue, "b"),
			tv(TokenComma, ","),
			tv(TokenBool, "true"),
			tv(TokenComma, ","),
			tv(TokenLeftBrace, "{"),
			tv(TokenIdentity, "name"),
			tv(TokenColon, ":"),
			tv(TokenValue, "world"),
			tv(TokenRightBrace, "}"),
			tv(TokenRightBracket, "]"),
		})
}

func TestLexJsonDialect(t *testing.T) {
	// The lexer should be able to parse json
	verifyJsonTokenTypes(t, `
		{
			"key1":"value2"
			,"key2":45, 
			"key3":["a",2,"b",true],
			"key4":{"hello":"value","age":55}
		}
		`,
		[]TokenType{TokenLeftBrace,
			TokenIdentity, TokenColon, TokenValue,
			TokenComma,
			TokenIdentity, TokenColon, TokenInteger,
			TokenComma,
			TokenIdentity, TokenColon, TokenLeftBracket, TokenValue, TokenComma, TokenInteger, TokenComma, TokenValue, TokenComma, TokenBool, TokenRightBracket,
			TokenComma,
			TokenIdentity, TokenColon, TokenLeftBrace, TokenIdentity, TokenColon, TokenValue, TokenComma, TokenIdentity, TokenColon, TokenInteger, TokenRightBrace,
			TokenRightBrace,
		})
}
