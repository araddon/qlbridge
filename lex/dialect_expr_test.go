package lex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExprDialectInit(t *testing.T) {
	// Make sure we can init more than once, see if it panics
	ExpressionDialect.Init()
	for _, stmt := range ExpressionDialect.Statements {
		assert.NotEqual(t, "", stmt.String())
	}
}

func tokenexpr(lexString string, runLex StateFn) Token {
	l := NewLexer(lexString, ExpressionDialect)
	runLex(l)
	return l.NextToken()
}

func verifyExprTokens(t *testing.T, expString string, tokens []Token) {
	l := NewLexer(expString, ExpressionDialect)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		assert.Equal(t, tok.T, goodToken.T, "want='%v' has %v ", goodToken.T, tok.T)
		assert.Equal(t, tok.V, goodToken.V, "want='%v' has %v ", goodToken.V, tok.V)
	}
}
func verifyExpr2Tokens(t *testing.T, expString string, tokens []Token) {
	l := NewLexer(expString, LogicalExpressionDialect)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		assert.Equal(t, tok.T, goodToken.T, "want='%v' has %v ", goodToken.T, tok.T)
		assert.Equal(t, tok.V, goodToken.V, "want='%v' has %v ", goodToken.V, tok.V)
	}
}
func TestLexExprDialect(t *testing.T) {
	verifyExprTokens(t, `eq(toint(item),5)`,
		[]Token{
			tv(TokenUdfExpr, "eq"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "toint"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "item"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyExprTokens(t, `eq(@@varfive,5)`,
		[]Token{
			tv(TokenUdfExpr, "eq"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "@@varfive"),
			tv(TokenComma, ","),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
		})
}

func TestLexLogicalDialect(t *testing.T) {

	verifyExpr2Tokens(t, `4 > 5`,
		[]Token{
			tv(TokenInteger, "4"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "5"),
		})

	verifyExpr2Tokens(t, `item || 5`,
		[]Token{
			tv(TokenIdentity, "item"),
			tv(TokenOr, "||"),
			tv(TokenInteger, "5"),
		})

	verifyExpr2Tokens(t, `10 > 5`,
		[]Token{
			tv(TokenInteger, "10"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "5"),
		})
	verifyExpr2Tokens(t, `toint(10 * 5)`,
		[]Token{
			tv(TokenUdfExpr, "toint"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "10"),
			tv(TokenMultiply, "*"),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyExpr2Tokens(t, `6 == !eq(5,6)`,
		[]Token{
			tv(TokenInteger, "6"),
			tv(TokenEqualEqual, "=="),
			tv(TokenNegate, "!"),
			tv(TokenUdfExpr, "eq"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "5"),
			tv(TokenComma, ","),
			tv(TokenInteger, "6"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyExpr2Tokens(t, `(4 + 5)/2`,
		[]Token{
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "4"),
			tv(TokenPlus, "+"),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenDivide, "/"),
			tv(TokenInteger, "2"),
		})

	verifyExpr2Tokens(t, `(4.5 + float(5))/2`,
		[]Token{
			tv(TokenLeftParenthesis, "("),
			tv(TokenFloat, "4.5"),
			tv(TokenPlus, "+"),
			tv(TokenUdfExpr, "float"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenDivide, "/"),
			tv(TokenInteger, "2"),
		})
}
