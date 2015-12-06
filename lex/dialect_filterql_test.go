package lex

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
)

var _ = u.EMPTY

func verifyFilterQLTokens(t *testing.T, ql string, tokens []Token) {
	l := NewFilterQLLexer(ql)
	u.Debugf("filterql: %v", ql)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok.T, goodToken.T, "want='%v' has %v %v", goodToken.T, tok.T, l.PeekX(10))
		assert.Equalf(t, tok.V, goodToken.V, "want='%v' has %v ", goodToken.V, tok.V)
	}
}

func TestFilterQLBasic(t *testing.T) {

	verifyFilterQLTokens(t, `
    FILTER AND (
          -- Lets make sure the date is good
          daysago(datefield) < 100
          -- as well as domain
          , domain(url) == "google.com"
          INCLUDE my_other_named_filter
          EXISTS my_field
          , OR (
              momentum > 20
             , propensity > 50
          )
          , NOT score > 20
       )
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " Lets make sure the date is good"),
			tv(TokenNewLine, ""),
			tv(TokenUdfExpr, "daysago"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "datefield"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenLT, "<"),
			tv(TokenInteger, "100"),
			tv(TokenNewLine, ""),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " as well as domain"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenUdfExpr, "domain"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "url"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEqualEqual, "=="),
			tv(TokenValue, "google.com"),
			tv(TokenNewLine, ""),
			tv(TokenInclude, "INCLUDE"),
			tv(TokenIdentity, "my_other_named_filter"),
			tv(TokenNewLine, ""),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "my_field"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenOr, "OR"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenIdentity, "momentum"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenIdentity, "propensity"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "50"),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenNegate, "NOT"),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	verifyFilterQLTokens(t, `
    FILTER AND( score > 20 ) ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	verifyFilterQLTokens(t, `
    FILTER
      AND(score > 20)
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenNewLine, ""),
			tv(TokenAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	// Ensure we support trailing commas
	verifyFilterQLTokens(t, `
    FILTER AND (
      	score > 20 ,
      )
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenComma, ","),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	// Ensure we support new lines in
	verifyFilterQLTokens(t, `
    FILTER AND(
        score IN (20,
        30,
        60)
      )
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenIdentity, "score"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "20"),
			tv(TokenComma, ","),
			tv(TokenInteger, "30"),
			tv(TokenComma, ","),
			tv(TokenInteger, "60"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	// Now for a really simple naked filter
	verifyFilterQLTokens(t, `
    FILTER x > 5
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "x"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "5"),
		})

}
