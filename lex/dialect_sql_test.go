package lex

import (
	"testing"
)

func TestLexSqlDescribe(t *testing.T) {
	/*
		describe myidentity
	*/
	verifyTokens(t, `DESCRIBE mytable;`,
		[]Token{
			tv(TokenDescribe, "DESCRIBE"),
			tv(TokenIdentity, "mytable"),
		})
	verifyTokens(t, `DESC mytable;`,
		[]Token{
			tv(TokenDesc, "DESC"),
			tv(TokenIdentity, "mytable"),
		})
}

func TestLexSqlShow(t *testing.T) {
	/*
		show myidentity
	*/
	verifyTokens(t, `SHOW mytable;`,
		[]Token{
			tv(TokenShow, "SHOW"),
			tv(TokenIdentity, "mytable"),
		})

	verifyTokenTypes(t, "SHOW FULL TABLES FROM `ourschema` LIKE '%'",
		[]TokenType{TokenShow,
			TokenFull, TokenTables,
			TokenFrom, TokenIdentity, TokenLike, TokenValue,
		})
}
