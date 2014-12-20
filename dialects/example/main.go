package main

import (
	"fmt"
	ql "github.com/araddon/qlbridge/lex"
	"strings"
)

/*
	This example is meant to show how to create a new
	Dialect/Query Language with a keyword"SUBSCRIBETO"
	then Lex/Consume query

*/
var (
	// We need a token to recognize our "SUBSCRIBETO" keyword
	// in our PUBSUB dialect
	TokenSubscribeTo ql.TokenType = 1000

	// We are going to create our own Dialect now
	//  that uses a "SUBSCRIBETO" keyword
	pubsub = &ql.Statement{TokenSubscribeTo, []*ql.Clause{
		{Token: TokenSubscribeTo, Lexer: ql.LexColumns},
		{Token: ql.TokenFrom, Lexer: LexMaybe},
		{Token: ql.TokenWhere, Lexer: ql.LexColumns, Optional: true},
	}}
	ourDialect = &ql.Dialect{
		"Subscribe To", []*ql.Statement{pubsub},
	}
)

func init() {
	// inject any new tokens into QLBridge.Lex describing the custom tokens we created
	ql.TokenNameMap[TokenSubscribeTo] = &ql.TokenInfo{Description: "subscribeto"}

	// OverRide the Identity Characters in lexer to allow a dash in identity
	ql.IDENTITY_CHARS = "_./-"
	ql.LoadTokenInfo()

	ourDialect.Init()
}

func verifyLexerTokens(l *ql.Lexer, tokens []ql.Token) {
	for _, goodToken := range tokens {
		tok := l.NextToken()
		if tok.T != goodToken.T || tok.V != goodToken.V {
			panic(fmt.Sprintf("bad token: %v but wanted %v\n", tok, goodToken))
		} else {
			fmt.Printf("Got good token: %v\n", tok)
		}
	}
}

// Custom lexer for our maybe hash function
//
//  SUBSCRIBE
//       valuect(item) AS stuff
//  FROM maybe(stuff)
//  WHERE x = y
//
func LexMaybe(l *ql.Lexer) ql.StateFn {

	l.SkipWhiteSpaces()

	keyWord := strings.ToLower(l.PeekWord())

	switch keyWord {
	case "maybe":
		l.ConsumeWord("maybe")
		l.Emit(ql.TokenIdentity)
		return ql.LexExpressionOrIdentity
	}
	return ql.LexExpressionOrIdentity
}

func Tok(tok ql.TokenType, val string) ql.Token { return ql.Token{tok, val, 0} }

func main() {

	/* Many *ql languages support some type of columnar layout such as:
	   name = value, name2 = value2
	*/
	l := ql.NewLexer(`
				SUBSCRIBETO
					count(x), Name
				FROM ourstream
				WHERE 
					k = REPLACE(LOWER(Name),'cde','xxx');`, ourDialect)

	verifyLexerTokens(l,
		[]ql.Token{
			Tok(TokenSubscribeTo, "SUBSCRIBETO"),
			Tok(ql.TokenUdfExpr, "count"),
			Tok(ql.TokenLeftParenthesis, "("),
			Tok(ql.TokenIdentity, "x"),
			Tok(ql.TokenRightParenthesis, ")"),
			Tok(ql.TokenComma, ","),
			Tok(ql.TokenIdentity, "Name"),
			Tok(ql.TokenFrom, "FROM"),
			Tok(ql.TokenIdentity, "ourstream"),
			Tok(ql.TokenWhere, "WHERE"),
			Tok(ql.TokenIdentity, "k"),
			Tok(ql.TokenEqual, "="),
			Tok(ql.TokenUdfExpr, "REPLACE"),
			Tok(ql.TokenLeftParenthesis, "("),
			Tok(ql.TokenUdfExpr, "LOWER"),
			Tok(ql.TokenLeftParenthesis, "("),
			Tok(ql.TokenIdentity, "Name"),
			Tok(ql.TokenRightParenthesis, ")"),
			Tok(ql.TokenComma, ","),
			Tok(ql.TokenValue, "cde"),
			Tok(ql.TokenComma, ","),
			Tok(ql.TokenValue, "xxx"),
			Tok(ql.TokenRightParenthesis, ")"),
			Tok(ql.TokenEOS, ";"),
		})
}
