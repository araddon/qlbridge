package main

import (
	"fmt"
	"github.com/araddon/qlbridge/lex"
	"strings"
)

/*
	This example is meant to show how to create a new
	Dialect Language with a keyword"SUBSCRIBETO"
	then Lex an example of this syntax

*/
var (
	// We need a token to recognize our "SUBSCRIBETO" keyword
	// in our PUBSUB dialect
	TokenSubscribeTo lex.TokenType = 1000

	// We are going to create our own Dialect now
	//  that uses a "SUBSCRIBETO" keyword
	pubsub = &lex.Clause{Token: TokenSubscribeTo, Clauses: []*lex.Clause{
		{Token: TokenSubscribeTo, Lexer: lex.LexColumns},
		{Token: lex.TokenFrom, Lexer: LexMaybe},
		{Token: lex.TokenWhere, Lexer: lex.LexColumns, Optional: true},
	}}
	ourDialect = &lex.Dialect{
		Name: "Subscribe To", Statements: []*lex.Clause{pubsub},
	}
)

func init() {
	// inject any new tokens into QLBridge.Lex describing the custom tokens we created
	lex.TokenNameMap[TokenSubscribeTo] = &lex.TokenInfo{Description: "subscribeto"}

	// OverRide the Identity Characters in lexer to allow a dash in identity
	lex.IDENTITY_CHARS = "_./-"
	lex.LoadTokenInfo()

	ourDialect.Init()
}

func verifyLexerTokens(l *lex.Lexer, tokens []lex.Token) {
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
func LexMaybe(l *lex.Lexer) lex.StateFn {

	l.SkipWhiteSpaces()

	keyWord := strings.ToLower(l.PeekWord())

	switch keyWord {
	case "maybe":
		l.ConsumeWord("maybe")
		l.Emit(lex.TokenIdentity)
		return lex.LexExpressionOrIdentity
	}
	return lex.LexExpressionOrIdentity
}

func Tok(tok lex.TokenType, val string) lex.Token { return lex.Token{tok, val, 0} }

func main() {

	/* Many *ql languages support some type of columnar layout such as:
	   name = value, name2 = value2
	*/
	l := lex.NewLexer(`
				SUBSCRIBETO
					count(x), Name
				FROM ourstream
				WHERE 
					k = REPLACE(LOWER(Name),"cde","xxx");`, ourDialect)

	verifyLexerTokens(l,
		[]lex.Token{
			Tok(TokenSubscribeTo, "SUBSCRIBETO"),
			Tok(lex.TokenUdfExpr, "count"),
			Tok(lex.TokenLeftParenthesis, "("),
			Tok(lex.TokenIdentity, "x"),
			Tok(lex.TokenRightParenthesis, ")"),
			Tok(lex.TokenComma, ","),
			Tok(lex.TokenIdentity, "Name"),
			Tok(lex.TokenFrom, "FROM"),
			Tok(lex.TokenIdentity, "ourstream"),
			Tok(lex.TokenWhere, "WHERE"),
			Tok(lex.TokenIdentity, "k"),
			Tok(lex.TokenEqual, "="),
			Tok(lex.TokenUdfExpr, "REPLACE"),
			Tok(lex.TokenLeftParenthesis, "("),
			Tok(lex.TokenUdfExpr, "LOWER"),
			Tok(lex.TokenLeftParenthesis, "("),
			Tok(lex.TokenIdentity, "Name"),
			Tok(lex.TokenRightParenthesis, ")"),
			Tok(lex.TokenComma, ","),
			Tok(lex.TokenValue, "cde"),
			Tok(lex.TokenComma, ","),
			Tok(lex.TokenValue, "xxx"),
			Tok(lex.TokenRightParenthesis, ")"),
			Tok(lex.TokenEOS, ";"),
		})
}
