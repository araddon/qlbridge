package main

import (
	"fmt"
	ql "github.com/araddon/qlparser"
	"strings"
)

var (
	// Tokens Specific to our PUBSUB
	TokenSubscribeTo ql.TokenType = 1000
	// We are going to create our own Dialect Right now
	// that uses a "SUBSCRIBETO" keyword
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
	// We are going to inject new tokens into QLparse
	ql.TokenNameMap[TokenSubscribeTo] = &ql.TokenInfo{Description: "subscribeto"}
	// OverRide the Identity Characters in QLparse to allow a dash in identity
	ql.IDENTITY_CHARS = "_./-"
	ql.LoadTokenInfo()

	ourDialect.Init()
	//ql.LoadTokenInfo()
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
			{TokenSubscribeTo, "SUBSCRIBETO"},
			{ql.TokenUdfExpr, "count"},
			{ql.TokenLeftParenthesis, "("},
			{ql.TokenIdentity, "x"},
			{ql.TokenRightParenthesis, ")"},
			{ql.TokenComma, ","},
			{ql.TokenIdentity, "Name"},
			{ql.TokenFrom, "FROM"},
			{ql.TokenIdentity, "ourstream"},
			{ql.TokenWhere, "WHERE"},
			{ql.TokenIdentity, "k"},
			{ql.TokenEqual, "="},
			{ql.TokenUdfExpr, "REPLACE"},
			{ql.TokenLeftParenthesis, "("},
			{ql.TokenUdfExpr, "LOWER"},
			{ql.TokenLeftParenthesis, "("},
			{ql.TokenIdentity, "Name"},
			{ql.TokenRightParenthesis, ")"},
			{ql.TokenComma, ","},
			{ql.TokenValue, "cde"},
			{ql.TokenComma, ","},
			{ql.TokenValue, "xxx"},
			{ql.TokenRightParenthesis, ")"},
			{ql.TokenEOS, ";"},
		})
}
