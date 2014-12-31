package influxql

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"strings"
)

var (
	// Tokens Specific to INFLUXDB
	TokenShortDesc lex.TokenType = 1000
	TokenLongDesc  lex.TokenType = 1001
	TokenKind      lex.TokenType = 1002
)

var selectQl = []*lex.Clause{
	{Token: lex.TokenSelect, Lexer: LexColumnsInflux},
	{Token: lex.TokenFrom, Lexer: LexInfluxName},
	{Token: lex.TokenGroupBy, Lexer: lex.LexColumns, Optional: true},
	{Token: lex.TokenLimit, Lexer: lex.LexNumber, Optional: true},
	{Token: lex.TokenInto, Lexer: lex.LexExpressionOrIdentity},
	{Token: lex.TokenWhere, Lexer: lex.LexColumns, Optional: true},
}

var InfluxQlDialect *lex.Dialect = &lex.Dialect{
	Statements: []*lex.Statement{
		&lex.Statement{lex.TokenSelect, selectQl},
	},
}

func init() {
	lex.TokenNameMap[TokenShortDesc] = &lex.TokenInfo{Description: "SHORTDESC"}
	lex.TokenNameMap[TokenLongDesc] = &lex.TokenInfo{Description: "LONGDESC"}
	lex.TokenNameMap[TokenKind] = &lex.TokenInfo{Description: "kind"}
	// OverRide the Identity Characters in QLparse
	lex.IDENTITY_CHARS = "_./-"
	lex.LoadTokenInfo()
	InfluxQlDialect.Init()
}

// Handle influx columns
//  SELECT
//       valuect(item) AS stuff SHORTDESC "stuff" KIND INT
//
// Examples:
//
//  (colx = y OR colb = b)
//  cola = 'a5'p
//  cola != "a5", colb = "a6"
//  REPLACE(cola,"stuff") != "hello"
//  FirstName = REPLACE(LOWER(name," "))
//  cola IN (1,2,3)
//  cola LIKE "abc"
//  eq(name,"bob") AND age > 5
//
func LexColumnsInflux(l *lex.Lexer) lex.StateFn {

	l.SkipWhiteSpaces()

	keyWord := strings.ToLower(l.PeekWord())

	u.Debugf("LexColumnsInflux  r= '%v'", string(keyWord))

	switch keyWord {
	case "if":
		l.ConsumeWord("if")
		l.Emit(lex.TokenIf)
		l.Push("LexColumns", LexColumnsInflux)
		return lex.LexColumns
	case "shortdesc":
		l.ConsumeWord("shortdesc")
		l.Emit(TokenShortDesc)
		l.Push("LexColumns", LexColumnsInflux)
		l.Push("lexIdentifier", lex.LexValue)
		return nil

	case "longdesc":
		l.ConsumeWord("longdesc")
		l.Emit(TokenLongDesc)
		l.Push("LexColumns", LexColumnsInflux)
		l.Push("lexIdentifier", lex.LexValue)
		return nil

	case "kind":
		l.ConsumeWord("kind")
		l.Emit(TokenKind)
		l.Push("LexColumns", lex.LexColumns)
		l.Push("lexIdentifier", lex.LexIdentifier)
		return nil

	}
	return lex.LexColumns
}

// lex value
//
//    SIMPLE_NAME_VALUE | TABLE_NAME_VALUE | REGEX_VALUE
func LexInfluxName(l *lex.Lexer) lex.StateFn {

	l.SkipWhiteSpaces()
	firstChar := l.Peek()
	u.Debugf("LexInfluxName:  %v", string(firstChar))

	switch firstChar {
	case '"':
		return lex.LexValue(l)
	case '/':
		// a regex
		return lex.LexRegex(l)
	}
	return lex.LexIdentifier
}
