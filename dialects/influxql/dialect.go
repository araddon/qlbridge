package influxql

import (
	u "github.com/araddon/gou"
	ql "github.com/araddon/qlbridge/lex"
	"strings"
)

var (
	// Tokens Specific to INFLUXDB
	TokenShortDesc ql.TokenType = 1000
	TokenLongDesc  ql.TokenType = 1001
	TokenKind      ql.TokenType = 1002
)

var selectQl = []*ql.Clause{
	{Token: ql.TokenSelect, Lexer: LexColumnsInflux},
	{Token: ql.TokenFrom, Lexer: LexInfluxName},
	{Token: ql.TokenGroupBy, Lexer: ql.LexColumns, Optional: true},
	{Token: ql.TokenLimit, Lexer: ql.LexNumber, Optional: true},
	{Token: ql.TokenInto, Lexer: ql.LexExpressionOrIdentity},
	{Token: ql.TokenWhere, Lexer: ql.LexColumns, Optional: true},
}

var InfluxQlDialect *ql.Dialect = &ql.Dialect{
	Statements: []*ql.Statement{
		&ql.Statement{ql.TokenSelect, selectQl},
	},
}

func init() {
	ql.TokenNameMap[TokenShortDesc] = &ql.TokenInfo{Description: "SHORTDESC"}
	ql.TokenNameMap[TokenLongDesc] = &ql.TokenInfo{Description: "LONGDESC"}
	ql.TokenNameMap[TokenKind] = &ql.TokenInfo{Description: "kind"}
	// OverRide the Identity Characters in QLparse
	ql.IDENTITY_CHARS = "_./-"
	ql.LoadTokenInfo()
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
func LexColumnsInflux(l *ql.Lexer) ql.StateFn {

	l.SkipWhiteSpaces()

	keyWord := strings.ToLower(l.PeekWord())

	u.Debugf("LexColumnsInflux  r= '%v'", string(keyWord))

	switch keyWord {
	case "if":
		l.ConsumeWord("if")
		l.Emit(ql.TokenIf)
		l.Push("LexColumns", LexColumnsInflux)
		return ql.LexColumns
	case "shortdesc":
		l.ConsumeWord("shortdesc")
		l.Emit(TokenShortDesc)
		l.Push("LexColumns", LexColumnsInflux)
		l.Push("lexIdentifier", ql.LexValue)
		return nil

	case "longdesc":
		l.ConsumeWord("longdesc")
		l.Emit(TokenLongDesc)
		l.Push("LexColumns", LexColumnsInflux)
		l.Push("lexIdentifier", ql.LexValue)
		return nil

	case "kind":
		l.ConsumeWord("kind")
		l.Emit(TokenKind)
		l.Push("LexColumns", ql.LexColumns)
		l.Push("lexIdentifier", ql.LexIdentifier)
		return nil

	}
	return ql.LexColumns
}

// lex value
//
//    SIMPLE_NAME_VALUE | TABLE_NAME_VALUE | REGEX_VALUE
func LexInfluxName(l *ql.Lexer) ql.StateFn {

	l.SkipWhiteSpaces()
	firstChar := l.Peek()
	u.Debugf("LexInfluxName:  %v", string(firstChar))

	switch firstChar {
	case '"':
		return ql.LexValue(l)
	case '/':
		// a regex
		return ql.LexRegex(l)
	}
	return ql.LexIdentifier
}
