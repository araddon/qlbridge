package qlparse

import (
	"flag"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
)

func init() {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
}

func TestDev(t *testing.T) {
	verifyTokens(t, `--hello
	-- multiple single
	-- line comments
	SELECT LOWER(REPLACE(x,"st")) AS xst FROM mytable`,
		[]Token{
			{TokenComment, "--hello"},
			{TokenComment, "-- multiple single"},
			{TokenComment, "-- line comments"},
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "x"},
			{TokenComma, ","},
			{TokenValue, "st"},
			{TokenRightParenthesis, ")"},
			{TokenRightParenthesis, ")"},
			{TokenAs, "AS"},
			{TokenValue, "xst"},
			{TokenFrom, "FROM"},
			{TokenTable, "mytable"},
		})

	// 	verifyTokens(t, `/*
	// hello
	// multiline
	// */
	// SELECT
	//     x, y
	// FROM mytable
	// WHERE x = 7 OR y != '2'`,
	// 		[]Token{
	// 			{TokenComment, "/*\nhello\nmultiline\n*/"},
	// 			{TokenSelect, "SELECT"},
	// 			{TokenColumn, "x"},
	// 			{TokenComma, ","},
	// 			{TokenColumn, "y"},
	// 			{TokenFrom, "FROM"},
	// 			{TokenTable, "mytable"},
	// 			{TokenWhere, "WHERE"},
	// 			{TokenColumn, "x"},
	// 			{TokenEqual, "="},
	// 			{TokenValue, "7"},
	// 			{TokenLogicOr, "OR"},
	// 			{TokenColumn, "y"},
	// 			{TokenNE, "!="},
	// 			{TokenValue, "2"}})
}

func TestLexScanNumber(t *testing.T) {
	validIntegers := []string{
		// Decimal.
		"42",
		"-827",
		// Hexadecimal.
		"0x1A2B",
	}
	invalidIntegers := []string{
		// Decimal.
		"042",
		"-0827",
		// Hexadecimal.
		"-0x1A2B",
		"0X1A2B",
		"0x1a2b",
		"0x1A2B.2B",
	}
	validFloats := []string{
		"0.5",
		"-100.0",
		"-3e-3",
		"6.02e23",
		"5.1e-9",
	}
	invalidFloats := []string{
		".5",
		"-.5",
		"100.",
		"-100.",
		"-3E-3",
		"6.02E23",
		"5.1E-9",
		"-3e",
		"6.02e",
	}

	for _, v := range validIntegers {
		l := NewLexer(v)
		typ, ok := scanNumber(l)
		res := l.input[l.start:l.pos]
		if !ok || typ != TokenInteger {
			t.Fatalf("Expected a valid integer for %q", v)
		}
		if res != v {
			t.Fatalf("Expected %q, got %q", v, res)
		}
	}
	for _, v := range invalidIntegers {
		l := NewLexer(v)
		_, ok := scanNumber(l)
		if ok {
			t.Fatalf("Expected an invalid integer for %q", v)
		}
	}
	for _, v := range validFloats {
		l := NewLexer(v)
		typ, ok := scanNumber(l)
		res := l.input[l.start:l.pos]
		if !ok || typ != TokenFloat {
			t.Fatalf("Expected a valid float for %q", v)
		}
		if res != v {
			t.Fatalf("Expected %q, got %q", v, res)
		}
	}
	for _, v := range invalidFloats {
		l := NewLexer(v)
		_, ok := scanNumber(l)
		if ok {
			t.Fatalf("Expected an invalid float for %q", v)
		}
	}
}

func verifyTokens(t *testing.T, sql string, tokens []Token) {
	l := NewLexer(sql)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok, goodToken, "has='%v' want='%v'", tok.V, goodToken.V)
	}
}

func TestLexCommentTypes(t *testing.T) {
	verifyTokens(t, `--hello
-- multiple single
-- line comments
SELECT x FROM mytable`,
		[]Token{
			{TokenComment, "--hello"},
			{TokenComment, "-- multiple single"},
			{TokenComment, "-- line comments"},
			{TokenSelect, "SELECT"},
			{TokenColumn, "x"},
			{TokenFrom, "FROM"},
			{TokenTable, "mytable"},
		})

	verifyTokens(t, `// hello
-- multiple single
-- line comments
SELECT x FROM mytable`,
		[]Token{
			{TokenComment, "// hello"},
			{TokenComment, "-- multiple single"},
			{TokenComment, "-- line comments"},
			{TokenSelect, "SELECT"},
			{TokenColumn, "x"},
			{TokenFrom, "FROM"},
			{TokenTable, "mytable"},
		})

	verifyTokens(t, `/*
hello
multiline
*/
SELECT x FROM mytable`,
		[]Token{
			{TokenComment, "/*\nhello\nmultiline\n*/"},
			{TokenSelect, "SELECT"},
			{TokenColumn, "x"},
			{TokenFrom, "FROM"},
			{TokenTable, "mytable"},
		})
}

func TestWhereClauses(t *testing.T) {
	verifyTokens(t, `SELECT x FROM p
	WHERE 
		Name IN ('Blade', 'c w', 1)
		, BOB LIKE '%bob';
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenColumn, "x"},
			{TokenFrom, "FROM"},
			{TokenTable, "p"},
			{TokenWhere, "WHERE"},
			{TokenColumn, "Name"},
			{TokenIN, "IN"},
			{TokenLeftParenthesis, "("},
			{TokenValue, "Blade"},
			{TokenComma, ","},
			{TokenValue, "c w"},
			{TokenComma, ","},
			{TokenValue, "1"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenColumn, "BOB"},
			{TokenLike, "LIKE"},
			{TokenValue, "%bob"},
		})

	// verifyTokens(t, `SELECT x FROM p
	// WHERE
	// 	Name =  REPLACE(LOWER(email), '@gmail.com', '')
	// `,
	// 	[]Token{
	// 		{TokenSelect, "SELECT"},
	// 		{TokenColumn, "x"},
	// 		{TokenFrom, "FROM"},
	// 		{TokenTable, "p"},
	// 		{TokenWhere, "WHERE"},
	// 		{TokenColumn, "Name"},
	// 		{TokenEqual, "="},
	// 		{TokenUdfExpr, "REPLACE"},
	// 		{TokenLeftParenthesis, "("},
	// 		{TokenUdfExpr, "LOWER"},
	// 		{TokenLeftParenthesis, "("},
	// 		{TokenColumn, "email"},
	// 		{TokenRightParenthesis, ")"},
	// 		{TokenComma, ","},
	// 		{TokenValue, "@gmail.com"},
	// 		{TokenComma, ","},
	// 		{TokenValue, ""},
	// 		{TokenRightParenthesis, ")"},
	// 	})

}

func TestLexGroupBy(t *testing.T) {
	verifyTokens(t, `SELECT x FROM p
	GROUP BY company, category
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenColumn, "x"},
			{TokenFrom, "FROM"},
			{TokenTable, "p"},
			{TokenGroupBy, "GROUP BY"},
			{TokenColumn, "company"},
			{TokenComma, ","},
			{TokenColumn, "category"},
		})
	verifyTokens(t, `SELECT x FROM p
	GROUP BY 
		LOWER(company), 
		LOWER(REPLACE(category,'cde','xxx'))
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenColumn, "x"},
			{TokenFrom, "FROM"},
			{TokenTable, "p"},
			{TokenGroupBy, "GROUP BY"},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "company"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "category"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenRightParenthesis, ")"},
		})
}

func TestLexTSQL(t *testing.T) {
	verifyTokens(t, `
	SELECT ProductID, Name, p_name AS pn
	FROM Production.Product
	WHERE Name IN ('Blade', 'Crown Race', 'Spokes');`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenColumn, "ProductID"},
			{TokenComma, ","},
			{TokenColumn, "Name"},
			{TokenComma, ","},
			{TokenColumn, "p_name"},
			{TokenAs, "AS"},
			{TokenValue, "pn"},
			{TokenFrom, "FROM"},
			{TokenTable, "Production.Product"},
			{TokenWhere, "WHERE"},
			{TokenColumn, "Name"},
			{TokenIN, "IN"},
			{TokenLeftParenthesis, "("},
			{TokenValue, "Blade"},
			{TokenComma, ","},
			{TokenValue, "Crown Race"},
			{TokenComma, ","},
			{TokenValue, "Spokes"},
			{TokenRightParenthesis, ")"},
			{TokenEOS, ";"},
		})
}

func TestLexSelectExpressions(t *testing.T) {
	verifyTokens(t, `SELECT LOWER(Name) FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "Name"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenTable, "Product"},
		})

	verifyTokens(t, `SELECT REPLACE(Name,'cde','xxx') FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "Name"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenTable, "Product"},
		})
	verifyTokens(t, `SELECT REPLACE(Name,'cde','xxx'), RIGHT(email,10) FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "Name"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "RIGHT"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "email"},
			{TokenComma, ","},
			{TokenValue, "10"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenTable, "Product"},
		})
}
func TestLexSelectNestedExpressions(t *testing.T) {

	verifyTokens(t, `SELECT 
						REPLACE(LOWER(Name),'cde','xxx'),
						REPLACE(LOWER(email),'@gmail.com','')
					FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "Name"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenColumn, "email"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenValue, "@gmail.com"},
			{TokenComma, ","},
			{TokenValue, ""},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenTable, "Product"},
		})
}
