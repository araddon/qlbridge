package qlparse

import (
	"flag"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
)

func init() {
	flag.Parse()
	if testing.Verbose() {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
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
		u.Debugf("%#v", tok)
		assert.Equal(t, tok, goodToken)
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
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "mytable"},
		})

	verifyTokens(t, `// hello
-- multiple single
-- line comments
SELECT x FROM mytable`,
		[]Token{
			{TokenComment, "// hello"},
			{TokenComment, "-- multiple single"},
			{TokenComment, "-- line comments"},
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "mytable"},
		})

	verifyTokens(t, `/*
hello
multiline
*/
SELECT x FROM mytable`,
		[]Token{
			{TokenComment, "/*\nhello\nmultiline\n*/"},
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "mytable"},
		})
}

func Test1(t *testing.T) {
	verifyTokens(t, `/*
hello
multiline
*/
SELECT 
    x, y 
FROM mytable 
WHERE x = 7 OR y != '2'`,
		[]Token{
			{TokenComment, "/*\nhello\nmultiline\n*/"},
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenComma, ","},
			{TokenSqlColumn, "y"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "mytable"},
			{TokenSqlWhere, "WHERE"},
			{TokenSqlColumn, "x"},
			{TokenEqual, "="},
			{TokenSqlValue, "7"},
			{TokenLogicOr, "OR"},
			{TokenSqlColumn, "y"},
			{TokenNE, "!="},
			{TokenSqlValue, "2"}})
}

func TestWhereClauses(t *testing.T) {
	verifyTokens(t, `SELECT x FROM p
	WHERE 
		Name IN ('Blade', 'c w', 1)
		, BOB LIKE '%bob';
	`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "p"},
			{TokenSqlWhere, "WHERE"},
			{TokenSqlColumn, "Name"},
			{TokenIN, "IN"},
			{TokenLeftParenthesis, "("},
			{TokenSqlValue, "Blade"},
			{TokenComma, ","},
			{TokenSqlValue, "c w"},
			{TokenComma, ","},
			{TokenSqlValue, "1"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenSqlColumn, "BOB"},
			{TokenLike, "LIKE"},
			{TokenSqlValue, "%bob"},
		})

	// verifyTokens(t, `SELECT x FROM p
	// WHERE
	// 	Name =  REPLACE(LOWER(email), '@gmail.com', '')
	// `,
	// 	[]Token{
	// 		{TokenSqlSelect, "SELECT"},
	// 		{TokenSqlColumn, "x"},
	// 		{TokenSqlFrom, "FROM"},
	// 		{TokenSqlTable, "p"},
	// 		{TokenSqlWhere, "WHERE"},
	// 		{TokenSqlColumn, "Name"},
	// 		{TokenEqual, "="},
	// 		{TokenUdfExpr, "REPLACE"},
	// 		{TokenLeftParenthesis, "("},
	// 		{TokenUdfExpr, "LOWER"},
	// 		{TokenLeftParenthesis, "("},
	// 		{TokenSqlColumn, "email"},
	// 		{TokenRightParenthesis, ")"},
	// 		{TokenComma, ","},
	// 		{TokenSqlValue, "@gmail.com"},
	// 		{TokenComma, ","},
	// 		{TokenSqlValue, ""},
	// 		{TokenRightParenthesis, ")"},
	// 	})

}

func TestLexGroupBy(t *testing.T) {
	verifyTokens(t, `SELECT x FROM p
	GROUP BY company, category
	`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "p"},
			{TokenSqlGroupBy, "GROUP BY"},
			{TokenSqlColumn, "company"},
			{TokenComma, ","},
			{TokenSqlColumn, "category"},
		})
	verifyTokens(t, `SELECT x FROM p
	GROUP BY 
		LOWER(company), 
		LOWER(REPLACE(category,'cde','xxx'))
	`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "x"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "p"},
			{TokenSqlGroupBy, "GROUP BY"},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "company"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "category"},
			{TokenComma, ","},
			{TokenSqlValue, "cde"},
			{TokenComma, ","},
			{TokenSqlValue, "xxx"},
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
			{TokenSqlSelect, "SELECT"},
			{TokenSqlColumn, "ProductID"},
			{TokenComma, ","},
			{TokenSqlColumn, "Name"},
			{TokenComma, ","},
			{TokenSqlColumn, "p_name"},
			{TokenSqlAs, "AS"},
			{TokenSqlColumn, "pn"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "Production.Product"},
			{TokenSqlWhere, "WHERE"},
			{TokenSqlColumn, "Name"},
			{TokenIN, "IN"},
			{TokenLeftParenthesis, "("},
			{TokenSqlValue, "Blade"},
			{TokenComma, ","},
			{TokenSqlValue, "Crown Race"},
			{TokenComma, ","},
			{TokenSqlValue, "Spokes"},
			{TokenRightParenthesis, ")"},
			{TokenEOS, ";"},
		})
}

func TestLexSelectExpressions(t *testing.T) {
	verifyTokens(t, `SELECT LOWER(Name) FROM Product`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "Name"},
			{TokenRightParenthesis, ")"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "Product"},
		})

	verifyTokens(t, `SELECT REPLACE(Name,'cde','xxx') FROM Product`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "Name"},
			{TokenComma, ","},
			{TokenSqlValue, "cde"},
			{TokenComma, ","},
			{TokenSqlValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "Product"},
		})
	verifyTokens(t, `SELECT REPLACE(Name,'cde','xxx'), RIGHT(email,10) FROM Product`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "Name"},
			{TokenComma, ","},
			{TokenSqlValue, "cde"},
			{TokenComma, ","},
			{TokenSqlValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "RIGHT"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "email"},
			{TokenComma, ","},
			{TokenSqlValue, "10"},
			{TokenRightParenthesis, ")"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "Product"},
		})
}
func TestLexSelectNestedExpressions(t *testing.T) {

	verifyTokens(t, `SELECT 
						REPLACE(LOWER(Name),'cde','xxx'),
						REPLACE(LOWER(email),'@gmail.com','')
					FROM Product`,
		[]Token{
			{TokenSqlSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "Name"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenSqlValue, "cde"},
			{TokenComma, ","},
			{TokenSqlValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenSqlColumn, "email"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenSqlValue, "@gmail.com"},
			{TokenComma, ","},
			{TokenSqlValue, ""},
			{TokenRightParenthesis, ")"},
			{TokenSqlFrom, "FROM"},
			{TokenSqlTable, "Product"},
		})
}
