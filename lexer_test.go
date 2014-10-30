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
			{TokenIdentity, "x"},
			{TokenComma, ","},
			{TokenValue, "st"},
			{TokenRightParenthesis, ")"},
			{TokenRightParenthesis, ")"},
			{TokenAs, "AS"},
			{TokenValue, "xst"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
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
	// 			{TokenIdentity, "x"},
	// 			{TokenComma, ","},
	// 			{TokenIdentity, "y"},
	// 			{TokenFrom, "FROM"},
	// 			{TokenTable, "mytable"},
	// 			{TokenWhere, "WHERE"},
	// 			{TokenIdentity, "x"},
	// 			{TokenEqual, "="},
	// 			{TokenValue, "7"},
	// 			{TokenLogicOr, "OR"},
	// 			{TokenIdentity, "y"},
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
		l := NewSqlLexer(v)
		typ, ok := ScanNumber(l)
		res := l.input[l.start:l.pos]
		if !ok || typ != TokenInteger {
			t.Fatalf("Expected a valid integer for %q", v)
		}
		if res != v {
			t.Fatalf("Expected %q, got %q", v, res)
		}
	}
	for _, v := range invalidIntegers {
		l := NewSqlLexer(v)
		_, ok := ScanNumber(l)
		if ok {
			t.Fatalf("Expected an invalid integer for %q", v)
		}
	}
	for _, v := range validFloats {
		l := NewSqlLexer(v)
		typ, ok := ScanNumber(l)
		res := l.input[l.start:l.pos]
		if !ok || typ != TokenFloat {
			t.Fatalf("Expected a valid float for %q", v)
		}
		if res != v {
			t.Fatalf("Expected %q, got %q", v, res)
		}
	}
	for _, v := range invalidFloats {
		l := NewSqlLexer(v)
		_, ok := ScanNumber(l)
		if ok {
			t.Fatalf("Expected an invalid float for %q", v)
		}
	}
}

func verifyTokens(t *testing.T, sql string, tokens []Token) {
	l := NewSqlLexer(sql)
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
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
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
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
		})

	verifyTokens(t, `/*
hello
multiline
*/
SELECT x FROM mytable`,
		[]Token{
			{TokenComment, "/*\nhello\nmultiline\n*/"},
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
		})
}

func TestWhereClauses(t *testing.T) {

	// Where statement that uses name = expr   syntax
	verifyTokens(t, `SELECT x FROM p
	WHERE
		username =  REPLACE(LOWER(email), '@gmail.com', '')
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "p"},
			{TokenWhere, "WHERE"},
			{TokenIdentity, "username"},
			{TokenEqual, "="},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "email"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenValue, "@gmail.com"},
			{TokenComma, ","},
			{TokenValue, ""},
			{TokenRightParenthesis, ")"},
		})

	verifyTokens(t, `SELECT x FROM p
	WHERE 
		Name IN ('Blade', 'c w', 1)
		, BOB LIKE '%bob';
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "p"},
			{TokenWhere, "WHERE"},
			{TokenIdentity, "Name"},
			{TokenIN, "IN"},
			{TokenLeftParenthesis, "("},
			{TokenValue, "Blade"},
			{TokenComma, ","},
			{TokenValue, "c w"},
			{TokenComma, ","},
			{TokenValue, "1"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenIdentity, "BOB"},
			{TokenLike, "LIKE"},
			{TokenValue, "%bob"},
		})

	verifyTokens(t, `SELECT x FROM p
	WHERE
		eq(name,"bob")
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "p"},
			{TokenWhere, "WHERE"},
			{TokenIdentity, "Name"},
			{TokenEqual, "="},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "email"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenValue, "@gmail.com"},
			{TokenComma, ","},
			{TokenValue, ""},
			{TokenRightParenthesis, ")"},
		})

}

func TestLexGroupBy(t *testing.T) {
	verifyTokens(t, `SELECT x FROM p
	GROUP BY company, category
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "p"},
			{TokenGroupBy, "GROUP BY"},
			{TokenIdentity, "company"},
			{TokenComma, ","},
			{TokenIdentity, "category"},
		})
	verifyTokens(t, `SELECT x FROM p
	GROUP BY 
		LOWER(company), 
		LOWER(REPLACE(category,'cde','xxx'))
	`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "p"},
			{TokenGroupBy, "GROUP BY"},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "company"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "LOWER"},
			{TokenLeftParenthesis, "("},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "category"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenRightParenthesis, ")"},
		})
}

func TestLexFrom(t *testing.T) {
	verifyTokens(t, `SELECT x FROM github.user`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "github.user"},
		})
	verifyTokens(t, `SELECT x FROM github/user`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "github/user"},
		})
}

func TestLexTSQL(t *testing.T) {
	verifyTokens(t, `
	SELECT ProductID, Name, p_name AS pn
	FROM Production.Product
	WHERE Name IN ('Blade', 'Crown Race', 'Spokes');`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenIdentity, "ProductID"},
			{TokenComma, ","},
			{TokenIdentity, "Name"},
			{TokenComma, ","},
			{TokenIdentity, "p_name"},
			{TokenAs, "AS"},
			{TokenValue, "pn"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "Production.Product"},
			{TokenWhere, "WHERE"},
			{TokenIdentity, "Name"},
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
			{TokenIdentity, "Name"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "Product"},
		})

	verifyTokens(t, `SELECT REPLACE(Name,'cde','xxx') FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "Name"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "Product"},
		})
	verifyTokens(t, `SELECT REPLACE(Name,'cde','xxx'), RIGHT(email,10) FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "REPLACE"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "Name"},
			{TokenComma, ","},
			{TokenValue, "cde"},
			{TokenComma, ","},
			{TokenValue, "xxx"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenUdfExpr, "RIGHT"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "email"},
			{TokenComma, ","},
			{TokenValue, "10"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "Product"},
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
			{TokenIdentity, "Name"},
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
			{TokenIdentity, "email"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenValue, "@gmail.com"},
			{TokenComma, ","},
			{TokenValue, ""},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "Product"},
		})
}
