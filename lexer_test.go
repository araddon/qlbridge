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

}

func token(lexString string, runLex StateFn) Token {
	l := NewSqlLexer(lexString)
	runLex(l)
	return l.NextToken()
}

func TestIdentity(t *testing.T) {
	tok := token("table_name", LexIdentifier)
	assert.T(t, tok.T == TokenIdentity && tok.V == "table_name")
	tok = token("`table_name`", LexIdentifier)
	assert.T(t, tok.T == TokenIdentity && tok.V == "table_name")
	tok = token("[first_name]", LexIdentifier)
	assert.T(t, tok.T == TokenIdentity && tok.V == "first_name")
	tok = token("'first_name'", LexIdentifier)
	assert.T(t, tok.T == TokenIdentity && tok.V == "first_name")
	tok = token("dostuff(arg1)", LexIdentifier)
	assert.T(t, tok.T == TokenIdentity && tok.V == "dostuff")
}

func TestLexScanNumber(t *testing.T) {
	validIntegers := []string{
		// Decimal
		"42",
		"-827",
		// Hexadecimal
		"0x1A2B",
	}
	invalidIntegers := []string{
		// Decimal
		"042",
		"-0827",
		// Hexadecimal
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
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok, goodToken, "want='%v' has %v ", goodToken.V, tok)
	}
}

func verifyLexerTokens(t *testing.T, l *Lexer, tokens []Token) {
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok, goodToken, "want='%v' has %v ", goodToken.V, tok)
	}
}

func TestLexCommentTypes(t *testing.T) {
	verifyTokens(t, `--hello
-- multiple single -- / # line comments w /* more */
SELECT x FROM mytable`,
		[]Token{
			{TokenCommentSingleLine, "--"},
			{TokenComment, "hello"},
			{TokenCommentSingleLine, "--"},
			{TokenComment, " multiple single -- / # line comments w /* more */"},
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
		})

	verifyTokens(t, `// hello
-- multiple single line comments
# with hash
SELECT x FROM mytable`,
		[]Token{
			{TokenCommentSlashes, "//"},
			{TokenComment, " hello"},
			{TokenCommentSingleLine, "--"},
			{TokenComment, " multiple single line comments"},
			{TokenCommentHash, "#"},
			{TokenComment, " with hash"},
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
		})

	verifyTokens(t, `/*
hello
multiline
*/
/* and more */
SELECT x FROM mytable`,
		[]Token{
			{TokenCommentML, "\nhello\nmultiline\n"},
			{TokenCommentML, " and more "},
			{TokenSelect, "SELECT"},
			{TokenIdentity, "x"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "mytable"},
		})
}

func TestWithDialect(t *testing.T) {
	withStatement := &Statement{TokenWith, []*Clause{
		{Token: TokenWith, Lexer: LexColumns, Optional: true},
	}}
	withDialect := &Dialect{
		"QL With", []*Statement{withStatement},
	}
	withDialect.Init()
	/* Many *ql languages support some type of columnar layout such as:
	   name = value, name2 = value2
	*/
	l := NewLexer(`WITH k = REPLACE(LOWER(Name),'cde','xxx')  ,
						k2 = REPLACE(LOWER(email),'@gmail.com','')
				`, withDialect)

	verifyLexerTokens(t, l,
		[]Token{
			{TokenWith, "WITH"},
			{TokenIdentity, "k"},
			{TokenEqual, "="},
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
			{TokenIdentity, "k2"},
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

func TestWhereClauses(t *testing.T) {

	// WHERE eq(myfield, "bob") AND ge(party, 1)
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
		Name IN ('Blade', 'c w', 1) AND Name LIKE '%bob';
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
			{TokenInteger, "1"},
			{TokenRightParenthesis, ")"},
			{TokenLogicAnd, "AND"},
			{TokenIdentity, "Name"},
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
			{TokenUdfExpr, "eq"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "name"},
			{TokenComma, ","},
			{TokenValue, "bob"},
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
			{TokenIdentity, "pn"},
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

	verifyTokens(t, `SELECT count(*) FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenUdfExpr, "count"},
			{TokenLeftParenthesis, "("},
			{TokenStar, "*"},
			{TokenRightParenthesis, ")"},
			{TokenFrom, "FROM"},
			{TokenIdentity, "Product"},
		})

	verifyTokens(t, `SELECT * FROM Product`,
		[]Token{
			{TokenSelect, "SELECT"},
			{TokenStar, "*"},
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
			{TokenInteger, "10"},
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

func TestLexAlter(t *testing.T) {

	verifyTokens(t, `-- lets alter the table
		ALTER TABLE t1 CHANGE colbefore colafter TEXT CHARACTER SET utf8;`,
		[]Token{
			{TokenCommentSingleLine, "--"},
			{TokenComment, " lets alter the table"},
			{TokenAlter, "ALTER"},
			{TokenTable, "TABLE"},
			{TokenIdentity, "t1"},
			{TokenChange, "CHANGE"},
			{TokenIdentity, "colbefore"},
			{TokenIdentity, "colafter"},
			{TokenText, "TEXT"},
			{TokenCharacterSet, "CHARACTER SET"},
			{TokenIdentity, "utf8"},
			{TokenEOS, ";"},
		})
	// ALTER TABLE t MODIFY latin1_varchar_col VARCHAR(M) CHARACTER SET utf8;

	verifyTokens(t, "ALTER TABLE `quoted_table`"+
		`CHANGE col1_old col1_new varchar(10),
		 CHANGE col2_old col2_new TEXT 
		CHARACTER SET utf8;`,
		[]Token{
			{TokenAlter, "ALTER"},
			{TokenTable, "TABLE"},
			{TokenIdentity, "quoted_table"},
			{TokenChange, "CHANGE"},
			{TokenIdentity, "col1_old"},
			{TokenIdentity, "col1_new"},
			{TokenVarChar, "varchar"},
			{TokenLeftParenthesis, "("},
			{TokenInteger, "10"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenChange, "CHANGE"},
			{TokenIdentity, "col2_old"},
			{TokenIdentity, "col2_new"},
			{TokenText, "TEXT"},
			{TokenCharacterSet, "CHARACTER SET"},
			{TokenIdentity, "utf8"},
			{TokenEOS, ";"},
		})

	verifyTokens(t, "ALTER TABLE `quoted_table`"+
		`CHANGE col1_old col1_new varchar(10),
		 ADD col2 TEXT FIRST,
		 ADD col3 BIGINT AFTER col1_new
		CHARACTER SET utf8;`,
		[]Token{
			{TokenAlter, "ALTER"},
			{TokenTable, "TABLE"},
			{TokenIdentity, "quoted_table"},
			{TokenChange, "CHANGE"},
			{TokenIdentity, "col1_old"},
			{TokenIdentity, "col1_new"},
			{TokenVarChar, "varchar"},
			{TokenLeftParenthesis, "("},
			{TokenInteger, "10"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenAdd, "ADD"},
			{TokenIdentity, "col2"},
			{TokenText, "TEXT"},
			{TokenFirst, "FIRST"},
			{TokenComma, ","},
			{TokenAdd, "ADD"},
			{TokenIdentity, "col3"},
			{TokenBigInt, "BIGINT"},
			{TokenAfter, "AFTER"},
			{TokenIdentity, "col1_new"},
			{TokenCharacterSet, "CHARACTER SET"},
			{TokenIdentity, "utf8"},
			{TokenEOS, ";"},
		})
}

func TestLexUpdate(t *testing.T) {
	/*
			UPDATE [LOW_PRIORITY] [IGNORE] table_reference
		    SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
		    [WHERE where_condition]
		    [ORDER BY ...]
		    [LIMIT row_count]
	*/
	verifyTokens(t, `-- lets update stuff
		UPDATE users SET name = 'bob', email = 'email@email.com' WHERE id = 12 AND user_type >= 2 LIMIT 10;`,
		[]Token{
			{TokenCommentSingleLine, "--"},
			{TokenComment, " lets update stuff"},
			{TokenUpdate, "UPDATE"},
			{TokenTable, "users"},
			{TokenSet, "SET"},
			{TokenIdentity, "name"},
			{TokenEqual, "="},
			{TokenValue, "bob"},
			{TokenComma, ","},
			{TokenIdentity, "email"},
			{TokenEqual, "="},
			{TokenValue, "email@email.com"},
			{TokenWhere, "WHERE"},
			{TokenIdentity, "id"},
			{TokenEqual, "="},
			{TokenInteger, "12"},
			{TokenLogicAnd, "AND"},
			{TokenIdentity, "user_type"},
			{TokenGE, ">="},
			{TokenInteger, "2"},
			{TokenLimit, "LIMIT"},
			{TokenInteger, "10"},
			{TokenEOS, ";"},
		})
}

func TestLexInsert(t *testing.T) {
	/*
		INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
		    [INTO] tbl_name [(col_name,...)]
		    {VALUES | VALUE} ({expr | DEFAULT},...),(...),...
		    [ ON DUPLICATE KEY UPDATE
		      col_name=expr
		        [, col_name=expr] ... ]
		OR
		INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
		    [INTO] tbl_name
		    SET col_name={expr | DEFAULT}, ...
		    [ ON DUPLICATE KEY UPDATE
		      col_name=expr
		        [, col_name=expr] ... ]

		INSERT INTO pre.`fusion` ( `en` ,`item` ,`segment`) SELECT * FROM f3p1 WHERE 1;

		INSERT INTO logs (`site_id`, `time`,`hits`) VALUES (1,"2004-08-09", 15) ON DUPLICATE KEY UPDATE

		INSERT INTO table (a, b, c) VALUES (1,2,3)

		INSERT INTO table SET a=1, b=2, c=3

	*/
	verifyTokens(t, `-- lets insert stuff
		INSERT INTO users SET name = 'bob', email = 'bob@email.com'`,
		[]Token{
			{TokenCommentSingleLine, "--"},
			{TokenComment, " lets insert stuff"},
			{TokenInsert, "INSERT"},
			{TokenInto, "INTO"},
			{TokenTable, "users"},
			{TokenSet, "SET"},
			{TokenIdentity, "name"},
			{TokenEqual, "="},
			{TokenValue, "bob"},
			{TokenComma, ","},
			{TokenIdentity, "email"},
			{TokenEqual, "="},
			{TokenValue, "bob@email.com"},
		})

	verifyTokens(t, `INSERT INTO users (name,email,ct) 
		VALUES 
			('bob', 'bob@email.com', 2),
			('bill', 'bill@email.com', 5);`,
		[]Token{
			{TokenInsert, "INSERT"},
			{TokenInto, "INTO"},
			{TokenTable, "users"},
			{TokenLeftParenthesis, "("},
			{TokenIdentity, "name"},
			{TokenComma, ","},
			{TokenIdentity, "email"},
			{TokenComma, ","},
			{TokenIdentity, "ct"},
			{TokenRightParenthesis, ")"},
			{TokenValues, "VALUES"},
			{TokenLeftParenthesis, "("},
			{TokenValue, "bob"},
			{TokenComma, ","},
			{TokenValue, "bob@email.com"},
			{TokenComma, ","},
			{TokenInteger, "2"},
			{TokenRightParenthesis, ")"},
			{TokenComma, ","},
			{TokenLeftParenthesis, "("},
			{TokenValue, "bill"},
			{TokenComma, ","},
			{TokenValue, "bill@email.com"},
			{TokenComma, ","},
			{TokenInteger, "5"},
			{TokenRightParenthesis, ")"},
			{TokenEOS, ";"},
		})
}

func TestLexDelete(t *testing.T) {
	/*
		DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
		[WHERE where_condition]
		[ORDER BY ...]
		[LIMIT row_count]
	*/
	verifyTokens(t, `-- lets delete stuff
		DELETE FROM users WHERE id = 12 AND user_type >= 2 LIMIT 10;`,
		[]Token{
			{TokenCommentSingleLine, "--"},
			{TokenComment, " lets delete stuff"},
			{TokenDelete, "DELETE"},
			{TokenFrom, "FROM"},
			{TokenTable, "users"},
			{TokenWhere, "WHERE"},
			{TokenIdentity, "id"},
			{TokenEqual, "="},
			{TokenInteger, "12"},
			{TokenLogicAnd, "AND"},
			{TokenIdentity, "user_type"},
			{TokenGE, ">="},
			{TokenInteger, "2"},
			{TokenLimit, "LIMIT"},
			{TokenInteger, "10"},
			{TokenEOS, ";"},
		})
}
