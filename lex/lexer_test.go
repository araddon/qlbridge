package lex

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
)

func TestMain(m *testing.M) {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
	// Now run the actual Tests
	os.Exit(m.Run())
}

func tv(t TokenType, v string) Token {
	return Token{T: t, V: v}
}

func token(lexString string, runLex StateFn) Token {
	l := NewSqlLexer(lexString)
	runLex(l)
	return l.NextToken()
}

func verifyIdentity(t *testing.T, input, expects string, isIdentity bool) {
	l := NewSqlLexer(input)
	assert.Equal(t, isIdentity, l.isIdentity(), "Expected %s to be %v identity", input, isIdentity)
	LexIdentifier(l)
	tok := l.NextToken()
	if isIdentity {
		assert.Equal(t, tok.T, TokenIdentity)
		assert.Equal(t, tok.V, expects, "expected %s got %v", expects, tok.V)
	}
}

func TestLexer(t *testing.T) {
	orig := Trace
	Trace = true
	verifyIdentity(t, `table_name`, "table_name", true)
	Trace = orig
	l := NewSqlLexer("SELECT x from y;")
	for i := 0; i < 600; i++ {
		l.Push("fake", LexNumber)
	}
	// Should not infinitely push
	assert.Equal(t, 500, len(l.stack))

	l = NewExpressionLexer(`#hello`)
	assert.True(t, l.IsComment())
	l = NewExpressionLexer(`//hello`)
	assert.True(t, l.IsComment())
	l = NewExpressionLexer(`--hello`)
	assert.True(t, l.IsComment())
	l.pop()
	l.pop()
	tok := l.NextToken()
	assert.Equal(t, TokenError, tok.T)

	l = NewExpressionLexer(`!exists(ident)`)
	assert.True(t, l.isExpr())
	l = NewExpressionLexer(`!--mistake-invalid`)
	assert.Equal(t, false, l.isExpr())

	assert.Equal(t, false, isJsonStart(rune('x')))
	assert.Equal(t, false, isJsonStart(rune(']')))
	assert.Equal(t, false, isJsonStart(rune('}')))
	assert.Equal(t, true, isJsonStart(rune('[')))
	assert.Equal(t, true, isJsonStart(rune('{')))

	assert.Equal(t, true, isLaxIdentifierRune(rune('x')))
	assert.Equal(t, true, isLaxIdentifierRune(rune('.')))
	assert.Equal(t, false, isLaxIdentifierRune(rune('&')))
}
func TestLexIdentity(t *testing.T) {

	assert.Equal(t, false, IsValidIdentity("☺☻☹"))
	assert.Equal(t, false, IsValidIdentity("日a本b語ç日ð本Ê語þ日¥本¼語i日©"))
	assert.Equal(t, false, IsValidIdentity("stream&more"))

	verifyIdentity(t, "`☺☻☹`", "☺☻☹", true)
	verifyIdentity(t, "`日a本b語ç日ð本Ê語þ日¥本¼語i日©`", "日a本b語ç日ð本Ê語þ日¥本¼語i日©", true)
	verifyIdentity(t, `"hello"`, "", false)
	verifyIdentity(t, `[table name]`, "table name", true)
	verifyIdentity(t, `table_name`, "table_name", true)
	verifyIdentity(t, "`table_name`", "table_name", true)
	verifyIdentity(t, "`table name`", "table name", true)
	verifyIdentity(t, "`table name`.`right side`", "table name`.`right side", true)
	verifyIdentity(t, `table_name*`, "table_name", true)
	tok := token("`table_name`", LexIdentifier)
	assert.True(t, tok.T == TokenIdentity && tok.V == "table_name", "%v", tok)
	tok = token("`table w *&$% ^ 56 rty`", LexIdentifier)
	assert.True(t, tok.T == TokenIdentity && tok.V == "table w *&$% ^ 56 rty", "%v", tok.V)
	tok = token("[first_name]", LexIdentifier)
	assert.True(t, tok.T == TokenIdentity && tok.V == "first_name", "%v", tok.V)
	// double quotes are not on by default for identities
	tok = token(`"first_name"`, LexIdentifier)
	assert.True(t, tok.T == TokenError)
	tok = token("dostuff(arg1)", LexIdentifier)
	assert.True(t, tok.T == TokenIdentity && tok.V == "dostuff", "%v", tok.V)
	tempIdentityQuotes := IdentityQuoting
	IdentityQuoting = []byte{'\''}
	tok = token("'first_name'", LexIdentifier)
	assert.True(t, tok.T == TokenIdentity && tok.V == "first_name", "%v", tok.V)
	IdentityQuoting = tempIdentityQuotes

	tok = token("`table name`.`right side`", LexIdentifier)
	assert.True(t, tok.T == TokenIdentity && tok.V == "table name`.`right side", "%v", tok.V)
	IdentityQuoting = tempIdentityQuotes
}

func TestLexValue(t *testing.T) {
	tok1 := token(`"Toys R"" Us"`, LexValue)
	assert.True(t, tok1.T == TokenValueEscaped, "%v", tok1)
	assert.True(t, tok1.V == `Toys R"" Us`, "%v", tok1.String())

	tok := token(`"hello's with quote"`, LexValue)
	assert.True(t, tok.T == TokenValue && tok.V == "hello's with quote")

	rawValue := `hello\"s with quote`
	quotedValue := fmt.Sprintf(`"%s"`, rawValue)
	tok = token(quotedValue, LexValue)
	assert.True(t, tok.T == TokenValueEscaped, "%v", tok)
	assert.True(t, strings.EqualFold(rawValue, tok.V), "%v", tok)

	rawValue = `string with \"double quotes\"`
	quotedValue = fmt.Sprintf(`"%s"`, rawValue)
	u.Debugf("quotedValue = %v", quotedValue)
	tok = token(quotedValue, LexValue)
	assert.True(t, tok.T == TokenValueEscaped, "%v", tok)

	rawValue = `string with \'single quotes\'`
	quotedValue = fmt.Sprintf(`'%s'`, rawValue)
	tok = token(quotedValue, LexValue)
	assert.True(t, tok.T == TokenValueEscaped, "%v", tok)
	assert.True(t, strings.EqualFold(rawValue, tok.V), "%v", tok)
	//u.Debugf("qv: %v rv:%v ", quotedValue, rawValue)
	//u.Debugf("%v", strings.EqualFold(rawValue, tok.V), tok.V)
	tok = token(`"Toys R"" Us"`, LexValue)
	assert.True(t, tok.T == TokenValueEscaped, "%v", tok)
	assert.True(t, tok.V == `Toys R"" Us`, "%v", tok.String())
}

func TestLexRegex(t *testing.T) {
	tok := token(` /^stats\./i `, LexRegex)
	assert.True(t, tok.T == TokenRegex && tok.V == `/^stats\./i`, "%v", tok)
	tok = token(` /^[a-z0-9_-]{3,16}$/ `, LexRegex)
	assert.True(t, tok.T == TokenRegex && tok.V == `/^[a-z0-9_-]{3,16}$/`, "%v", tok)
	tok = token(` /<TAG\b[^>]*>(.*?)</TAG>/ `, LexRegex)
	assert.True(t, tok.T == TokenRegex && tok.V == `/<TAG\b[^>]*>(.*?)</TAG>/`, "%v", tok)
}

func TestLexNumber(t *testing.T) {
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
		LexNumber(l)
		tok := l.NextToken()
		if tok.T != TokenInteger {
			t.Fatalf("Expected a valid integer for %q but got %v", v, tok)
		}
		if tok.V != v {
			t.Fatalf("Expected %q, got %v", v, tok)
		}
	}
	for _, v := range invalidIntegers {
		l := NewSqlLexer(v)
		_, ok := scanNumber(l)
		if ok {
			t.Fatalf("Expected an invalid integer for %q", v)
		}
	}
	for _, v := range validFloats {
		l := NewSqlLexer(v)
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
		l := NewSqlLexer(v)
		_, ok := scanNumber(l)
		if ok {
			t.Fatalf("Expected an invalid float for %q", v)
		}
	}
}

func TestLexDuration(t *testing.T) {
	// Test some valid ones
	for _, v := range strings.Split("5m,4y ,45m, 2w, 4ms", ",") {
		l := NewSqlLexer(v)
		LexDuration(l)
		tok := l.NextToken()
		if tok.T != TokenDuration {
			t.Fatalf("Expected a valid integer for %q but got %v", v, tok)
		}
		if tok.V != strings.Trim(v, " ") {
			t.Fatalf("Expected %q, got %v", v, tok)
		}
	}
}

func verifyTokens(t *testing.T, sql string, tokens []Token) {
	l := NewSqlLexer(sql)
	u.Debugf("sql: %v", sql)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("got:%v  want:%v", tok, goodToken)
		assert.Equal(t, tok.T, goodToken.T, "want='%v' has %v ", goodToken.T, tok)
		assert.Equal(t, tok.V, goodToken.V, "want='%v' has %v ", goodToken.V, tok)
	}
}

func verifyTokenTypes(t *testing.T, sql string, tt []TokenType) {
	l := NewSqlLexer(sql)
	u.Debug(sql)
	for _, tokenType := range tt {
		tok := l.NextToken()
		//u.Infof("%#v  expects:%v", tok, tokenType)
		assert.Equal(t, tok.T, tokenType, "want='%v' has %v ", tokenType, tok.T)
	}
}

func lexTokens(sql string) []Token {
	tokens := make([]Token, 0)
	l := NewSqlLexer(sql)
	for {
		tok := l.NextToken()
		if tok.T == TokenEOF || tok.T == TokenEOS {
			break
		}
		tokens = append(tokens, tok)
	}
	return tokens
}

func verifyLexerTokens(t *testing.T, l *Lexer, tokens []Token) {
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equal(t, tok.T, goodToken.T, "want='%v' has %v ", goodToken.T, tok.T)
		assert.Equal(t, tok.V, goodToken.V, "want='%v' has %v ", goodToken.V, tok.V)
	}
}

func TestLexExpressions(t *testing.T) {
	verifyExprTokens(t, `gt(toint(total_amount),0)`,
		[]Token{
			tv(TokenUdfExpr, "gt"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "toint"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "total_amount"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenInteger, "0"),
			tv(TokenRightParenthesis, ")"),
		})
}

func TestLexCommentTypes(t *testing.T) {
	verifyTokens(t, `--hello
-- multiple single -- / # line comments w /* more */
SELECT x FROM mytable`,
		[]Token{
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, "hello"),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " multiple single -- / # line comments w /* more */"),
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mytable"),
		})

	verifyTokens(t, `// hello
-- multiple single line comments
# with hash
SELECT x FROM mytable`,
		[]Token{
			tv(TokenCommentSlashes, "//"),
			tv(TokenComment, " hello"),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " multiple single line comments"),
			tv(TokenCommentHash, "#"),
			tv(TokenComment, " with hash"),
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mytable"),
		})

	verifyTokens(t, `/*
hello
multiline
*/
/* and more */
SELECT x FROM mytable`,
		[]Token{
			tv(TokenCommentML, "\nhello\nmultiline\n"),
			tv(TokenCommentML, " and more "),
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mytable"),
		})
}

func TestLexSqlIdentities(t *testing.T) {
	// http://stackoverflow.com/questions/1992314/what-is-the-difference-between-single-and-double-quotes-in-sql
	// Verify a variety of things in identities
	// 1)   ` ' or [   delimiters
	// 2)   spaces in name
	verifyTokens(t, "select `abc`, [abcd], [abc def] as ab2, @@varname, id-dash from tbl1",
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "abc"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "abcd"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "abc def"),
			tv(TokenAs, "as"),
			tv(TokenIdentity, "ab2"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "@@varname"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "id-dash"),
			tv(TokenFrom, "from"),
			tv(TokenIdentity, "tbl1"),
		})

	verifyTokens(t, "select `abc` AS abc1, `def` AS def1 from tbl1",
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "abc"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "abc1"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "def"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "def1"),
			tv(TokenFrom, "from"),
			tv(TokenIdentity, "tbl1"),
		})

	verifyTokens(t, "select `a field`.`table name` AS abc1 from `table name`",
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "a field`.`table name"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "abc1"),
			tv(TokenFrom, "from"),
			tv(TokenIdentity, "table name"),
		})
}

func TestWithDialect(t *testing.T) {
	// We are going to create our own Dialect Right now
	withStatement := &Clause{Token: TokenWith, Clauses: []*Clause{
		{Token: TokenWith, Lexer: LexColumns, Optional: true},
	}}
	withDialect := &Dialect{
		"QL With", []*Clause{withStatement}, IdentityQuoting, false,
	}
	withDialect.Init()
	/* Many *ql languages support some type of columnar layout such as:
	   name = value, name2 = value2
	*/
	l := NewLexer(`WITH k = REPLACE(LOWER(Name),"cde","xxx")  ,
						k2 = REPLACE(LOWER(email),"@gmail.com","")
				`, withDialect)

	verifyLexerTokens(t, l,
		[]Token{
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "k"),
			tv(TokenEqual, "="),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Name"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenValue, "cde"),
			tv(TokenComma, ","),
			tv(TokenValue, "xxx"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "k2"),
			tv(TokenEqual, "="),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "email"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenValue, "@gmail.com"),
			tv(TokenComma, ","),
			tv(TokenValue, ""),
			tv(TokenRightParenthesis, ")"),
		})
}

func TestWhereClauses(t *testing.T) {

	// WHERE eq(myfield, "bob") AND ge(party, 1)
	// Where statement that uses name = expr   syntax

	verifyTokens(t, `SELECT x FROM p
		WHERE
			username =  REPLACE(LOWER(email), "@gmail.com", "")
		`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "p"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "username"),
			tv(TokenEqual, "="),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "email"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenValue, "@gmail.com"),
			tv(TokenComma, ","),
			tv(TokenValue, ""),
			tv(TokenRightParenthesis, ")"),
		})

	verifyTokens(t, `SELECT x FROM p
		WHERE
			Name IN ("Blade", "c w", 1) AND Name LIKE "%bob";
		`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "p"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "Name"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "Blade"),
			tv(TokenComma, ","),
			tv(TokenValue, "c w"),
			tv(TokenComma, ","),
			tv(TokenInteger, "1"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenIdentity, "Name"),
			tv(TokenLike, "LIKE"),
			tv(TokenValue, "%bob"),
		})

	verifyTokens(t, `SELECT x FROM p
		WHERE
			eq(name,"bob")
			AND x == 4 * 5
		`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "p"),
			tv(TokenWhere, "WHERE"),
			tv(TokenUdfExpr, "eq"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "name"),
			tv(TokenComma, ","),
			tv(TokenValue, "bob"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenIdentity, "x"),
			tv(TokenEqualEqual, "=="),
			tv(TokenInteger, "4"),
			tv(TokenMultiply, "*"),
			tv(TokenInteger, "5"),
		})

	verifyTokens(t, `
		select 
			user_id 
		FROM stdio
		WHERE
			yy(reg_date) == 11 and !(bval == false)`,
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "user_id"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "stdio"),
			tv(TokenWhere, "WHERE"),
			tv(TokenUdfExpr, "yy"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "reg_date"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEqualEqual, "=="),
			tv(TokenInteger, "11"),
			tv(TokenLogicAnd, "and"),
			tv(TokenNegate, "!"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "bval"),
			tv(TokenEqualEqual, "=="),
			tv(TokenIdentity, "false"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyTokens(t, `
		select 
			user_id 
		FROM stdio
		WHERE
			year BETWEEN 1 AND 5`,
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "user_id"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "stdio"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "year"),
			tv(TokenBetween, "BETWEEN"),
			tv(TokenInteger, "1"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenInteger, "5"),
		})

	verifyTokens(t, `
		select
			user_id
		FROM stdio
		WHERE
			domain IN (@@whitelist_domains)`,
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "user_id"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "stdio"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "domain"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "@@whitelist_domains"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyTokens(t, `
		select user_id, email FROM user
		WHERE email IS NOT NULL`,
		[]Token{
			tv(TokenSelect, "select"),
			tv(TokenIdentity, "user_id"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "email"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "user"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "email"),
			tv(TokenIs, "IS"),
			tv(TokenNegate, "NOT"),
			tv(TokenNull, "NULL"),
		})
}

func TestLexGreedyUdf(t *testing.T) {
	// The lexer should recognize that
	//   not here is a UDF, not "not"
	verifyTokenTypes(t, `
		SELECT 
			f1
			, not(eq(field1))
		FROM employee`,
		[]TokenType{TokenSelect,
			TokenIdentity, TokenComma,
			TokenUdfExpr, TokenLeftParenthesis, TokenUdfExpr, TokenLeftParenthesis,
			TokenIdentity, TokenRightParenthesis, TokenRightParenthesis,
			TokenFrom, TokenIdentity,
		})
}

func TestLexUdfSpecialAs(t *testing.T) {
	// Their is a special case for UDF's that allow an AS
	//    CAST(field AS int)
	verifyTokenTypes(t, `
		SELECT 
			name
			, cast(score AS int)
		FROM employee`,
		[]TokenType{TokenSelect,
			TokenIdentity, TokenComma,
			TokenUdfExpr, TokenLeftParenthesis, TokenIdentity, TokenAs,
			TokenIdentity, TokenRightParenthesis,
			TokenFrom, TokenIdentity,
		})
}

func TestLexSqlJoin(t *testing.T) {

	verifyTokenTypes(t, `
		SELECT 
			t1.name, t2.salary
		FROM employee AS t1 
		INNER JOIN info AS t2 
		ON t1.name = t2.name;`,
		[]TokenType{TokenSelect,
			TokenIdentity, TokenComma, TokenIdentity,
			TokenFrom, TokenIdentity, TokenAs, TokenIdentity,
			TokenInner, TokenJoin, TokenIdentity, TokenAs, TokenIdentity,
			TokenOn, TokenIdentity, TokenEqual, TokenIdentity,
		})
}

func TestLexSqlSubQuery(t *testing.T) {

	verifyTokenTypes(t, `select
	         user_id, email
	     FROM users
	     WHERE user_id in
	     	(select user_id from orders where qty > 5)`,
		[]TokenType{TokenSelect,
			TokenIdentity, TokenComma, TokenIdentity,
			TokenFrom, TokenIdentity, TokenWhere, TokenIdentity,
			TokenIN, TokenLeftParenthesis, TokenSelect, TokenIdentity,
			TokenFrom, TokenIdentity, TokenWhere, TokenIdentity,
			TokenGT, TokenInteger,
			TokenRightParenthesis,
		})
}

func TestLexSqlPreparedStmt(t *testing.T) {
	verifyTokens(t, `
		PREPARE stmt1 
		FROM 
			'SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse';`,
		[]Token{
			tv(TokenPrepare, "PREPARE"),
			tv(TokenIdentity, "stmt1"),
			tv(TokenFrom, "FROM"),
			tv(TokenValue, `SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse`),
			tv(TokenEOS, ";"),
		})
}

func TestLexGroupBy(t *testing.T) {
	verifyTokens(t, `SELECT x FROM p
	GROUP BY company, category
	`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "p"),
			tv(TokenGroupBy, "GROUP BY"),
			tv(TokenIdentity, "company"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "category"),
		})
	verifyTokens(t, `SELECT x FROM p
	GROUP BY 
		LOWER(company), 
		LOWER(REPLACE(category,"cde","xxx"))
	`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "p"),
			tv(TokenGroupBy, "GROUP BY"),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "company"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "category"),
			tv(TokenComma, ","),
			tv(TokenValue, "cde"),
			tv(TokenComma, ","),
			tv(TokenValue, "xxx"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenRightParenthesis, ")"),
		})
}

func TestLexFrom(t *testing.T) {
	verifyTokens(t, `SELECT x FROM github.user`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "github.user"),
		})
	verifyTokens(t, `SELECT x FROM github/user`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "x"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "github/user"),
		})
}

func TestLexOrderBy(t *testing.T) {
	verifyTokens(t, `
	SELECT product_id, name, category
	FROM product
	WHERE status = "instock"
	ORDER BY category, otherstuff;`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "product_id"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "name"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "category"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "product"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "status"),
			tv(TokenEqual, "="),
			tv(TokenValue, "instock"),
			tv(TokenOrderBy, "ORDER BY"),
			tv(TokenIdentity, "category"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "otherstuff"),
			tv(TokenEOS, ";"),
		})

	verifyTokens(t, "SELECT actor.id, actor FROM github_watch where `actor.id` < 1000 ORDER BY `actor.id` ASC;",
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "actor.id"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "actor"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "github_watch"),
			tv(TokenWhere, "where"),
			tv(TokenIdentity, "actor.id"),
			tv(TokenLT, "<"),
			tv(TokenInteger, "1000"),
			tv(TokenOrderBy, "ORDER BY"),
			tv(TokenIdentity, "actor.id"),
			tv(TokenAsc, "ASC"),
			tv(TokenEOS, ";"),
		})
}

func TestLexTSQL(t *testing.T) {
	verifyTokens(t, `
	SELECT ProductID, Name, p_name AS pn
	FROM Production.Product
	WHERE Name IN ("Blade", "Crown Race", "Spokes");`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "ProductID"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "Name"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "p_name"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "pn"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Production.Product"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "Name"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "Blade"),
			tv(TokenComma, ","),
			tv(TokenValue, "Crown Race"),
			tv(TokenComma, ","),
			tv(TokenValue, "Spokes"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEOS, ";"),
		})
}

func TestLexSelectStar(t *testing.T) {
	verifyTokens(t, `SELECT * FROM github.user`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenStar, "*"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "github.user"),
		})
}

func TestLexSelectExpressions(t *testing.T) {
	verifyTokens(t, `SELECT LOWER(Name) FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Name"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})

	verifyTokens(t, `SELECT count(*) FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenUdfExpr, "count"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenStar, "*"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})

	verifyTokens(t, `SELECT * FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenStar, "*"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})

	verifyTokens(t, `SELECT REPLACE(Name,"cder","xxx") FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Name"),
			tv(TokenComma, ","),
			tv(TokenValue, "cder"),
			tv(TokenComma, ","),
			tv(TokenValue, "xxx"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})
	verifyTokens(t, `SELECT REPLACE(Name,"abcd","xx22x"), RIGHT(email,10) FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Name"),
			tv(TokenComma, ","),
			tv(TokenValue, "abcd"),
			tv(TokenComma, ","),
			tv(TokenValue, "xx22x"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenUdfExpr, "RIGHT"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "email"),
			tv(TokenComma, ","),
			tv(TokenInteger, "10"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})
}

func TestLexSelectIfGuard(t *testing.T) {

	verifyTokens(t, `SELECT 
            lol AS notlol IF hey == 0
        FROM nothing`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "lol"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "notlol"),
			tv(TokenIf, "IF"),
			tv(TokenIdentity, "hey"),
			tv(TokenEqualEqual, "=="),
			tv(TokenInteger, "0"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "nothing"),
		})

	verifyTokens(t, `SELECT sum(price) AS total_value IF price > 0 FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenUdfExpr, "sum"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "price"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "total_value"),
			tv(TokenIf, "IF"),
			tv(TokenIdentity, "price"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "0"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})
}

func TestLexSelectLogicalColumns(t *testing.T) {

	verifyTokens(t, `SELECT item > 5 AS item1, item > itemb, itemx > "value", itema + 5 > 4 FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "item"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "5"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "item1"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "item"),
			tv(TokenGT, ">"),
			tv(TokenIdentity, "itemb"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "itemx"),
			tv(TokenGT, ">"),
			tv(TokenValue, "value"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "itema"),
			tv(TokenPlus, "+"),
			tv(TokenInteger, "5"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "4"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})
}

func TestLexSelectNestedExpressions(t *testing.T) {

	verifyTokens(t, `SELECT 
						REPLACE(LOWER(Name),"cde","xxx"),
						REPLACE(LOWER(email),"@gmail.com","")
					FROM Product`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Name"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenValue, "cde"),
			tv(TokenComma, ","),
			tv(TokenValue, "xxx"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenUdfExpr, "REPLACE"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenUdfExpr, "LOWER"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "email"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenValue, "@gmail.com"),
			tv(TokenComma, ","),
			tv(TokenValue, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "Product"),
		})
}

func TestLexAlter(t *testing.T) {

	verifyTokens(t, `-- lets alter the table
		ALTER TABLE t1 CHANGE colbefore colafter TEXT CHARACTER SET utf8;`,
		[]Token{
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " lets alter the table"),
			tv(TokenAlter, "ALTER"),
			tv(TokenTable, "TABLE"),
			tv(TokenIdentity, "t1"),
			tv(TokenChange, "CHANGE"),
			tv(TokenIdentity, "colbefore"),
			tv(TokenIdentity, "colafter"),
			tv(TokenTypeText, "TEXT"),
			tv(TokenCharacterSet, "CHARACTER SET"),
			tv(TokenIdentity, "utf8"),
			tv(TokenEOS, ";"),
		})
	// ALTER TABLE t MODIFY latin1_varchar_col VARCHAR(M) CHARACTER SET utf8;

	verifyTokens(t, "ALTER TABLE `quoted_table` "+
		`CHANGE col1_old col1_new varchar(10),
		 CHANGE col2_old col2_new TEXT 
		CHARACTER SET utf8;`,
		[]Token{
			tv(TokenAlter, "ALTER"),
			tv(TokenTable, "TABLE"),
			tv(TokenIdentity, "quoted_table"),
			tv(TokenChange, "CHANGE"),
			tv(TokenIdentity, "col1_old"),
			tv(TokenIdentity, "col1_new"),
			tv(TokenTypeVarChar, "varchar"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "10"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenChange, "CHANGE"),
			tv(TokenIdentity, "col2_old"),
			tv(TokenIdentity, "col2_new"),
			tv(TokenTypeText, "TEXT"),
			tv(TokenCharacterSet, "CHARACTER SET"),
			tv(TokenIdentity, "utf8"),
			tv(TokenEOS, ";"),
		})

	verifyTokens(t, "ALTER TABLE `quoted_table`"+
		` CHANGE col1_old col1_new varchar(10),
		 ADD col2 TEXT FIRST,
		 ADD col3 BIGINT AFTER col1_new
		CHARACTER SET utf8;`,
		[]Token{
			tv(TokenAlter, "ALTER"),
			tv(TokenTable, "TABLE"),
			tv(TokenIdentity, "quoted_table"),
			tv(TokenChange, "CHANGE"),
			tv(TokenIdentity, "col1_old"),
			tv(TokenIdentity, "col1_new"),
			tv(TokenTypeVarChar, "varchar"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "10"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenAdd, "ADD"),
			tv(TokenIdentity, "col2"),
			tv(TokenTypeText, "TEXT"),
			tv(TokenFirst, "FIRST"),
			tv(TokenComma, ","),
			tv(TokenAdd, "ADD"),
			tv(TokenIdentity, "col3"),
			tv(TokenTypeBigInt, "BIGINT"),
			tv(TokenAfter, "AFTER"),
			tv(TokenIdentity, "col1_new"),
			tv(TokenCharacterSet, "CHARACTER SET"),
			tv(TokenIdentity, "utf8"),
			tv(TokenEOS, ";"),
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
		UPDATE users SET name = "bob", email = "email@email.com" WHERE id = 12 AND user_type >= 2 LIMIT 10;`,
		[]Token{
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " lets update stuff"),
			tv(TokenUpdate, "UPDATE"),
			tv(TokenTable, "users"),
			tv(TokenSet, "SET"),
			tv(TokenIdentity, "name"),
			tv(TokenEqual, "="),
			tv(TokenValue, "bob"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "email"),
			tv(TokenEqual, "="),
			tv(TokenValue, "email@email.com"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "id"),
			tv(TokenEqual, "="),
			tv(TokenInteger, "12"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenIdentity, "user_type"),
			tv(TokenGE, ">="),
			tv(TokenInteger, "2"),
			tv(TokenLimit, "LIMIT"),
			tv(TokenInteger, "10"),
			tv(TokenEOS, ";"),
		})
}

func TestLexUpsert(t *testing.T) {
	/*
		UPSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
		    [INTO] tbl_name [(col_name,...)]
		    {VALUES | VALUE} ({expr | DEFAULT),...),(...),...
		    [ ON DUPLICATE KEY UPDATE
		      col_name=expr
		        [, col_name=expr] ... ]
		OR
		UPSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
		    [INTO] tbl_name
		    SET col_name={expr | DEFAULT), ...
		    [ ON DUPLICATE KEY UPDATE
		      col_name=expr
		        [, col_name=expr] ... ]
	*/
	verifyTokens(t, `UPSERT INTO users (name,email,ct) 
		VALUES 
			("bob", "bob@email.com", 2),
			("bill", "bill@email.com", 5);`,
		[]Token{
			tv(TokenUpsert, "UPSERT"),
			tv(TokenInto, "INTO"),
			tv(TokenTable, "users"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "name"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "email"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "ct"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenValues, "VALUES"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "bob"),
			tv(TokenComma, ","),
			tv(TokenValue, "bob@email.com"),
			tv(TokenComma, ","),
			tv(TokenInteger, "2"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "bill"),
			tv(TokenComma, ","),
			tv(TokenValue, "bill@email.com"),
			tv(TokenComma, ","),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEOS, ";"),
		})
}

func TestLexInsert(t *testing.T) {
	/*
		INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
		    [INTO] tbl_name [(col_name,...)]
		    {VALUES | VALUE} ({expr | DEFAULT),...),(...),...
		    [ ON DUPLICATE KEY UPDATE
		      col_name=expr
		        [, col_name=expr] ... ]
		OR
		INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
		    [INTO] tbl_name
		    SET col_name={expr | DEFAULT), ...
		    [ ON DUPLICATE KEY UPDATE
		      col_name=expr
		        [, col_name=expr] ... ]

		INSERT INTO pre.`fusion` ( `en` ,`item` ,`segment`) SELECT * FROM f3p1 WHERE 1;

		INSERT INTO logs (`site_id`, `time`,`hits`) VALUES (1,"2004-08-09", 15) ON DUPLICATE KEY UPDATE

		INSERT INTO table (a, b, c) VALUES (1,2,3)

		INSERT INTO table SET a=1, b=2, c=3

	*/
	verifyTokens(t, `insert into mytable (id, str) values (0, "a")`,
		[]Token{
			tv(TokenInsert, "insert"),
			tv(TokenInto, "into"),
			tv(TokenTable, "mytable"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "id"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "str"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenValues, "values"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "0"),
			tv(TokenComma, ","),
			tv(TokenValue, "a"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyTokens(t, `-- lets insert stuff
		INSERT INTO users SET name = "bob", email = "bob@email.com"`,
		[]Token{
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " lets insert stuff"),
			tv(TokenInsert, "INSERT"),
			tv(TokenInto, "INTO"),
			tv(TokenTable, "users"),
			tv(TokenSet, "SET"),
			tv(TokenIdentity, "name"),
			tv(TokenEqual, "="),
			tv(TokenValue, "bob"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "email"),
			tv(TokenEqual, "="),
			tv(TokenValue, "bob@email.com"),
		})

	verifyTokens(t, `INSERT INTO users (name,email,ct) 
		VALUES 
			("bob", "bob@email.com", 2),
			("bill", "bill@email.com", 5);`,
		[]Token{
			tv(TokenInsert, "INSERT"),
			tv(TokenInto, "INTO"),
			tv(TokenTable, "users"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "name"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "email"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "ct"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenValues, "VALUES"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "bob"),
			tv(TokenComma, ","),
			tv(TokenValue, "bob@email.com"),
			tv(TokenComma, ","),
			tv(TokenInteger, "2"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "bill"),
			tv(TokenComma, ","),
			tv(TokenValue, "bill@email.com"),
			tv(TokenComma, ","),
			tv(TokenInteger, "5"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEOS, ";"),
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
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " lets delete stuff"),
			tv(TokenDelete, "DELETE"),
			tv(TokenFrom, "FROM"),
			tv(TokenTable, "users"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "id"),
			tv(TokenEqual, "="),
			tv(TokenInteger, "12"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenIdentity, "user_type"),
			tv(TokenGE, ">="),
			tv(TokenInteger, "2"),
			tv(TokenLimit, "LIMIT"),
			tv(TokenInteger, "10"),
			tv(TokenEOS, ";"),
		})
}

func TestWithJson(t *testing.T) {
	// The lexer should be able to parse json
	verifyTokenTypes(t, `
		SELECT f1 FROM employee
		WITH {
			"key1":"value2"
			,"key2":45, 
			"key3":["a",2,"b",true],
			"key4":{"hello":"value","age":55}
		}
		`,
		[]TokenType{TokenSelect, TokenIdentity, TokenFrom, TokenIdentity,
			TokenWith,
			TokenLeftBrace,
			TokenIdentity, TokenColon, TokenValue,
			TokenComma,
			TokenIdentity, TokenColon, TokenInteger,
			TokenComma,
			TokenIdentity, TokenColon, TokenLeftBracket, TokenValue, TokenComma, TokenInteger, TokenComma, TokenValue, TokenComma, TokenBool, TokenRightBracket,
			TokenComma,
			TokenIdentity, TokenColon, TokenLeftBrace, TokenIdentity, TokenColon, TokenValue, TokenComma, TokenIdentity, TokenColon, TokenInteger, TokenRightBrace,
			TokenRightBrace,
		})
}
