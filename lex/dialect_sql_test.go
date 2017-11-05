package lex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlDialectInit(t *testing.T) {
	// Make sure we can init more than once, see if it panics
	SqlDialect.Init()
	for _, stmt := range SqlDialect.Statements {
		assert.NotEqual(t, "", stmt.String())
	}
}

func TestLexSqlDescribe(t *testing.T) {
	/*
		describe myidentity
	*/
	verifyTokens(t, `DESCRIBE mytable;`,
		[]Token{
			tv(TokenDescribe, "DESCRIBE"),
			tv(TokenIdentity, "mytable"),
		})
	verifyTokens(t, `DESC mytable;`,
		[]Token{
			tv(TokenDesc, "DESC"),
			tv(TokenIdentity, "mytable"),
		})
}

func TestLexSqlShow(t *testing.T) {
	/*
		show myidentity
	*/
	verifyTokens(t, `SHOW mytable;`,
		[]Token{
			tv(TokenShow, "SHOW"),
			tv(TokenIdentity, "mytable"),
		})
	verifyTokens(t, `SHOW CREATE TRIGGER mytrigger;`,
		[]Token{
			tv(TokenShow, "SHOW"),
			tv(TokenCreate, "CREATE"),
			tv(TokenIdentity, "TRIGGER"),
			tv(TokenIdentity, "mytrigger"),
		})
	verifyTokenTypes(t, "SHOW FULL TABLES FROM `ourschema` LIKE '%'",
		[]TokenType{TokenShow,
			TokenFull, TokenTables,
			TokenFrom, TokenIdentity, TokenLike, TokenValue,
		})

	/*
	   SHOW TABLES
	   FROM `<yourdbname>`
	   WHERE
	       `Tables_in_<yourdbname>` LIKE '%cms%'
	       OR `Tables_in_<yourdbname>` LIKE '%role%';
	*/
	// SHOW TRIGGERS [FROM db_name] [like_or_where]
	verifyTokens(t, `SHOW TRIGGERS FROM mydb LIKE "tr*";`,
		[]Token{
			tv(TokenShow, "SHOW"),
			tv(TokenIdentity, "TRIGGERS"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mydb"),
			tv(TokenLike, "LIKE"),
			tv(TokenValue, "tr*"),
		})
	verifyTokens(t, "SHOW TRIGGERS FROM mydb WHERE `Triggers_in_mydb` LIKE 'tr*';",
		[]Token{
			tv(TokenShow, "SHOW"),
			tv(TokenIdentity, "TRIGGERS"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mydb"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "Triggers_in_mydb"),
			tv(TokenLike, "LIKE"),
			tv(TokenValue, "tr*"),
		})
	// SHOW INDEX FROM tbl_name [FROM db_name]
	verifyTokens(t, `SHOW INDEX FROM mydb LIKE "idx*";`,
		[]Token{
			tv(TokenShow, "SHOW"),
			tv(TokenIdentity, "INDEX"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mydb"),
			tv(TokenLike, "LIKE"),
			tv(TokenValue, "idx*"),
		})
}

func TestLexSqlCreate(t *testing.T) {

	// CREATE {DATABASE | SCHEMA | SOURCE | VIEW | CONTINUOUSVIEW} [IF NOT EXISTS] db_name
	// [create_specification] ...
	verifyTokens(t, `CREATE SCHEMA IF NOT EXISTS mysource 
		WITH stuff = "hello";
		`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenSchema, "SCHEMA"),
			tv(TokenIf, "IF"),
			tv(TokenNegate, "NOT"),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "mysource"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})

	verifyTokens(t, `CREATE SCHEMA mysource WITH stuff = "hello";`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenSchema, "SCHEMA"),
			tv(TokenIdentity, "mysource"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})

	verifyTokens(t, `CREATE DATABASE mydb WITH stuff = "hello";`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenDatabase, "DATABASE"),
			tv(TokenIdentity, "mydb"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})

	verifyTokens(t, `CREATE SOURCE mysource WITH stuff = "hello";`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenSource, "SOURCE"),
			tv(TokenIdentity, "mysource"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})

	verifyTokens(t, `CREATE OR REPLACE VIEW viewx 
			AS SELECT a, b FROM mydb.tbl 
			WITH stuff = "hello";`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenOr, "OR"),
			tv(TokenReplace, "REPLACE"),
			tv(TokenView, "VIEW"),
			tv(TokenIdentity, "viewx"),
			tv(TokenAs, "AS"),
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "a"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "b"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "mydb.tbl"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})

	verifyTokens(t, `CREATE TABLE articles 
		 (
		  ID int(11) NOT NULL AUTO_INCREMENT,
		  Email char(150) NOT NULL DEFAULT '',
		  PRIMARY KEY (ID),
		  -- lets put comments in here
		  CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email)
		) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8
	WITH stuff = "hello";`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenTable, "TABLE"),
			tv(TokenIdentity, "articles"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "ID"),
			tv(TokenTypeInteger, "int"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "11"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNegate, "NOT"),
			tv(TokenNull, "NULL"),
			tv(TokenIdentity, "AUTO_INCREMENT"),
			tv(TokenComma, ","),
			tv(TokenIdentity, "Email"),
			tv(TokenTypeChar, "char"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "150"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNegate, "NOT"),
			tv(TokenNull, "NULL"),
			tv(TokenDefault, "DEFAULT"),
			tv(TokenValue, ""),
			tv(TokenComma, ","),
			tv(TokenPrimary, "PRIMARY"),
			tv(TokenKey, "KEY"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "ID"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenComma, ","),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " lets put comments in here"),
			tv(TokenConstraint, "CONSTRAINT"),
			tv(TokenIdentity, "emails_fk"),
			tv(TokenForeign, "FOREIGN"),
			tv(TokenKey, "KEY"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Email"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenReferences, "REFERENCES"),
			tv(TokenIdentity, "Emails"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "Email"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEngine, "ENGINE"),
			tv(TokenEqual, "="),
			tv(TokenIdentity, "InnoDB"),
			tv(TokenIdentity, "AUTO_INCREMENT"),
			tv(TokenEqual, "="),
			tv(TokenInteger, "4080"),
			tv(TokenDefault, "DEFAULT"),
			tv(TokenIdentity, "CHARSET"),
			tv(TokenEqual, "="),
			tv(TokenIdentity, "utf8"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})
}
func TestLexSqlDrop(t *testing.T) {
	// DROP {DATABASE | SCHEMA | SOURCE | TABLE} [IF EXISTS] db_name
	verifyTokens(t, `DROP SCHEMA IF EXISTS myschema;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenSchema, "SCHEMA"),
			tv(TokenIf, "IF"),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "myschema"),
		})
	verifyTokens(t, `DROP TABLE IF EXISTS mytable;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenTable, "TABLE"),
			tv(TokenIf, "IF"),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "mytable"),
		})
	verifyTokens(t, `DROP TEMPORARY TABLE mytable;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenTemp, "TEMPORARY"),
			tv(TokenTable, "TABLE"),
			tv(TokenIdentity, "mytable"),
		})
	verifyTokens(t, `DROP SOURCE IF EXISTS mysource;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenSource, "SOURCE"),
			tv(TokenIf, "IF"),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "mysource"),
		})
	verifyTokens(t, `DROP DATABASE IF EXISTS mydb;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenDatabase, "DATABASE"),
			tv(TokenIf, "IF"),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "mydb"),
		})
	verifyTokens(t, `DROP DATABASE mydb;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenDatabase, "DATABASE"),
			tv(TokenIdentity, "mydb"),
		})
	verifyTokens(t, `DROP VIEW myv;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenView, "VIEW"),
			tv(TokenIdentity, "myv"),
		})
	verifyTokens(t, `DROP CONTINUOUSVIEW myv;`,
		[]Token{
			tv(TokenDrop, "DROP"),
			tv(TokenContinuousView, "CONTINUOUSVIEW"),
			tv(TokenIdentity, "myv"),
		})
}

func TestLexSqlSelect(t *testing.T) {
	/*
	   SELECT column1, column2, column3, ...
	   INTO newtable [IN externaldb]
	   FROM oldtable
	   WHERE condition;
	*/
	verifyTokens(t, `SELECT a, tolower(b) AS b INTO newtable FROM oldtable WHERE a != "hello";`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "a"),
			tv(TokenComma, ","),
			tv(TokenUdfExpr, "tolower"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "b"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenAs, "AS"),
			tv(TokenIdentity, "b"),
			tv(TokenInto, "INTO"),
			tv(TokenTable, "newtable"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "oldtable"),
			tv(TokenWhere, "WHERE"),
			tv(TokenIdentity, "a"),
			tv(TokenNE, "!="),
			tv(TokenValue, "hello"),
		})
	verifyTokens(t, `SELECT a FROM tbl LIMIT 1";`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "a"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "tbl"),
			tv(TokenLimit, "LIMIT"),
			tv(TokenInteger, "1"),
		})
	verifyTokens(t, `SELECT a FROM tbl LIMIT 1, 100";`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "a"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "tbl"),
			tv(TokenLimit, "LIMIT"),
			tv(TokenInteger, "1"),
			tv(TokenComma, ","),
			tv(TokenInteger, "100"),
		})
	// LIMIT 1000 OFFSET 100
	verifyTokens(t, `SELECT a FROM tbl LIMIT 1 OFFSET 100";`,
		[]Token{
			tv(TokenSelect, "SELECT"),
			tv(TokenIdentity, "a"),
			tv(TokenFrom, "FROM"),
			tv(TokenIdentity, "tbl"),
			tv(TokenLimit, "LIMIT"),
			tv(TokenInteger, "1"),
			tv(TokenOffset, "OFFSET"),
			tv(TokenInteger, "100"),
		})
}
