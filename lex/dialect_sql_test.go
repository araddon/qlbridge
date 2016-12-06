package lex

import (
	"testing"
)

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

	verifyTokenTypes(t, "SHOW FULL TABLES FROM `ourschema` LIKE '%'",
		[]TokenType{TokenShow,
			TokenFull, TokenTables,
			TokenFrom, TokenIdentity, TokenLike, TokenValue,
		})
}

func TestLexSqlCreate(t *testing.T) {
	/*
		CREATE SOURCE
	*/
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

	/*
		CREATE VIEW
	*/
	verifyTokens(t, `CREATE VIEW mysource WITH stuff = "hello";`,
		[]Token{
			tv(TokenCreate, "CREATE"),
			tv(TokenView, "VIEW"),
			tv(TokenIdentity, "mysource"),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "stuff"),
			tv(TokenEqual, "="),
			tv(TokenValue, "hello"),
		})

}
func TestLexSqlCreateTable(t *testing.T) {

	/*
		CREATE TABLE
	*/
	verifyTokens(t, `CREATE TABLE articles 
		 (
		  ID int(11) NOT NULL AUTO_INCREMENT,
		  Email char(150) NOT NULL DEFAULT '',
		  PRIMARY KEY (ID),
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
