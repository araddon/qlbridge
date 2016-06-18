package lex

import (
	u "github.com/araddon/gou"
	"strings"
)

var _ = u.EMPTY

var SqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable), Optional: true},
	{Token: TokenFrom, Lexer: LexTableReferenceFirst, Optional: true, Repeat: false, Clauses: fromSource, Name: "sqlSelect.From"},
	{KeywordMatcher: sourceMatch, Optional: true, Repeat: true, Clauses: moreSources, Name: "sqlSelect.sources"},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Clauses: whereQuery, Name: "sqlSelect.where"},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true, Name: "sqlSelect.groupby"},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true, Name: "sqlSelect.having"},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true, Name: "sqlSelect.orderby"},
	{Token: TokenLimit, Lexer: LexLimit, Optional: true},
	{Token: TokenOffset, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}

// find any keyword that starts a source
//    FROM <name>
//    FROM (select ...)
//         [(INNER | LEFT)] JOIN
func sourceMatch(c *Clause, peekWord string, l *Lexer) bool {
	//u.Debugf("%p sourceMatch?   peekWord: %s", c, peekWord)
	switch peekWord {
	case "(":
		return true
	case "select":
		return true
	case "left", "right", "inner", "outer", "join":
		return true
	}
	return false
}

var fromSource = []*Clause{
	{KeywordMatcher: sourceMatch, Lexer: LexTableReferenceFirst, Name: "fromSource.matcher"},
	{Token: TokenSelect, Lexer: LexSelectClause, Name: "fromSource.Select"},
	{Token: TokenFrom, Lexer: LexTableReferenceFirst, Optional: true, Repeat: true, Name: "fromSource.From"},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Name: "fromSource.Where"},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
	{Token: TokenLimit, Lexer: LexLimit, Optional: true},
	{Token: TokenOffset, Lexer: LexNumber, Optional: true},
	{Token: TokenAs, Lexer: LexIdentifier, Optional: true},
	{Token: TokenOn, Lexer: LexConditionalClause, Optional: true},
}

var moreSources = []*Clause{
	{KeywordMatcher: sourceMatch, Lexer: LexJoinEntry, Name: "moreSources.JoinEntry"},
	{Token: TokenSelect, Lexer: LexSelectClause, Optional: true, Name: "moreSources.Select"},
	{Token: TokenFrom, Lexer: LexTableReferenceFirst, Optional: true, Repeat: true, Name: "moreSources.From"},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Name: "moreSources.Where"},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true, Name: "moreSources.Having"},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true, Name: "moreSources.GroupBy"},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true, Name: "moreSources.OrderBy"},
	{Token: TokenLimit, Lexer: LexLimit, Optional: true, Name: "moreSources.Limit"},
	{Token: TokenOffset, Lexer: LexNumber, Optional: true, Name: "moreSources.Offset"},
	{Token: TokenAs, Lexer: LexIdentifier, Optional: true, Name: "moreSources.As"},
	{Token: TokenOn, Lexer: LexConditionalClause, Optional: true, Name: "moreSources.On"},
}

var whereQuery = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
}

var SqlUpdate = []*Clause{
	{Token: TokenUpdate, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexColumns},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlUpsert = []*Clause{
	{Token: TokenUpsert, Lexer: LexUpsertClause, Name: "upsert.entry"},
	{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
	{Token: TokenLeftParenthesis, Lexer: LexTableColumns, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlInsert = []*Clause{
	{Token: TokenInsert, Lexer: LexUpsertClause, Name: "insert.entry"},
	{Token: TokenLeftParenthesis, Lexer: LexColumnNames, Optional: true},
	{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
	{Token: TokenSelect, Optional: true, Clauses: insertSubQuery},
	{Token: TokenValues, Lexer: LexTableColumns, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var insertSubQuery = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
}

var SqlReplace = []*Clause{
	{Token: TokenReplace, Lexer: LexEmpty},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
	{Token: TokenLeftParenthesis, Lexer: LexTableColumns, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlDelete = []*Clause{
	{Token: TokenDelete, Lexer: LexEmpty},
	{Token: TokenFrom, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexColumns, Optional: true},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlAlter = []*Clause{
	{Token: TokenAlter, Lexer: LexEmpty},
	{Token: TokenTable, Lexer: LexIdentifier},
	{Token: TokenChange, Lexer: LexDdlColumn},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlDescribe = []*Clause{
	{Token: TokenDescribe, Lexer: LexColumns},
}

// alternate spelling of Describe
var SqlDescribeAlt = []*Clause{
	{Token: TokenDesc, Lexer: LexColumns},
}

// Explain is alias of describe
var SqlExplain = []*Clause{
	{Token: TokenExplain, Lexer: LexColumns},
}

var SqlShow = []*Clause{
	{Token: TokenShow, Lexer: LexShowClause},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
}

var SqlPrepare = []*Clause{
	{Token: TokenPrepare, Lexer: LexPreparedStatement},
	{Token: TokenFrom, Lexer: LexTableReferences},
}

var SqlSet = []*Clause{
	{Token: TokenSet, Lexer: LexColumns},
}
var SqlUse = []*Clause{
	{Token: TokenUse, Lexer: LexIdentifier},
}

// SqlDialect is a SQL like dialect
//
//    SELECT
//    UPDATE
//    INSERT
//    UPSERT
//    DELETE
//
//    SHOW idenity;
//    DESCRIBE identity;
//    PREPARE
//
// ddl
//    ALTER
//
//  TODO:
//      CREATE
//      VIEW
var SqlDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenPrepare, Clauses: SqlPrepare},
		&Clause{Token: TokenSelect, Clauses: SqlSelect},
		&Clause{Token: TokenUpdate, Clauses: SqlUpdate},
		&Clause{Token: TokenUpsert, Clauses: SqlUpsert},
		&Clause{Token: TokenInsert, Clauses: SqlInsert},
		&Clause{Token: TokenDelete, Clauses: SqlDelete},
		&Clause{Token: TokenAlter, Clauses: SqlAlter},
		&Clause{Token: TokenDescribe, Clauses: SqlDescribe},
		&Clause{Token: TokenExplain, Clauses: SqlExplain},
		&Clause{Token: TokenDesc, Clauses: SqlDescribeAlt},
		&Clause{Token: TokenShow, Clauses: SqlShow},
		&Clause{Token: TokenSet, Clauses: SqlSet},
		&Clause{Token: TokenUse, Clauses: SqlUse},
	},
}

// Handle show statement
//  SHOW [FULL] <multi_word_identifier> <identity> <like_or_where>
//
func LexShowClause(l *Lexer) StateFn {

	/*
	   SHOW {BINARY | MASTER} LOGS
	   SHOW BINLOG EVENTS [IN 'log_name'] [FROM pos] [LIMIT [offset,] row_count]
	   SHOW CHARACTER SET [like_or_where]
	   SHOW COLLATION [like_or_where]
	   SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
	   SHOW CREATE DATABASE db_name
	   SHOW CREATE EVENT event_name
	   SHOW CREATE FUNCTION func_name
	   SHOW CREATE PROCEDURE proc_name
	   SHOW CREATE TABLE tbl_name
	   SHOW CREATE TRIGGER trigger_name
	   SHOW CREATE VIEW view_name
	   SHOW DATABASES [like_or_where]
	   SHOW ENGINE engine_name {STATUS | MUTEX}
	   SHOW [STORAGE] ENGINES
	   SHOW ERRORS [LIMIT [offset,] row_count]
	   SHOW EVENTS
	   SHOW FUNCTION CODE func_name
	   SHOW FUNCTION STATUS [like_or_where]
	   SHOW GRANTS FOR user
	   SHOW INDEX FROM tbl_name [FROM db_name]
	   SHOW MASTER STATUS
	   SHOW OPEN TABLES [FROM db_name] [like_or_where]
	   SHOW PLUGINS
	   SHOW PROCEDURE CODE proc_name
	   SHOW PROCEDURE STATUS [like_or_where]
	   SHOW PRIVILEGES
	   SHOW [FULL] PROCESSLIST
	   SHOW PROFILE [types] [FOR QUERY n] [OFFSET n] [LIMIT n]
	   SHOW PROFILES
	   SHOW SLAVE HOSTS
	   SHOW SLAVE STATUS [NONBLOCKING]
	   SHOW [GLOBAL | SESSION] STATUS [like_or_where]
	   SHOW TABLE STATUS [FROM db_name] [like_or_where]
	   SHOW [FULL] TABLES [FROM db_name] [like_or_where]
	   SHOW TRIGGERS [FROM db_name] [like_or_where]
	   SHOW [GLOBAL | SESSION] VARIABLES [like_or_where]
	   SHOW WARNINGS [LIMIT [offset,] row_count]

	   like_or_where:
	       LIKE 'pattern'
	     | WHERE expr
	*/

	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("LexShowClause  r= '%v'", string(keyWord))

	switch keyWord {
	case "full":
		l.ConsumeWord(keyWord)
		l.Emit(TokenFull)
		return LexShowClause
	case "tables":
		l.ConsumeWord(keyWord)
		l.Emit(TokenTables)
		return LexShowClause
	case "columns", "global", "session", "variables", "status",
		"engine", "engines", "procedure", "indexes", "index", "keys",
		"function", "functions":
		// TODO:  these should not be identities but tokens?
		l.ConsumeWord(keyWord)
		l.Emit(TokenIdentity)
		return LexShowClause
	case "from":
		l.ConsumeWord(keyWord)
		l.Emit(TokenFrom)
		l.Push("LexShowClause", LexShowClause)
		return LexIdentifier
	case "like":
		l.ConsumeWord(keyWord)
		l.Emit(TokenLike)
		return LexValue
	case "create":
		// SHOW CREATE TABLE tbl_name
		l.ConsumeWord(keyWord)
		l.Emit(TokenCreate)
		l.Push("LexIdentifier", LexIdentifier)
		return LexIdentifier
	case "where":
		return nil
	case "", ";":
		return nil
	}
	return LexIdentifier
}

// LexLimit clause
//    LIMIT 1000 OFFSET 100
//    LIMIT 0, 1000
//    LIMIT 1000
func LexLimit(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("LexLimit  r= '%v'", string(keyWord))

	switch keyWord {
	case "limit":
		l.ConsumeWord(keyWord)
		l.Emit(TokenLimit)
		return LexLimit
	case "offset":
		return nil
	case "", ";":
		return nil
	case ",":
		l.ConsumeWord(keyWord)
		l.Emit(TokenComma)
		return LexNumber
	default:
		l.Push("LexLimit", LexLimit)
		return LexNumber
	}
	return nil
}
