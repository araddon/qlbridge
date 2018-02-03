package lex

import (
	"strings"

	u "github.com/araddon/gou"
)

var (

	// SqlDialect is a SQL dialect
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
	//    CREATE (TABLE|VIEW|CONTINUOUSVIEW|SOURCE)
	//
	//  TODO:
	//      CREATE
	//      VIEW
	SqlDialect *Dialect = &Dialect{
		Statements: []*Clause{
			{Token: TokenPrepare, Clauses: SqlPrepare},
			{Token: TokenSelect, Clauses: SqlSelect},
			{Token: TokenUpdate, Clauses: SqlUpdate},
			{Token: TokenUpsert, Clauses: SqlUpsert},
			{Token: TokenInsert, Clauses: SqlInsert},
			{Token: TokenDelete, Clauses: SqlDelete},
			{Token: TokenCreate, Clauses: SqlCreate},
			{Token: TokenDrop, Clauses: SqlDrop},
			{Token: TokenAlter, Clauses: SqlAlter},
			{Token: TokenDescribe, Clauses: SqlDescribe},
			{Token: TokenExplain, Clauses: SqlExplain},
			{Token: TokenDesc, Clauses: SqlDescribeAlt},
			{Token: TokenShow, Clauses: SqlShow},
			{Token: TokenSet, Clauses: SqlSet},
			{Token: TokenUse, Clauses: SqlUse},
			{Token: TokenRollback, Clauses: SqlRollback},
			{Token: TokenCommit, Clauses: SqlCommit},
		},
	}
	// SqlSelect Select statement.
	SqlSelect = []*Clause{
		{Token: TokenSelect, Lexer: LexSelectClause, Name: "sqlSelect.Select"},
		{Token: TokenInto, Lexer: LexInto, Optional: true, Name: "sqlSelect.INTO"},
		{Token: TokenFrom, Lexer: LexTableReferenceFirst, Optional: true, Repeat: false, Clauses: fromSource, Name: "sqlSelect.From"},
		{KeywordMatcher: sourceMatch, Optional: true, Repeat: true, Clauses: moreSources, Name: "sqlSelect.sources"},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Clauses: whereQuery, Name: "sqlSelect.where"},
		{Token: TokenGroupBy, Lexer: LexColumns, Optional: true, Name: "sqlSelect.groupby"},
		{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true, Name: "sqlSelect.having"},
		{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true, Name: "sqlSelect.orderby"},
		{Token: TokenLimit, Lexer: LexLimit, Optional: true, Name: "sqlSelect.limit"},
		{Token: TokenOffset, Lexer: LexNumber, Optional: true, Name: "sqlSelect.offset"},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true, Name: "sqlSelect.with"},
		{Token: TokenAlias, Lexer: LexIdentifier, Optional: true, Name: "sqlSelect.alias"},
		{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false, Name: "sqlSelect.eos"},
	}
	fromSource = []*Clause{
		{KeywordMatcher: sourceMatch, Lexer: LexTableReferenceFirst, Name: "fromSource.matcher"},
		{Token: TokenSelect, Lexer: LexSelectClause, Name: "fromSource.Select"},
		{Token: TokenFrom, Lexer: LexTableReferenceFirst, Optional: true, Repeat: true, Name: "fromSource.From"},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Name: "fromSource.Where"},
		{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true, Name: "fromSource.having"},
		{Token: TokenGroupBy, Lexer: LexColumns, Optional: true, Name: "fromSource.GroupBy"},
		{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true, Name: "fromSource.OrderBy"},
		{Token: TokenLimit, Lexer: LexLimit, Optional: true, Name: "fromSource.Limit"},
		{Token: TokenOffset, Lexer: LexNumber, Optional: true, Name: "fromSource.Offset"},
		{Token: TokenRightParenthesis, Lexer: LexEndOfSubStatement, Optional: true, Name: "fromSource.EndParen"},
		{Token: TokenAs, Lexer: LexIdentifier, Optional: true, Name: "fromSource.As"},
		{Token: TokenOn, Lexer: LexConditionalClause, Optional: true, Name: "fromSource.On"},
	}
	moreSources = []*Clause{
		{KeywordMatcher: sourceMatch, Lexer: LexJoinEntry, Name: "moreSources.JoinEntry"},
		{Token: TokenSelect, Lexer: LexSelectClause, Optional: true, Name: "moreSources.Select"},
		{Token: TokenFrom, Lexer: LexTableReferenceFirst, Optional: true, Repeat: true, Name: "moreSources.From"},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Name: "moreSources.Where"},
		{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true, Name: "moreSources.Having"},
		{Token: TokenGroupBy, Lexer: LexColumns, Optional: true, Name: "moreSources.GroupBy"},
		{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true, Name: "moreSources.OrderBy"},
		{Token: TokenLimit, Lexer: LexLimit, Optional: true, Name: "moreSources.Limit"},
		{Token: TokenOffset, Lexer: LexNumber, Optional: true, Name: "moreSources.Offset"},
		{Token: TokenRightParenthesis, Lexer: LexEndOfSubStatement, Optional: false, Name: "moreSources.EndParen"},
		{Token: TokenAs, Lexer: LexIdentifier, Optional: true, Name: "moreSources.As"},
		{Token: TokenOn, Lexer: LexConditionalClause, Optional: true, Name: "moreSources.On"},
	}
	whereQuery = []*Clause{
		{Token: TokenSelect, Lexer: LexSelectClause, Name: "whereQuery.Select"},
		{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true, Name: "whereQuery.From"},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Name: "whereQuery.Where"},
		{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true, Name: "whereQuery.Having"},
		{Token: TokenGroupBy, Lexer: LexColumns, Optional: true, Name: "whereQuery.GroupBy"},
		{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true, Name: "whereQuery.OrderBy"},
		{Token: TokenLimit, Lexer: LexNumber, Optional: true, Name: "whereQuery.Limit"},
		{Token: TokenRightParenthesis, Lexer: LexEndOfSubStatement, Optional: false, Name: "whereQuery.EOS"},
	}
	// SqlUpdate update statement
	SqlUpdate = []*Clause{
		{Token: TokenUpdate, Lexer: LexIdentifierOfType(TokenTable)},
		{Token: TokenSet, Lexer: LexColumns},
		{Token: TokenWhere, Lexer: LexColumns, Optional: true},
		{Token: TokenLimit, Lexer: LexNumber, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	// SqlUpsert sql upsert
	SqlUpsert = []*Clause{
		{Token: TokenUpsert, Lexer: LexUpsertClause, Name: "upsert.entry"},
		{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
		{Token: TokenLeftParenthesis, Lexer: LexTableColumns, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	// SqlInsert insert statement
	SqlInsert = []*Clause{
		{Token: TokenInsert, Lexer: LexUpsertClause, Name: "insert.entry"},
		{Token: TokenLeftParenthesis, Lexer: LexColumnNames, Optional: true},
		{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
		{Token: TokenSelect, Optional: true, Clauses: insertSubQuery},
		{Token: TokenValues, Lexer: LexTableColumns, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	insertSubQuery = []*Clause{
		{Token: TokenSelect, Lexer: LexSelectClause},
		{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
		{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
		{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
		{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
		{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	}
	// SqlReplace replace statement
	SqlReplace = []*Clause{
		{Token: TokenReplace, Lexer: LexEmpty},
		{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable)},
		{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
		{Token: TokenLeftParenthesis, Lexer: LexTableColumns, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	// SqlDelete delete statement
	SqlDelete = []*Clause{
		{Token: TokenDelete, Lexer: LexEmpty},
		{Token: TokenFrom, Lexer: LexIdentifierOfType(TokenTable)},
		{Token: TokenSet, Lexer: LexColumns, Optional: true},
		{Token: TokenWhere, Lexer: LexColumns, Optional: true},
		{Token: TokenLimit, Lexer: LexNumber, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	// SqlAlter alter statement
	SqlAlter = []*Clause{
		{Token: TokenAlter, Lexer: LexEmpty},
		{Token: TokenTable, Lexer: LexIdentifier},
		{Token: TokenChange, Lexer: LexDdlAlterColumn},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	// SqlCreate CREATE {SCHEMA | DATABASE | SOURCE | TABLE | VIEW | CONTINUOUSVIEW}
	SqlCreate = []*Clause{
		{Token: TokenCreate, Lexer: LexCreate},
		{Token: TokenEngine, Lexer: LexDdlTableStorage, Optional: true},
		{Token: TokenSelect, Clauses: SqlSelect, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	}
	// SqlDrop DROP {SCHEMA | DATABASE | SOURCE | TABLE}
	SqlDrop = []*Clause{
		{Token: TokenDrop, Lexer: LexDrop},
	}
	// SqlDescribe Describe {table,database}
	SqlDescribe = []*Clause{
		{Token: TokenDescribe, Lexer: LexColumns},
	}
	// SqlDescribeAlt alternate spelling of Describe
	SqlDescribeAlt = []*Clause{
		{Token: TokenDesc, Lexer: LexColumns},
	}
	// SqlExplain is alias of describe
	SqlExplain = []*Clause{
		{Token: TokenExplain, Lexer: LexColumns},
	}
	// SqlShow
	SqlShow = []*Clause{
		{Token: TokenShow, Lexer: LexShowClause},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
	}
	// SqlPrepare
	SqlPrepare = []*Clause{
		{Token: TokenPrepare, Lexer: LexPreparedStatement},
		{Token: TokenFrom, Lexer: LexTableReferences},
	}
	// SqlSet
	SqlSet = []*Clause{
		{Token: TokenSet, Lexer: LexColumns},
	}
	// SqlUse
	SqlUse = []*Clause{
		{Token: TokenUse, Lexer: LexIdentifier},
	}
	// SqlRollback
	SqlRollback = []*Clause{
		{Token: TokenRollback, Lexer: LexEmpty},
	}
	// SqlCommit
	SqlCommit = []*Clause{
		{Token: TokenCommit, Lexer: LexEmpty},
	}
)

// NewSqlLexer creates a new lexer for the input string using SqlDialect
// this is sql(ish) compatible parser.
func NewSqlLexer(input string) *Lexer {
	return NewLexer(input, SqlDialect)
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

// LexEndOfSubStatement Look for end of statement defined by either
// a semicolon or end of file.
func LexEndOfSubStatement(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	if strings.ToLower(l.PeekX(2)) == "as" {
		return nil
	}
	l.backup()
	return l.errorToken("Unexpected token:" + l.current())
}

// LexShowClause Handle show statement
//
//    SHOW [FULL] <multi_word_identifier> <identity> <like_or_where>
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
	// u.Debugf("LexShowClause  r= '%v'", string(keyWord))

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
		"function", "functions", "triggers":
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

// LexInto clause
func LexInto(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("LexInto  r= '%v'", string(keyWord))

	switch keyWord {
	case "from":
		return l.errorf("Expected table got %v", keyWord)
	default:
		if IsValidIdentity(keyWord) {
			l.ConsumeWord(keyWord)
			l.Emit(TokenTable)
			return nil
		}
	}
	return nil
}

// LexLimit clause
//
//    LIMIT 1000 OFFSET 100
//    LIMIT 0, 1000
//    LIMIT 1000
func LexLimit(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("LexLimit  r= '%v'", string(keyWord))

	switch keyWord {
	case "offset":
		return nil
	case "", ";":
		return nil
	case ",":
		l.ConsumeWord(keyWord)
		l.Emit(TokenComma)
		return LexNumber
	default:
		if isDigit(l.Peek()) {
			l.Push("LexLimit", LexLimit)
			return LexNumber
		}
	}
	return nil
}

// LexCreate allows us to lex the words after CREATE
//
//    CREATE {SCHEMA|DATABASE|SOURCE} [IF NOT EXISTS] <identity>  <WITH>
//    CREATE {TABLE} <identity> [IF NOT EXISTS] <table_spec> [WITH]
//    CREATE [OR REPLACE] {VIEW|CONTINUOUSVIEW} <identity> AS <select_statement> [WITH]
//
func LexCreate(l *Lexer) StateFn {

	/*
		CREATE TABLE [IF NOT EXISTS] <identity> [WITH]
		CREATE SOURCE [IF NOT EXISTS] <identity> [WITH]
		CREATE [OR REPLACE] VIEW <identity> AS <select_statement> [WITH]
	*/

	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("LexCreate  r= '%v'", string(keyWord))

	switch keyWord {
	case "or":
		l.Push("LexCreate", LexCreate)
		return lexOrReplace
	case "table":
		l.ConsumeWord(keyWord)
		l.Emit(TokenTable)
		l.Push("LexDdlTable", LexDdlTable)
		return nil
	case "source":
		l.ConsumeWord(keyWord)
		l.Emit(TokenSource)
		l.Push("LexIdentifier", LexIdentifier)
		return LexCreate
	case "schema":
		l.ConsumeWord(keyWord)
		l.Emit(TokenSchema)
		l.Push("LexIdentifier", LexIdentifier)
		return LexCreate
	case "database":
		l.ConsumeWord(keyWord)
		l.Emit(TokenDatabase)
		l.Push("LexIdentifier", LexIdentifier)
		return LexCreate
	case "view":
		l.ConsumeWord(keyWord)
		l.Emit(TokenView)
		l.Push("lexAs", lexAs)
		return LexIdentifier
	case "continuousview":
		l.ConsumeWord(keyWord)
		l.Emit(TokenContinuousView)
		l.Push("lexAs", lexAs)
		return LexIdentifier
	case "if":
		l.Push("LexCreate", LexCreate)
		return lexNotExists
	default:
		return nil
	}
	return nil
}
func lexAs(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	switch keyWord {
	case "as":
		l.ConsumeWord(keyWord)
		l.Emit(TokenAs)
	}
	return nil
}
func lexNotExists(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("lexNotExists  r= '%v'", string(keyWord))

	switch keyWord {
	case "if":
		l.ConsumeWord(keyWord)
		l.Emit(TokenIf)
		return lexNotExists
	case "not":
		l.ConsumeWord(keyWord)
		l.Emit(TokenNegate)
		return lexNotExists
	case "exists":
		l.ConsumeWord(keyWord)
		l.Emit(TokenExists)
	}
	return nil
}
func lexOrReplace(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("lexOrReplace  r= '%v'", string(keyWord))

	switch keyWord {
	case "or":
		l.ConsumeWord(keyWord)
		l.Emit(TokenOr)
		return lexOrReplace
	case "replace":
		l.ConsumeWord(keyWord)
		l.Emit(TokenReplace)
		return nil
	}
	return nil
}

// LexDrop allows us to lex the words after DROP
//
//    DROP {DATABASE | SCHEMA} [IF EXISTS] db_name
//
//    DROP [TEMPORARY] TABLE [IF EXISTS] tbl_name [, tbl_name] [RESTRICT | CASCADE]
//
//    DROP INDEX index_name ON tbl_name
//        [algorithm_option | lock_option] ...
func LexDrop(l *Lexer) StateFn {

	/*
		DROP {DATABASE | SCHEMA} [IF EXISTS] db_name

		DROP INDEX index_name ON tbl_name
		    [algorithm_option | lock_option] ...

		algorithm_option:
		    ALGORITHM [=] {DEFAULT|INPLACE|COPY}

		lock_option:
		    LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}
	*/

	l.SkipWhiteSpaces()
	keyWord := strings.ToLower(l.PeekWord())
	//u.Debugf("LexCreate  r= '%v'", string(keyWord))

	switch keyWord {
	case "temporary":
		l.ConsumeWord(keyWord)
		l.Emit(TokenTemp)
		return LexDrop
	case "table":
		l.ConsumeWord(keyWord)
		l.Emit(TokenTable)
	case "source":
		l.ConsumeWord(keyWord)
		l.Emit(TokenSource)
	case "database":
		l.ConsumeWord(keyWord)
		l.Emit(TokenDatabase)
	case "schema":
		l.ConsumeWord(keyWord)
		l.Emit(TokenSchema)
	case "view":
		l.ConsumeWord(keyWord)
		l.Emit(TokenView)
	case "continuousview":
		l.ConsumeWord(keyWord)
		l.Emit(TokenContinuousView)
	default:
		return nil
	}
	l.Push("LexIdentifier", LexIdentifier)
	return lexNotExists
}

// LexDdlTable data definition language table
func LexDdlTable(l *Lexer) StateFn {

	/*
		CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
		    (create_definition,...)
		    [table_options]
		    [partition_options]

		CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
		    [(create_definition,...)]
		    [table_options]
		    [partition_options]
		    [IGNORE | REPLACE]
		    [AS] query_expression

		CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
		    { LIKE old_tbl_name | (LIKE old_tbl_name) }

		CREATE TABLE `City` (
		  `ID` int(11) NOT NULL AUTO_INCREMENT,
		  `Name` char(35) NOT NULL DEFAULT '',
		  `CountryCode` char(3) NOT NULL DEFAULT '',
		  `District` char(20) NOT NULL DEFAULT '',
		  `Population` int(11) NOT NULL DEFAULT '0',
		  PRIMARY KEY (`ID`),
		  KEY `CountryCode` (`CountryCode`),
		  CONSTRAINT `city_ibfk_1` FOREIGN KEY (`CountryCode`)
		     REFERENCES `Country` (`Code`)
		) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8
	*/
	l.SkipWhiteSpaces()
	r := l.Next()

	//u.Debugf("LexDdlTable  r= '%v'", string(r))

	// Cover the logic and grouping
	switch r {
	case '(':
		// Start of columns
		l.Emit(TokenLeftParenthesis)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		return LexIdentifier
	case ')':
		// end of columns
		l.Emit(TokenRightParenthesis)
		return LexDdlTableStorage
	case '-', '/': // comment?
		p := l.Peek()
		if p == '-' {
			l.backup()
			l.Push("LexDdlTable", LexDdlTable)
			return LexInlineComment
		}
		u.Warnf("unhandled comment non inline ")
	case ';':
		l.backup()
		return nil
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("looking table col start:  word=%s", word)
	r = l.Peek()
	if r == ',' {
		l.Emit(TokenComma)
		l.Push("LexDdlTable", l.clauseState())
		return LexExpressionOrIdentity
	}
	if l.isNextKeyword(word) {
		return nil
	}
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 10 {
		l.Push("LexDdlTable", LexDdlTable)
	} else {
		u.Errorf("Gracefully refusing to add more LexDdlTable: ")
	}
	return LexExpressionOrIdentity
}

// LexDdlTableStorage data definition language column (repeated)
//
//     ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8
//
func LexDdlTableStorage(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}

	r := l.Peek()
	if r == '=' {
		return LexEngineKeyValue
	}
	return nil
}

// LexDdlAlterColumn data definition language column alter
//
//   CHANGE col1_old col1_new varchar(10),
//   CHANGE col2_old col2_new TEXT
//   ADD col3 BIGINT AFTER col1_new
//   ADD col2 TEXT FIRST,
//
func LexDdlAlterColumn(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	r := l.Peek()

	//u.Debugf("LexDdlAlterColumn  r= '%v'", string(r))

	// Cover the logic and grouping
	switch r {
	case '-', '/': // comment?
		p := l.Peek()
		if p == '-' {
			l.Push("entryStateFn", l.clauseState())
			return LexInlineComment
		}
	case ')':
		return nil
	case ';':
		return nil
	case ',':
		l.Next()
		l.Emit(TokenComma)
		return l.clauseState()
	}

	word := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for operator:  word=%s", word)
	switch word {
	case "change":
		l.ConsumeWord(word)
		l.Emit(TokenChange)
		return LexDdlAlterColumn
	case "add":
		l.ConsumeWord(word)
		l.Emit(TokenAdd)
		return LexDdlAlterColumn
	case "after":
		l.ConsumeWord(word)
		l.Emit(TokenAfter)
		return LexDdlAlterColumn
	case "first":
		l.ConsumeWord(word)
		l.Emit(TokenFirst)
		return LexDdlAlterColumn

	// Character set is end of ddl column
	case "character": // character set
		cs := strings.ToLower(l.PeekX(len("character set")))
		if cs == "character set" {
			l.ConsumeWord(cs)
			l.Emit(TokenCharacterSet)
			l.Push("LexDdlAlterColumn", l.clauseState())
			return nil
		}

	// Below here are Data Types
	case "text":
		l.ConsumeWord(word)
		l.Emit(TokenTypeText)
		return l.clauseState()
	case "bigint":
		l.ConsumeWord(word)
		l.Emit(TokenTypeBigInt)
		return l.clauseState()
	case "varchar":
		l.ConsumeWord(word)
		l.Emit(TokenTypeVarChar)
		l.Push("LexDdlAlterColumn", l.clauseState())
		l.Push("LexParenRight", LexParenRight)
		return LexListOfArgs

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexDdlAlterColumn", l.clauseState())
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			//u.Infof("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Infof("LexDdlAlterColumn = '%v'", string(r))

	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexDdlAlterColumn", l.clauseState())
	} else {
		u.Errorf("Gracefully refusing to add more LexDdlAlterColumn: ")
	}
	return LexExpressionOrIdentity
}

// LexDdlTableColumn data definition language column (repeated)
//
//   col1_new varchar(10),
//   col2_new TEXT
//
func LexDdlTableColumn(l *Lexer) StateFn {

	/*
		http://dev.mysql.com/doc/refman/5.7/en/create-table.html

		create_definition:
		    col_name column_definition
		  | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
		      [index_option] ...
		  | {INDEX|KEY} [index_name] [index_type] (index_col_name,...)
		      [index_option] ...
		  | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY]
		      [index_name] [index_type] (index_col_name,...)
		      [index_option] ...
		  | {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (index_col_name,...)
		      [index_option] ...
		  | [CONSTRAINT [symbol]] FOREIGN KEY
		      [index_name] (index_col_name,...) reference_definition
		  | CHECK (expr)

		column_definition:
		    data_type [NOT NULL | NULL] [DEFAULT default_value]
		      [AUTO_INCREMENT] [UNIQUE [KEY] | [PRIMARY] KEY]
		      [COMMENT 'string']
		      [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
		      [STORAGE {DISK|MEMORY|DEFAULT}]
		      [reference_definition]
		  | data_type [GENERATED ALWAYS] AS (expression)
		      [VIRTUAL | STORED] [UNIQUE [KEY]] [COMMENT comment]
		      [NOT NULL | NULL] [[PRIMARY] KEY]
	*/
	l.SkipWhiteSpaces()
	r := l.Next()

	//u.Debugf("LexDdlTableColumn  r= '%v'  peek: %s", string(r), l.PeekX(20))

	//
	switch r {
	case '-', '/': // comment?
		p := l.Peek()
		if p == '-' {
			l.backup()
			l.Push("LexDdlTableColumn", LexDdlTableColumn)
			return LexInlineComment
		}
	case ';':
		l.backup()
		return nil
	case '(':
		l.Emit(TokenLeftParenthesis)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		l.Push("LexParenRight", LexParenRight)
		return LexListOfArgs
	case ')':
		l.Emit(TokenRightParenthesis)
		return nil
	case ',':
		l.Emit(TokenComma)
		return LexDdlTableColumn
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for ddl col start:  word=%s", word)
	switch word {
	case "primary":
		l.ConsumeWord(word)
		l.Emit(TokenPrimary)
		return LexDdlTableColumn
	case "not":
		l.ConsumeWord(word)
		l.Emit(TokenNegate)
		return LexDdlTableColumn
	case "null":
		l.ConsumeWord(word)
		l.Emit(TokenNull)
		return LexDdlTableColumn
	case "default":
		l.ConsumeWord(word)
		l.Emit(TokenDefault)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		return LexValue
	case "auto_increment":
		l.ConsumeWord(word)
		l.Emit(TokenIdentity)
		return LexDdlTableColumn
	case "unique":
		l.ConsumeWord(word)
		l.Emit(TokenUnique)
		return LexDdlTableColumn
	case "foreign":
		l.ConsumeWord(word)
		l.Emit(TokenForeign)
		return LexDdlTableColumn
	case "key":
		l.ConsumeWord(word)
		l.Emit(TokenKey)
		return LexDdlTableColumn
	case "constraint":
		l.ConsumeWord(word)
		l.Emit(TokenConstraint)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		return LexIdentifier
	case "references":
		l.ConsumeWord(word)
		l.Emit(TokenReferences)
		return LexDdlTableColumn
	case "comment":
		l.ConsumeWord(word)
		l.Emit(TokenIdentity)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		return LexValue
	// Character set is end of ddl column
	// case "character": // character set
	// 	cs := strings.ToLower(l.PeekX(len("character set")))
	// 	if cs == "character set" {
	// 		l.ConsumeWord(cs)
	// 		l.Emit(TokenCharacterSet)
	// 		l.Push("LexDdlTableColumn", LexDdlTableColumn)
	// 		return nil
	// 	}
	// Below here are Data Types
	case "int", "integer":
		l.ConsumeWord(word)
		l.Emit(TokenTypeInteger)
		p := l.Peek()
		if p == '(' {
			l.Push("LexDdlTableColumn", LexDdlTableColumn)
			l.Push("LexParenRight", LexParenRight)
			return LexListOfArgs
		}
		return LexDdlTableColumn
	case "text":
		l.ConsumeWord(word)
		l.Emit(TokenTypeText)
		return LexDdlTableColumn
	case "bigint":
		l.ConsumeWord(word)
		l.Emit(TokenTypeBigInt)
		p := l.Peek()
		if p == '(' {
			l.Push("LexDdlTableColumn", LexDdlTableColumn)
			l.Push("LexParenRight", LexParenRight)
			return LexListOfArgs
		}
		return LexDdlTableColumn
	case "varchar":
		l.ConsumeWord(word)
		l.Emit(TokenTypeVarChar)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		l.Push("LexParenRight", LexParenRight)
		return LexListOfArgs
	case "char":
		l.ConsumeWord(word)
		l.Emit(TokenTypeChar)
		l.Push("LexDdlTableColumn", LexDdlTableColumn)
		l.Push("LexParenRight", LexParenRight)
		return LexListOfArgs
	default:
		if l.isIdentity() {
			l.ConsumeWord(word)
			l.Emit(TokenIdentity)
			return LexDdlTableColumn
		}
	}

	u.Warnf("Did not find anything %s", word)
	return nil
}

// LexEngineKeyValue key value pairs
//
//    Start with identity for key/value pairs
//    supports keyword DEFAULT
//    supports non-quoted values
//
func LexEngineKeyValue(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	if r == '=' {
		l.ConsumeWord("=")
		l.Emit(TokenEqual)
		l.Push("LexEngineKeyValue", LexEngineKeyValue)
		return LexExpression
	}
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexEngineKeyValue  %q  peek= %v", word, l.PeekX(10))
	switch word {
	case "with":
		return nil
	case "default":
		l.ConsumeWord(word)
		l.Emit(TokenDefault)
		return LexEngineKeyValue
	case "=":
		return LexExpression
	}
	if l.isIdentity() {
		l.Push("LexEngineKeyValue", LexEngineKeyValue)
		return LexExpression
	}
	u.Debugf("Did not find key-value? %v", l.PeekX(20))
	return nil
}
