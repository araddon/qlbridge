package lex

import ()

var SqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable), Optional: true},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true, Clauses: sqlSubQuery},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Clauses: sqlSubQuery},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}

var sqlSubQuery = []*Clause{
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
	{Token: TokenUpsert, Lexer: nil},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
	{Token: TokenLeftParenthesis, Lexer: LexTableColumns, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlInsert = []*Clause{
	{Token: TokenInsert, Lexer: nil},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenLeftParenthesis, Lexer: LexColumnNames, Optional: true},
	{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
	{Token: TokenSelect, Optional: true, Clauses: sqlSubQuery},
	{Token: TokenValues, Lexer: LexTableColumns, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlReplace = []*Clause{
	{Token: TokenReplace, Lexer: nil},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexTableColumns, Optional: true},
	{Token: TokenLeftParenthesis, Lexer: LexTableColumns, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlDelete = []*Clause{
	{Token: TokenDelete, Lexer: nil},
	{Token: TokenFrom, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexColumns, Optional: true},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
}

var SqlAlter = []*Clause{
	{Token: TokenAlter, Lexer: nil},
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
	{Token: TokenShow, Lexer: LexColumns},
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
