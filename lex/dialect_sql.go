package lex

import ()

var SqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true, Clauses: sqlSubQuery},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Clauses: sqlSubQuery},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
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
}

var SqlInsert = []*Clause{
	{Token: TokenInsert, Lexer: nil},
	{Token: TokenInto, Lexer: LexTableNameColumns},
}

var SqlDelete = []*Clause{
	{Token: TokenDelete, Lexer: nil},
	{Token: TokenFrom, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexColumns, Optional: true},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
}

var SqlAlter = []*Clause{
	{Token: TokenAlter, Lexer: nil},
	{Token: TokenTable, Lexer: LexIdentifier},
	{Token: TokenChange, Lexer: LexDdlColumn},
}

var SqlDescribe = []*Clause{
	{Token: TokenDescribe, Lexer: LexColumns},
}

var SqlShow = []*Clause{
	{Token: TokenShow, Lexer: LexColumns},
}

var SqlPrepare = []*Clause{
	{Token: TokenPrepare, Lexer: LexPreparedStatement},
	{Token: TokenFrom, Lexer: LexTableReferences},
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
	Statements: []*Statement{
		&Statement{TokenPrepare, SqlPrepare},
		&Statement{TokenSelect, SqlSelect},
		&Statement{TokenUpdate, SqlUpdate},
		&Statement{TokenInsert, SqlInsert},
		&Statement{TokenDelete, SqlDelete},
		&Statement{TokenAlter, SqlAlter},
		&Statement{TokenDescribe, SqlDescribe},
		&Statement{TokenShow, SqlShow},
	},
}
