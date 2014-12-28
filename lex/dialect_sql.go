package lex

import ()

var SqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexColumns},
	{Token: TokenFrom, Lexer: LexExpressionOrIdentity, Optional: true},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenHaving, Lexer: LexColumns, Optional: true},
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

// SqlDialect is a SQL like dialect
//
//    SELECT
//    UPDATE
//    INSERT
//    DELETE
//
//    SHOW idenity;
//    DESCRIBE identity;
// ddl
//    ALTER
//
//  TODO:
//      CREATE
var SqlDialect *Dialect = &Dialect{
	Statements: []*Statement{
		&Statement{TokenSelect, SqlSelect},
		&Statement{TokenUpdate, SqlUpdate},
		&Statement{TokenInsert, SqlInsert},
		&Statement{TokenDelete, SqlDelete},
		&Statement{TokenAlter, SqlAlter},
		&Statement{TokenDescribe, SqlDescribe},
		&Statement{TokenShow, SqlShow},
	},
}
