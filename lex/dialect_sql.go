package lex

import ()

// SELECT
var SqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexColumns},
	{Token: TokenFrom, Lexer: LexExpressionOrIdentity},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
}

// UPDATE
var SqlUpdate = []*Clause{
	{Token: TokenUpdate, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexColumns},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
}

// INSERT
var SqlInsert = []*Clause{
	{Token: TokenInsert, Lexer: nil},
	{Token: TokenInto, Lexer: LexTableNameColumns},
}

// DELETE
var SqlDelete = []*Clause{
	{Token: TokenDelete, Lexer: nil},
	{Token: TokenFrom, Lexer: LexIdentifierOfType(TokenTable)},
	{Token: TokenSet, Lexer: LexColumns, Optional: true},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
}

// ALTER
var SqlAlter = []*Clause{
	{Token: TokenAlter, Lexer: nil},
	{Token: TokenTable, Lexer: LexIdentifier},
	{Token: TokenChange, Lexer: LexDdlColumn},
}

var SqlDialect *Dialect = &Dialect{
	Statements: []*Statement{
		&Statement{TokenSelect, SqlSelect},
		&Statement{TokenUpdate, SqlUpdate},
		&Statement{TokenInsert, SqlInsert},
		&Statement{TokenDelete, SqlDelete},
		&Statement{TokenAlter, SqlAlter},
	},
}
