package qlparser

import (
	u "github.com/araddon/gou"
	"strings"
)

var _ = u.EMPTY

type Dialect struct {
	Name       string
	Statements []*Statement
}

func (m *Dialect) Init() {
	for _, s := range m.Statements {
		s.init()
	}
}

type Statement struct {
	Keyword TokenType
	Clauses []*Clause
}

func (m *Statement) init() {
	for _, clause := range m.Clauses {
		clause.init()
	}
}

type Clause struct {
	keyword   string
	multiWord bool
	Optional  bool
	Token     TokenType
	Lexer     StateFn
	Clauses   []*Clause
}

func (c *Clause) init() {
	c.keyword = strings.ToLower(c.Token.MatchString())
	c.multiWord = c.Token.MultiWord()
	for _, clause := range c.Clauses {
		clause.init()
	}
}

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

// Alter
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
