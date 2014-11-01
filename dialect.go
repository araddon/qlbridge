package qlparse

import (
	"strings"
)

type Dialect struct {
	Name    string
	Clauses []*Clause
}

func (d *Dialect) init() {
	for _, clause := range d.Clauses {
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

var sqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexColumns},
	{Token: TokenFrom, Lexer: LexExpressionOrIdentity},
	{Token: TokenWhere, Lexer: LexColumns, Optional: true},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
}

var SqlDialect *Dialect = &Dialect{
	Clauses: sqlSelect,
}
