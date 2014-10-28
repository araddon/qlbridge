package qlparse

import (
	u "github.com/araddon/gou"
	"strings"
)

var _ = u.EMPTY

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
	u.Debugf("match keyword: %v", c.keyword)
	for _, clause := range c.Clauses {
		clause.init()
	}
}

var sqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexColumnOrComma},
	{Token: TokenFrom, Lexer: LexExpressionOrIdentity},
	{Token: TokenWhere, Lexer: LexWhereColumn, Optional: true},
	{Token: TokenGroupBy, Lexer: LexGroupByColumns, Optional: true},
}

var SqlDialect *Dialect = &Dialect{
	Clauses: sqlSelect,
}
