package qlparse

import (
	"strings"
)

type Dialect struct {
	Name       string
	Statements []*Statement
}

func (d *Dialect) init() {
	for _, stmt := range d.Statements {
		stmt.init()
	}
}

type Statement struct {
	keyword    string
	Token      TokenType
	Lexer      StateFn
	Statements []*Statement
}

func (s *Statement) init() {
	s.keyword = strings.ToLower(s.Token.String())
	for _, stmt := range s.Statements {
		stmt.init()
	}
}

// type Clause struct {
// 	Keyword  string
// 	Lexer    StateFn
// 	Optional bool
// }

var sqlSelect *Statement = &Statement{
	Statements: []*Statement{
		{Token: TokenSelect, Lexer: LexColumnOrComma},
		{Token: TokenFrom, Lexer: LexColumnOrComma},
	},
}

var SqlDialect *Dialect = &Dialect{
	Statements: []*Statement{sqlSelect},
}
