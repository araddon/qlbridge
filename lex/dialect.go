package lex

import (
	u "github.com/araddon/gou"
	"strings"
)

var _ = u.EMPTY

// Dialect is a Language made up of multiple Statement Options
//   SQL
//   CQL
//   INFLUXQL   etc
//
type Dialect struct {
	Name       string
	Statements []*Clause
	inited     bool
}

func (m *Dialect) Init() {
	if m.inited {
		return
	}
	m.inited = true
	for _, s := range m.Statements {
		s.init()
	}
}

type Clause struct {
	parent    *Clause
	next      *Clause
	prev      *Clause
	keyword   string    // keyword is firstWord, not full word, "GROUP" portion of "GROUP BY"
	fullWord  string    // In event multi word such as "GROUP BY"
	multiWord bool      // flag if is multi-word
	Optional  bool      // Is this Clause/Keyword optional?
	Repeat    bool      // Repeatable clause?
	Token     TokenType // Token identifiyng start of clause, optional
	Lexer     StateFn   // Optional special Lex Function
	Clauses   []*Clause // Children Clauses
}

func (c *Clause) init() {
	// Find the Keyword, MultiWord options
	c.fullWord = c.Token.String()
	c.keyword = strings.ToLower(c.Token.MatchString())
	c.multiWord = c.Token.MultiWord()
	for i, clause := range c.Clauses {
		clause.init()
		if i != 0 {
			clause.prev = c.Clauses[i-1]
		}
		if i+1 < len(c.Clauses) {
			clause.next = c.Clauses[i+1]
		}
	}
}
