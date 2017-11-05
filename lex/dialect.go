package lex

import (
	"fmt"
	"strings"
)

type (
	// KeywordMatcher A Clause may supply a keyword matcher instead of keyword-token
	KeywordMatcher func(c *Clause, peekWord string, l *Lexer) bool

	// Dialect is a Language made up of multiple Statements.
	// Examples are {SQL, CQL, GRAPHQL}
	Dialect struct {
		Name            string
		Statements      []*Clause
		IdentityQuoting []byte
		inited          bool
	}
	// Clause is a unique "Section" of a statement
	Clause struct {
		parent         *Clause
		next           *Clause
		prev           *Clause
		keyword        string    // keyword is firstWord, not full word, "GROUP" portion of "GROUP BY"
		fullWord       string    // In event multi word such as "GROUP BY"
		multiWord      bool      // flag if is multi-word
		Optional       bool      // Is this Clause/Keyword optional?
		Repeat         bool      // Repeatable clause?
		Token          TokenType // Token identifiyng start of clause, optional
		KeywordMatcher KeywordMatcher
		Lexer          StateFn   // Lex Function to lex clause, optional
		Clauses        []*Clause // Children Clauses
		Name           string
	}
)

// Init Dialects have one time load-setup.
func (m *Dialect) Init() {
	if m.inited {
		return
	}
	m.inited = true
	for _, s := range m.Statements {
		s.init()
	}
}

// MatchesKeyword
func (c *Clause) MatchesKeyword(peekWord string, l *Lexer) bool {
	if c.KeywordMatcher != nil {
		return c.KeywordMatcher(c, peekWord, l)
	} else if c.keyword == peekWord && !c.multiWord {
		return true
	} else if c.multiWord {
		if strings.ToLower(l.PeekX(len(c.fullWord))) == c.fullWord {
			return true
		}
	}
	return false
}
func (c *Clause) init() {
	if c.KeywordMatcher == nil {
		// Find the Keyword, MultiWord options
		c.fullWord = c.Token.String()
		c.keyword = strings.ToLower(c.Token.MatchString())
		c.multiWord = c.Token.MultiWord()
	}
	for i, clause := range c.Clauses {
		clause.init()
		clause.parent = c
		if i != 0 { // .prev is nil on first clause
			clause.prev = c.Clauses[i-1]
		}
		if i+1 < len(c.Clauses) { // .next is nil on last clause
			clause.next = c.Clauses[i+1]
		}
	}
}
func (c *Clause) String() string {
	if c.parent != nil {
		return fmt.Sprintf(`<clause %p %q kw=%q fullword=%q multiword?%v clausesct=%d parentKw=%q />`, c, c.Name, c.keyword, c.fullWord, c.multiWord, len(c.Clauses), c.parent.keyword)
	}
	return fmt.Sprintf(`<clause %p %q kw=%q full=%q multiword?%v clausesct=%d />`, c, c.Name, c.keyword, c.fullWord, c.multiWord, len(c.Clauses))
}
