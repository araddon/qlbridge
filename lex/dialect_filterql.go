package lex

import (
	"strings"

	u "github.com/araddon/gou"
)

var (
	_ = u.EMPTY
)

// creates a new lexer for the input string using SqlDialect
//  this is sql(ish) compatible parser
//
func NewFilterQLLexer(input string) *Lexer {
	// Two tokens of buffering is sufficient for all state functions.
	l := &Lexer{
		input:   input,
		state:   LexDialectForStatement,
		tokens:  make(chan Token, 1),
		stack:   make([]NamedStateFn, 0, 10),
		dialect: FilterQLDialect,
	}
	l.ReverseTrim()
	return l
}

// Handle Filter QL Main Statement
//  FILTER := <filter_bool_expr>
//
//  <filter_bool_expr> :=  ( AND | OR )'(' ( <filter_bool_expr> | <filter_expr> ) [, ( <filter_bool_expr> | <filter_expr> ) ] ')'
//
//  <filter_expr> :=  <expr>
//
// Examples:
//
//    FILTER
///      AND (
//          daysago(datefield) < 100
//          , domain(url) == "google.com"
//          , OR (
//              momentum > 20
//             , propensity > 50
//          )
//       )
//
func LexFilterClause(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	if l.IsComment() {
		l.Push("LexFilterClause", LexFilterClause)
		return LexComment
	}

	keyWord := strings.ToLower(l.PeekWord())

	//u.Debugf("LexFilterClause  r= '%v'", string(keyWord))

	switch keyWord {
	case "and":
		l.ConsumeWord(keyWord)
		l.Emit(TokenAnd)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case "or":
		l.ConsumeWord(keyWord)
		l.Emit(TokenOr)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case "(":
		l.ConsumeWord(keyWord)
		l.Emit(TokenLeftParenthesis)
		l.Push("LexFilterClause", LexFilterClause)
		return LexExpression
	case ")":
		l.ConsumeWord(keyWord)
		l.Emit(TokenRightParenthesis)
		return nil
	}
	return LexExpression
}

/*

var expressionStatement = []*Clause{
	{Token: TokenIdentity, Lexer: LexExpressionOrIdentity},
}

// ExpressionDialect, is a Single Expression dialect, useful for parsing Single
// function
//
//    eq(tolower(item_name),"buy")
var ExpressionDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenNil, Clauses: expressionStatement},
	},
}

var logicalEpressions = []*Clause{
	{Token: TokenNil, Lexer: LexLogical},
}

// logical Expression Statement of the following functional format
//
//   5 > 4   => true
//   4 + 5   => 9
//   tolower(item) + 12 > 4
//   4 IN (4,5,6)
//
var LogicalExpressionDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenNil, Clauses: logicalEpressions},
	},
}


*/
var FilterStatement = []*Clause{
	{Token: TokenFilter, Lexer: LexFilterClause, Optional: false},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}

// FilterQL is a Where Clause filtering language slightly
//   more DSL'ish than SQL Where Clause
//
var FilterQLDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenFilter, Clauses: FilterStatement},
	},
}
