package lex

import (
	"strings"

	u "github.com/araddon/gou"
)

var (
	_ = u.EMPTY
)

// Handle Filter QL Main Statement
//
//  FILTER := ( <filter_bool_expr> | <filter_expr> )
//
//  <filter_bool_expr> :=  ( AND | OR ) '(' ( <filter_bool_expr> | <filter_expr> ) [, ( <filter_bool_expr> | <filter_expr> ) ] ')'
//
//  <filter_expr> :=  <expr>
//
// Examples:
//
//    FILTER
///      AND (
//          daysago(datefield) < 100
//          , domain(url) == "google.com"
//          , INCLUDE name_of_filter
//          ,
//          , OR (
//              momentum > 20
//             , propensity > 50
//          )
//       )
//    ALIAS myfilter
//
//   FILTER x > 7
//
func LexFilterClause(l *Lexer) StateFn {

	if l.SkipWhiteSpacesNewLine() {
		l.Emit(TokenNewLine)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	}

	if l.IsComment() {
		l.Push("LexFilterClause", LexFilterClause)
		return LexComment
	}

	keyWord := strings.ToLower(l.PeekWord())

	//u.Debugf("LexFilterClause  r= '%v'", string(keyWord))

	switch keyWord {
	case "include":
		l.ConsumeWord(keyWord)
		l.Emit(TokenInclude)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
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
	case "not":
		l.ConsumeWord(keyWord)
		l.Emit(TokenNegate)
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
	l.init()
	return l
}

/*
var SqlSelect = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause},
	{Token: TokenInto, Lexer: LexIdentifierOfType(TokenTable), Optional: true},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: true, Repeat: true, Clauses: sqlSubQuery},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true, Clauses: sqlSubQuery},
	{Token: TokenGroupBy, Lexer: LexColumns, Optional: true},
	{Token: TokenHaving, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenOrderBy, Lexer: LexOrderByColumn, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJson, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}
*/
var FilterStatement = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause, Optional: true},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: false},
	{Token: TokenFilter, Lexer: LexFilterClause, Optional: true},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}

// FilterQL is a Where Clause filtering language slightly
//   more DSL'ish than SQL Where Clause
//
var FilterQLDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenNil, Clauses: FilterStatement},
	},
}
