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
		return LexFilterClause
	}

	if l.IsComment() {
		l.Push("LexFilterClause", LexFilterClause)
		return LexComment
	}

	keyWord := strings.ToLower(l.PeekWord())

	//u.Debugf("%p LexFilterClause  r=%-15q stack=%d", l, string(keyWord), len(l.stack))

	switch keyWord {
	case "from", "with":
		return nil
	case "include":
		l.ConsumeWord(keyWord)
		l.Emit(TokenInclude)
		l.Push("LexFilterClause", LexFilterClause)
		return LexIdentifier
	case "and":
		l.ConsumeWord(keyWord)
		l.Emit(TokenLogicAnd)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case "or":
		l.ConsumeWord(keyWord)
		l.Emit(TokenLogicOr)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case "not":
		l.ConsumeWord(keyWord)
		l.Emit(TokenNegate)
		//l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case "(":
		l.ConsumeWord(keyWord)
		l.Emit(TokenLeftParenthesis)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case ",":
		l.ConsumeWord(keyWord)
		l.Emit(TokenComma)
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	case ")":
		l.ConsumeWord(keyWord)
		l.Emit(TokenRightParenthesis)
		return nil
	}
	//l.Push("LexFilterClause", LexFilterClause)
	return LexExpression
}

// creates a new lexer for the input string using FilterQLDialect
//  which is dsl for where/filtering
//
func NewFilterQLLexer(input string) *Lexer {
	return NewLexer(input, FilterQLDialect)
}

var FilterStatement = []*Clause{
	{Token: TokenFilter, Lexer: LexFilterClause, Optional: true},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}

var FilterSelectStatement = []*Clause{
	{Token: TokenSelect, Lexer: LexSelectClause, Optional: false},
	{Token: TokenFrom, Lexer: LexTableReferences, Optional: false},
	{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
	{Token: TokenFilter, Lexer: LexFilterClause, Optional: true},
	{Token: TokenLimit, Lexer: LexNumber, Optional: true},
	{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
	{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
	{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
}

// FilterQL is a Where Clause filtering language slightly
//   more DSL'ish than SQL Where Clause
//
var FilterQLDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenFilter, Clauses: FilterStatement},
		&Clause{Token: TokenSelect, Clauses: FilterSelectStatement},
	},
	IdentityQuoting: IdentityQuotingWSingleQuote,
}
