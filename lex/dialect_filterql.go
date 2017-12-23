package lex

import (
	"strings"
)

var (
	// FilterStatement a FilterQL statement.
	FilterStatement = []*Clause{
		{Token: TokenFilter, Lexer: LexFilterClause, Optional: true},
		{Token: TokenFrom, Lexer: LexIdentifier, Optional: true},
		{Token: TokenLimit, Lexer: LexNumber, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
		{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
		{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
	}
	// FilterSelectStatement Filter statement that also supports column projection.
	FilterSelectStatement = []*Clause{
		{Token: TokenSelect, Lexer: LexSelectClause, Optional: false},
		{Token: TokenFrom, Lexer: LexIdentifier, Optional: false},
		{Token: TokenWhere, Lexer: LexConditionalClause, Optional: true},
		{Token: TokenFilter, Lexer: LexFilterClause, Optional: true},
		{Token: TokenLimit, Lexer: LexNumber, Optional: true},
		{Token: TokenWith, Lexer: LexJsonOrKeyValue, Optional: true},
		{Token: TokenAlias, Lexer: LexIdentifier, Optional: true},
		{Token: TokenEOF, Lexer: LexEndOfStatement, Optional: false},
	}
	// FilterQLDialect is a Where Clause filtering language slightly
	// more DSL'ish than SQL Where Clause.
	FilterQLDialect *Dialect = &Dialect{
		Statements: []*Clause{
			{Token: TokenFilter, Clauses: FilterStatement},
			{Token: TokenSelect, Clauses: FilterSelectStatement},
		},
		IdentityQuoting: IdentityQuotingWSingleQuote,
	}
)

// NewFilterQLLexer creates a new lexer for the input string using FilterQLDialect
// which is dsl for where/filtering.
func NewFilterQLLexer(input string) *Lexer {
	return NewLexer(input, FilterQLDialect)
}

// LexFilterClause Handle Filter QL Main Statement
//
//    FILTER := ( <filter_bool_expr> | <filter_expr> )
//
//    <filter_bool_expr> :=  ( AND | OR ) '(' ( <filter_bool_expr> | <filter_expr> ) [, ( <filter_bool_expr> | <filter_expr> ) ] ')'
//
//    <filter_expr> :=  <expr>
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
//    FILTER x > 7
//
func LexFilterClause(l *Lexer) StateFn {

	if l.SkipWhiteSpacesNewLine() {
		l.Emit(TokenNewLine)
		debugf("%p LexFilterClause emit new line stack=%d", l, len(l.stack))
		l.Push("LexFilterClause", LexFilterClause)
		return LexFilterClause
	}

	if l.IsComment() {
		l.Push("LexFilterClause", LexFilterClause)
		debugf("%p LexFilterClause comment stack=%d", l, len(l.stack))
		return LexComment
	}

	keyWord := strings.ToLower(l.PeekWord())

	debugf("%p LexFilterClause  r=%-15q stack=%d", l, string(keyWord), len(l.stack))

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
	return LexExpression
}
