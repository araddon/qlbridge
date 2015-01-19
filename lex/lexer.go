package lex

import (
	"bytes"
	"fmt"
	u "github.com/araddon/gou"
	"strings"
	"unicode"
	"unicode/utf8"
)

var _ = u.EMPTY

var (
	// FEATURE FLAGS
	SUPPORT_DURATION = true
	// Identity Quoting
	//  http://stackoverflow.com/questions/1992314/what-is-the-difference-between-single-and-double-quotes-in-sql
	// you might want to set this to not include single ticks
	//  http://dev.mysql.com/doc/refman/5.1/en/string-literals.html
	IdentityQuoting = []byte{'[', '`', '"'} // mysql ansi-ish, no single quote identities, and allowing double-quote
	// IdentityQuoting = []byte{'[', '`', '\''} // more ansi-ish, allow double quotes around identities
)

const (
	eof       = -1
	decDigits = "0123456789"
	hexDigits = "0123456789ABCDEF"
)

// StateFn represents the state of the lexer as a function that returns the
// next state.
type StateFn func(*Lexer) StateFn

type NamedStateFn struct {
	Name    string
	StateFn StateFn
}

// Creates a new lexer for the input string
//
func NewLexer(input string, dialect *Dialect) *Lexer {
	// Two tokens of buffering is sufficient for all state functions.
	l := &Lexer{
		input:   input,
		state:   LexDialectForStatement,
		tokens:  make(chan Token, 1),
		stack:   make([]NamedStateFn, 0, 10),
		dialect: dialect,
	}
	return l
}

// creates a new lexer for the input string using SqlDialect
//  this is sql(ish) compatible parser
//
func NewSqlLexer(input string) *Lexer {
	// Two tokens of buffering is sufficient for all state functions.
	l := &Lexer{
		input:   input,
		state:   LexDialectForStatement,
		tokens:  make(chan Token, 1),
		stack:   make([]NamedStateFn, 0, 10),
		dialect: SqlDialect,
	}
	return l
}

// lexer holds the state of the lexical scanning.
//
// Based on the lexer from the "text/template" package.
// See http://www.youtube.com/watch?v=HxaD_trXwRE
type Lexer struct {
	input         string     // the string being scanned.
	state         StateFn    // the next lexing function to enter
	entryStateFn  StateFn    // The current clause top level StateFn
	pos           int        // current position in the input
	start         int        // start position of this token
	width         int        // width of last rune read from input
	lastToken     Token      // last token we emitted
	tokens        chan Token // channel of scanned tokens.
	doubleDelim   bool       // flag for tags starting with double braces
	dialect       *Dialect
	statement     *Statement
	curClause     *Clause
	statementPos  int
	peekedWordPos int
	peekedWord    string

	// Due to nested Expressions and evaluation this allows us to descend/ascend
	// during lex, using push/pop to add and remove states needing evaluation
	stack []NamedStateFn
}

// returns the next token from the input.
func (l *Lexer) NextToken() Token {

	for {
		//u.Debugf("token: start=%v  pos=%v  peek5=%s", l.start, l.pos, l.peekX(5))
		select {
		case token := <-l.tokens:
			return token
		default:
			if l.state == nil && len(l.stack) > 0 {
				l.state = l.pop()
			} else if l.state == nil {
				//u.Error("no state? ")
				//panic("no state?")
				return Token{T: TokenEOF, V: ""}
			}
			//u.Debugf("calling l.state()")
			l.state = l.state(l)
		}
	}
	panic("not reached")
}

func (l *Lexer) Push(name string, state StateFn) {
	//u.LogTracef(u.INFO, "pushed item onto stack: %v", len(l.stack))
	//u.Infof("pushed item onto stack: %v  %v", name, len(l.stack))
	l.stack = append(l.stack, NamedStateFn{name, state})
}

func (l *Lexer) pop() StateFn {
	if len(l.stack) == 0 {
		return l.errorf("BUG in lexer: no states to pop.")
	}
	li := len(l.stack) - 1
	last := l.stack[li]
	l.stack = l.stack[0:li]
	//u.Infof("popped item off stack:  %v", last.Name)
	return last.StateFn
}

// next returns the next rune in the input.
func (l *Lexer) Next() (r rune) {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return r
}

func (l *Lexer) skipX(ct int) {
	for i := 0; i < ct; i++ {
		l.Next()
	}
}

// peek returns but does not consume the next rune in the input.
func (l *Lexer) Peek() rune {
	r := l.Next()
	l.backup()
	return r
}

// grab the next x characters without consuming
func (l *Lexer) peekX(x int) string {
	if l.pos+x > len(l.input) {
		return l.input[l.pos:]
	}
	return l.input[l.pos : l.pos+x]
}

// lets grab the next word (till whitespace, without consuming)
func (l *Lexer) PeekWord2() string {

	skipWs := 0
	for ; skipWs < len(l.input)-l.pos; skipWs++ {
		r, _ := utf8.DecodeRuneInString(l.input[l.pos+skipWs:])
		if !unicode.IsSpace(r) {
			break
		}
	}

	word := ""
	for i := skipWs; i < len(l.input)-l.pos; i++ {
		r, _ := utf8.DecodeRuneInString(l.input[l.pos+i:])
		if unicode.IsSpace(r) || !isIdentifierRune(r) {
			u.Infof("hm:   '%v' word='%s' %v", l.input[l.pos:l.pos+i], word, l.input[l.pos:l.pos+i] == word)
			return word
		} else {
			word = word + string(r)
		}
	}
	return word
}

// lets grab the next word (till whitespace, without consuming)
func (l *Lexer) PeekWord() string {

	// TODO:  optimize this, this is by far the most expensive operation
	//  in the lexer
	//    - move to some type of early bail?  ie, use Accept() whereever possible?
	skipWs := 0
	for ; skipWs < len(l.input)-l.pos; skipWs++ {
		r, ri := utf8.DecodeRuneInString(l.input[l.pos+skipWs:])
		if ri != 1 {
			//skipWs += (ri - 1)
		}
		if !unicode.IsSpace(r) {
			break
		}
	}

	for i := skipWs; i < len(l.input)-l.pos; i++ {
		r, ri := utf8.DecodeRuneInString(l.input[l.pos+i:])
		if ri != 1 {
			//i += (ri - 1)
		}
		if unicode.IsSpace(r) || !isIdentifierRune(r) {
			if i > 0 {
				//u.Infof("hm:   '%v'", l.input[l.pos+skipWs:l.pos+i])
				return l.input[l.pos+skipWs : l.pos+i]
			}
		}
	}
	return ""
}

// peek word, but using laxIdentifier characters
func (l *Lexer) peekLaxWord() string {
	word := ""
	for i := 0; i < len(l.input)-l.pos; i++ {
		r, _ := utf8.DecodeRuneInString(l.input[l.pos+i:])
		if !isLaxIdentifierRune(r) {
			return word
		} else {
			word = word + string(r)
		}
	}
	return word
}

// backup steps back one rune. Can only be called once per call of next.
func (l *Lexer) backup() {
	l.pos -= l.width
}

// have we consumed all input
func (l *Lexer) isEnd() bool {
	if l.pos >= len(l.input) {
		return true
	}
	// if r := l.Peek(); r == ';' {
	// 	return true
	// }
	return false
}

// emit passes an token back to the client.
func (l *Lexer) Emit(t TokenType) {
	//u.Debugf("emit: %s  '%s'", t, l.input[l.start:l.pos])
	l.lastToken = Token{T: t, V: l.input[l.start:l.pos], Pos: l.start}
	l.tokens <- l.lastToken
	l.start = l.pos
}

// ignore skips over the pending input before this point.
func (l *Lexer) ignore() {
	l.start = l.pos
}

// ignore skips over the item
func (l *Lexer) ignoreWord(word string) {
	l.pos += len(word)
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
func (l *Lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.Next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *Lexer) acceptRun(valid string) bool {
	pos := l.pos
	for strings.IndexRune(valid, l.Next()) >= 0 {
	}
	l.backup()
	return l.pos > pos
}

// Returns current string not yet emitted
func (l *Lexer) current() string {
	str := l.input[l.start:l.pos]
	l.start = l.pos
	return str
}

// Returns remainder of input not yet lexed
func (l *Lexer) remainder() string {
	return l.input[l.start : len(l.input)-1]
}

// lets move position to consume given word
func (l *Lexer) ConsumeWord(word string) {
	// pretty sure the len(word) is valid right?
	l.pos += len(word)
}

// lineNumber reports which line we're on. Doing it this way
// means we don't have to worry about peek double counting.
func (l *Lexer) lineNumber() int {
	return 1 + strings.Count(l.input[:l.pos], "\n")
}

// columnNumber reports which column in the current line we're on.
func (l *Lexer) columnNumber() int {
	n := strings.LastIndex(l.input[:l.pos], "\n")
	if n == -1 {
		n = 0
	}
	return l.pos - n
}

// error returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextToken.
func (l *Lexer) errorf(format string, args ...interface{}) StateFn {
	l.tokens <- Token{T: TokenError, V: fmt.Sprintf(format, args...)}
	return nil
}

// Skips white space characters in the input.
func (l *Lexer) SkipWhiteSpaces() {
	for rune := l.Next(); unicode.IsSpace(rune); rune = l.Next() {
	}
	l.backup()
	l.ignore()
}

// Scans input and matches against the string.
// Returns true if the expected string was matched.
// expects matchTo to be a lower case string
func (l *Lexer) match(matchTo string, skip int) bool {

	//u.Debugf("match() : %v", matchTo)
	for _, matchRune := range matchTo {
		//u.Debugf("match rune? %v", string(matchRune))
		if skip > 0 {
			skip--
			continue
		}

		nr := l.Next()
		//u.Debugf("rune=%s n=%s   %v  %v", string(matchRune), string(nr), matchRune != nr, unicode.ToLower(nr) != matchRune)
		if matchRune != nr && unicode.ToLower(nr) != matchRune {
			//u.Debugf("setting done = false?, ie did not match")
			return false
		}
	}
	// If we finished looking for the match word, and the next item is not
	// whitespace, it means we failed
	if !isWhiteSpace(l.Peek()) {
		return false
	}
	//u.Debugf("Found match():  %v", matchTo)
	return true
}

// Scans input and tries to match the expected string.
// Returns true if the expected string was matched.
// Does not advance the input if the string was not matched.
//
// NOTE:  this assumes the @val you are trying to match against is LOWER CASE
func (l *Lexer) tryMatch(matchTo string) bool {
	i := 0
	//u.Debugf("tryMatch:  start='%v'", l.PeekWord())
	for _, matchRune := range matchTo {
		i++
		nextRune := l.Next()
		if unicode.ToLower(nextRune) != matchRune {
			for ; i > 0; i-- {
				l.backup()
			}
			//u.Warnf("not found:  %v:%v", string(nextRune), matchTo)
			return false
		}
	}
	//u.Debugf("tryMatch:  good='%v'", matchTo)
	return true
}

// Emits an error token and terminates the scan
// by passing back a nil ponter that will be the next state
// terminating lexer.next function
func (l *Lexer) errorToken(format string, args ...interface{}) StateFn {
	//fmt.Sprintf(format, args...)
	l.Emit(TokenError)
	return nil
}

// non-consuming isExpression, expressions are defined by
//  starting with
//    - negation (!)
//    - non quoted alpha character
//
func (l *Lexer) isExpr() bool {
	// Expressions are strings not values, so quoting them means no
	if r := l.Peek(); r == '\'' {
		return false
	} else if isDigit(r) {
		return false
	} else if r == '!' {
		u.Debugf("found negation! : %v", string(r))
		// Negation is possible?
		l.Next()
		if l.isExpr() {
			l.backup()
			return true
		}
		l.backup()
	}
	// Expressions are terminated by either a parenthesis
	// never by spaces
	for i := 0; i < len(l.input)-l.pos; i++ {
		r, _ := utf8.DecodeRuneInString(l.input[l.pos+i:])
		if r == '(' && i > 0 {
			return true
		} else if unicode.IsSpace(r) {
			return false
		} else if !isAlNumOrPeriod(r) {
			return false
		} // else isAlNumOrPeriod so keep looking
	}
	return false
}

// non-consuming check to see if we are about to find next keyword
func (l *Lexer) isNextKeyword(peekWord string) bool {

	if len(peekWord) == 0 {
		return false
	}
	kwMaybe := strings.ToLower(peekWord)
	//u.Debugf("isNextKeyword?  '%s'   pos:%v len:%v", kwMaybe, l.statementPos, len(l.statement.Clauses))
	var clause *Clause
	//u.Infof("clause: %+v", l.statement.Clauses[l.statementPos])

	for i := l.statementPos; i < len(l.statement.Clauses); i++ {
		clause = l.statement.Clauses[i]
		//u.Infof("clause: %+v", clause)
		//u.Debugf("clause next keyword?    peek=%s  keyword=%v multi?%v children?%v", kwMaybe, clause.keyword, clause.multiWord, len(clause.Clauses))
		if clause.keyword == kwMaybe || (clause.multiWord && strings.ToLower(l.peekX(len(clause.fullWord))) == clause.fullWord) {
			//u.Infof("return true:  %v", strings.ToLower(l.peekX(len(clause.fullWord))))
			return true
		}
		switch kwMaybe {
		case "select", "insert", "delete", "update", "from":
			//u.Warnf("doing true: %v", kwMaybe)
			return true
		}
		if !clause.Optional {
			return false
		}
	}

	return false
}

// non-consuming isIdentity
//  Identities are non-numeric string values that are not quoted
func (l *Lexer) isIdentity() bool {
	// Identity are strings not values
	r := l.Peek()
	switch {
	case isIdentityQuoteMark(r):
		// are these always identities?  or do we need
		// to also check first identifier
		peek2 := l.peekX(2)
		if len(peek2) == 2 {
			return isIdentifierFirstRune(rune(peek2[1]))
		}
		return false
	}
	return isIdentifierFirstRune(r)
}

// matches expected tokentype emitting the token on success
// and returning passed state function.
func (l *Lexer) LexMatchSkip(tok TokenType, skip int, fn StateFn) StateFn {
	//u.Debugf("lexMatch   t=%s peek=%s", tok, l.PeekWord())
	if l.match(tok.String(), skip) {
		//u.Debugf("found match: %s   %v", tok, fn)
		l.Emit(tok)
		return fn
	}
	u.Error("unexpected token", tok)
	return l.errorToken("Unexpected token:" + l.current())
}

// lexer to match expected value returns with args of
//   @matchState state function if match
//   if no match, return nil
func (l *Lexer) lexIfMatch(tok TokenType, matchState StateFn) StateFn {
	l.SkipWhiteSpaces()
	if l.tryMatch(tok.String()) {
		l.Emit(tok)
		return matchState
	}
	return nil
}

// matches expected tokentype emitting the token on success
// and returning passed state function.
func LexMatchClosure(tok TokenType, nextFn StateFn) StateFn {
	return func(l *Lexer) StateFn {
		//u.Debugf("lexMatch   t=%s peek=%s", tok, l.PeekWord())
		if l.match(tok.String(), 0) {
			//u.Debugf("found match: %s   %v", tok, fn)
			l.Emit(tok)
			return nextFn
		}
		u.Error("unexpected token", tok)
		return l.errorToken("Unexpected token:" + l.current())
	}
}

// State functions ------------------------------------------------------------

// look for the first keyword in this Dialect, looking for the first keyword
// which will indicate the statement such as [SELECT, ALTER, CREATE, INSERT] in sql
func LexDialectForStatement(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	r := l.Peek()

	switch r {
	case '/', '-', '#':
		// ensure we have consumed all comments
		l.Push("LexDialectForStatement", LexDialectForStatement)
		return LexComment(l)
	default:
		peekWord := strings.ToLower(l.PeekWord())
		for _, stmt := range l.dialect.Statements {
			if l.isEnd() {
				break
			}
			//u.Debugf("stmt lexer?  peek=%s  keyword=%v ", peekWord, stmt.Keyword.String())
			if stmt.Keyword.String() == peekWord {
				// We aren't actually going to consume anything here, just find
				// the correct statement
				l.statement = stmt
				return LexStatement
			} else if stmt.Keyword == TokenNil {
				if len(stmt.Clauses) == 1 {
					l.statement = stmt
					l.entryStateFn = stmt.Clauses[0].Lexer
					return stmt.Clauses[0].Lexer
				}
				l.statement = stmt
				return LexStatement
			}

		}
		return l.errorToken("un recognized keyword token:" + peekWord)

	}

	return l.errorToken("could not lex statement" + l.remainder())
}

// LexStatement is the main entrypoint to lex Grammars primarily associated with QL type
// languages, which is keywords seperate clauses, and have order [select .. FROM name WHERE ..]
// the keywords which are reserved serve as identifiers to stop lexing and move to next clause
// lexer
func LexStatement(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	r := l.Peek()

	switch r {
	case '/', '-', '#':
		// ensure we have consumed all comments
		l.Push("LexStatement", LexStatement)
		return LexComment(l)
	default:
		var clause *Clause
		peekWord := strings.ToLower(l.PeekWord())
		for i := l.statementPos; i < len(l.statement.Clauses); i++ {
			if l.isEnd() {
				break
			}
			clause = l.statement.Clauses[i]
			// we only ever consume each clause once
			l.statementPos++
			//u.Debugf("stmt.clause parser?  i?%v pos?%v  peek=%s  keyword=%v multi?%v", i, l.statementPos, peekWord, clause.keyword, clause.multiWord)
			if clause.keyword == peekWord || (clause.multiWord && strings.ToLower(l.peekX(len(clause.keyword))) == clause.keyword) {

				// Set the default entry point for this keyword
				l.entryStateFn = clause.Lexer

				//u.Debugf("dialect clause:  '%v' last?%v \n\t %s ", clause.keyword, len(l.statement.Clauses) == l.statementPos, l.input)
				l.Push("LexStatement", LexStatement)
				if clause.Optional {
					return l.lexIfMatch(clause.Token, clause.Lexer)
				}

				return LexMatchClosure(clause.Token, clause.Lexer)
			}

		}
		// If we have consumed all clauses, we are ready to be done?
		//u.Debugf("not found? word? '%s' done?%v", peekWord, len(l.statement.Clauses) == l.statementPos)
		if l.statementPos == len(l.statement.Clauses) {
			//u.Infof("Run End of statement")
			return LexEndOfStatement
		}

	}

	// Correctly reached EOF.
	if l.pos > l.start {
		// What is this?
		l.Emit(TokenRaw)
	}
	l.Emit(TokenEOF)
	return nil
}

// LexLogical is a lex entry function for logical expression language (+-/> etc)
func LexLogical(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	// r := l.Peek()
	// switch r {
	// case '/', '-', '#':
	// 	// ensure we have consumed all comments
	// 	l.Push("LexLogical", LexLogical)
	// 	return LexComment(l)
	// default:
	//}
	if l.isEnd() {
		l.Emit(TokenEOF)
		return nil
	}

	l.Push("LexLogical", LexLogical)
	//u.Debugf("LexLogical:  %v", l.PeekWord())
	return LexExpression(l)
}

// lex a value:   string, integer, float
//
//  strings must be quoted
//
//  "stuff"    -> stuff
//  'stuff'    ->
//  "items's with quote"
//  1.23
//  100
//
func LexValue(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return l.errorToken("expected value but got EOF")
	}
	rune := l.Next()
	typ := TokenValue
	if rune == ')' {
		// Whoops
		u.Warnf("why did we get paren? ")
		panic("should not have paren")
		return nil
	}
	if rune == '*' {
		u.LogTracef(u.WARN, "why are we having a star here? %v", l.peekX(10))
	}

	//u.Debugf("in LexValue: %v", string(rune))

	// quoted string
	if rune == '\'' || rune == '"' {
		firstRune := rune
		l.ignore() // consume the quote mark
		previousEscaped := rune == '\\'
		for rune = l.Next(); ; rune = l.Next() {

			//u.Debugf("LexValue rune=%v  end?%v  prevEscape?%v", string(rune), rune == eof, previousEscaped)
			if (rune == '\'' || rune == '"') && rune == firstRune && !previousEscaped {
				if !l.isEnd() {
					rune = l.Next()
					// check for '''
					if rune == '\'' || rune == '"' {
						typ = TokenValueWithSingleQuote
					} else {
						// since we read lookahead after single quote that ends the string
						// for lookahead
						l.backup()
						// for single quote which is not part of the value
						l.backup()
						l.Emit(typ)
						// now ignore that single quote
						l.Next()
						l.ignore()
						return nil
					}
				} else {
					// at the very end
					l.backup()
					l.Emit(typ)
					l.Next()
					return nil
				}
			}
			if rune == 0 {
				return l.errorToken("string value was not delimited")
			}
			previousEscaped = rune == '\\'
		}
	} else {
		// Non-Quoted String?   Should this be a numeric?   or date or what?  duration?  what kinds are valid?
		//  A:   numbers
		//
		l.backup()
		//u.Debugf("lexNumber?  %v", string(l.peekX(5)))
		return LexNumber(l)
		// for rune = l.Next(); !isWhiteSpace(rune) && rune != ',' && rune != ')'; rune = l.Next() {
		// }
		// l.backup()
		// l.Emit(typ)
	}
	return nil
}

// lex a regex:   first character must be a /
//
//  /^stats\./i
//  /.*/
//  /^stats.*/
//
func LexRegex(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		//u.Error("wat?")
		return l.errorToken("expected value but got EOF")
	}

	rune := l.Next()
	if rune != '/' {
		//u.Errorf("wat? %v", string(rune))
		return nil
	}

	previousEscaped := rune == '/'
	// scan looking for ending character = /
	for rune = l.Next(); ; rune = l.Next() {
		if rune == eof {
			return l.errorToken("expected value but got EOF")
		}
		//u.Debugf("LexRegex rune=%v  end?%v  prevEscape?%v", string(rune), rune == eof, previousEscaped)
		if rune == '/' && !previousEscaped {
			// now that we have found what appears to be end, lets see if it
			// has a modifier - the i/g at end of    /^stats\./i
			for rune = l.Next(); ; rune = l.Next() {
				if rune == eof {
					return l.errorToken("expected value but got EOF")
				}
				if isWhiteSpace(rune) {
					l.backup()
					l.Emit(TokenRegex)
					return nil
				}
			}
		}

		previousEscaped = rune == '/'
	}

	return nil
}

// look for either an Expression or Identity
//
//  expressions:    Legal identity characters, terminated by (
//  identity:    legal identity characters
//
//  REPLACE(name,"stuff")
//  name
//
func LexExpressionOrIdentity(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	//u.Debugf("LexExpressionOrIdentity identity?%v expr?%v %v peek5='%v'", l.isIdentity(), l.isExpr(), string(l.Peek()), string(l.peekX(5)))
	// Expressions end in Parens:     LOWER(item)
	if l.isExpr() {
		return lexExpressionIdentifier(l)
	} else if l.isIdentity() {
		// Non Expressions are Identities, or Columns
		// u.Warnf("in expr is identity? %s", l.PeekWord())
		// by passing nil here, we are going to go back to Pull items off stack)
		return LexIdentifier(l)
	} else {
		//u.Warnf("LexExpressionOrIdentity ??? -> LexValue")
		return LexValue(l)
	}

	return nil
}

// lex Expression looks for an expression, identified by parenthesis, may be nested
//
//           |--expr----|
//    dostuff(name,"arg")    // the left parenthesis identifies it as Expression
//    eq(trim(name," "),"gmail.com")
func LexExpressionParens(l *Lexer) StateFn {

	// first rune must be opening Parenthesis
	firstChar := l.Next()
	//u.Debugf("LexExpressionParens:  %v", string(firstChar))
	if firstChar != '(' {
		u.Errorf("bad expression? %v", string(firstChar))
		return l.errorToken("expression must begin with a paren: ( " + l.current())
	}
	l.Emit(TokenLeftParenthesis)
	//u.Infof("LexExpressionParens:   %v", string(firstChar))
	return LexListOfArgs
}

// lex expression identity keyword, does not consume parenthesis
//
//    |--expridentity---|
//    name_of_expression(name,"arg")
func lexExpressionIdentifier(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	//u.Debugf("lexExpressionIdentifier identity?%v expr?%v %v:%v", l.isIdentity(), l.isExpr(), string(l.Peek()), string(l.PeekWord()))

	// first rune has to be valid unicode letter
	firstChar := l.Next()
	if firstChar == '!' {
		l.Emit(TokenNegate)
		return lexExpressionIdentifier
	}
	if !unicode.IsLetter(firstChar) {
		//u.Warnf("lexExpressionIdentifier couldnt find expression idenity?  %v stack=%v", string(firstChar), len(l.stack))
		return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
	}
	// Now look for run of runes, where run is ended by first non-identifier character
	for rune := l.Next(); isIdentifierRune(rune); rune = l.Next() {
		// iterate until we find non-identifer character
	}
	// TODO:  validate identity vs next keyword?, ie ensure it is not a keyword/reserved word

	l.backup() // back up one character
	l.Emit(TokenUdfExpr)
	return LexExpressionParens
}

//  list of arguments, comma seperated list of args which may be a mixture
//   of expressions, identities, values
//
//       REPLACE(LOWER(x),"xyz")
//       REPLACE(x,"xyz")
//       COUNT(*)
//       sum( 4 * toint(age))
//       IN (a,b,c)
//       varchar(10)
//
func LexListOfArgs(l *Lexer) StateFn {

	// as we descend into Expressions, we are going to use push/pop to
	//  ascend/descend
	l.SkipWhiteSpaces()

	r := l.Next()
	//u.Debugf("in LexListOfArgs:  '%s'", string(r))

	switch r {
	case ')':
		l.Emit(TokenRightParenthesis)
		return nil // Send signal to pop
	case '(':
		l.Emit(TokenLeftParenthesis)
		return LexListOfArgs
	case ',':
		l.Emit(TokenComma)
		return LexListOfArgs
	case '*':
		if &l.lastToken != nil && l.lastToken.T == TokenLeftParenthesis {
			l.Emit(TokenStar)
			return nil
		} else {
			//l.Emit(TokenMultiply)
			//return LexListOfArgs
			l.backup()
			return nil
		}
	case '!', '=', '>', '<', '-', '+', '%', '&', '/', '|':
		l.backup()
		return nil
	case ';':
		l.backup()
		return nil
	default:
		// So, not comma, * so either is Expression, Identity, Value
		l.backup()
		peekWord := strings.ToLower(l.PeekWord())
		//u.Debugf("in LexListOfArgs:  '%s'", peekWord)
		// First, lets ensure we haven't blown past into keyword?
		if l.isNextKeyword(peekWord) {
			//u.Warnf("found keyword while looking for arg? %v", string(r))
			return nil
		}

		//u.Debugf("LexListOfArgs sending LexExpressionOrIdentity: %v", string(peekWord))
		l.Push("LexListOfArgs", LexListOfArgs)
		return LexExpressionOrIdentity
	}

	//u.Warnf("exit LexListOfArgs")
	return nil
}

// LexIdentifier scans and finds named things (tables, columns)
//  and specifies them as TokenIdentity, uses LexIdentifierType
//
//  TODO: dialect controls escaping/quoting techniques
//
//  [name]         select [first name] from usertable;
//  'name'         select 'user' from usertable;
//  first_name     select first_name from usertable;
//  usertable      select first_name AS fname from usertable;
//  _name          select _name AS name from stuff;
//
var LexIdentifier = LexIdentifierOfType(TokenIdentity)

// LexIdentifierOfType scans and finds named things (tables, columns)
//  supports quoted, bracket, or raw identifiers
//
//  TODO: dialect controls escaping/quoting techniques
//
//  [name]         select [first name] from usertable;
//  'name'         select 'user' from usertable;
//  `user`         select first_name from `user`;
//  first_name     select first_name from usertable;
//  usertable      select first_name AS fname from usertable;
//  _name          select _name AS name from stuff;
//
func LexIdentifierOfType(forToken TokenType) StateFn {

	return func(l *Lexer) StateFn {
		l.SkipWhiteSpaces()

		wasQouted := false
		// first rune has to be valid unicode letter
		firstChar := l.Next()
		//u.Debugf("LexIdentifierOfType:   %s :  %v", string(firstChar), l.peekX(6))
		//u.LogTracef(u.INFO, "LexIdentifierOfType: %v", string(firstChar))
		switch {
		case isIdentityQuoteMark(firstChar):
			// Fields can be bracket or single quote escaped
			//  [user]
			//  [email]
			//  'email'
			//  `user`
			//u.Debugf("in quoted identity")
			l.ignore()
			nextChar := l.Next()
			if !unicode.IsLetter(nextChar) {
				u.Warnf("aborting LexIdentifierOfType: %v", string(nextChar))
				return l.errorToken("identifier must begin with a letter " + l.input[l.start:l.pos])
			}
			// Since we escaped this with a quote we allow laxIdentifier characters
			for nextChar = l.Next(); isLaxIdentifierRune(nextChar); nextChar = l.Next() {

			}
			// iterate until we find non-identifier, then make sure it is valid/end
			if firstChar == '[' && nextChar == ']' {
				// valid
			} else if firstChar == nextChar && isIdentityQuoteMark(nextChar) {
				// also valid
			} else {
				u.Errorf("unexpected character in identifier?  %v", string(nextChar))
				return l.errorToken("unexpected character in identifier:  " + string(nextChar))
			}
			wasQouted = true
			l.backup()
			//u.Debugf("quoted?:   %v  ", l.input[l.start:l.pos])
		default:
			if !isIdentifierFirstRune(firstChar) {
				//u.Warnf("aborting LexIdentifier: '%v'", string(firstChar))
				return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
			}
			for rune := l.Next(); isIdentifierRune(rune); rune = l.Next() {
				// iterate until we find non-identifer character
			}
			l.backup()
		}

		//u.Debugf("about to emit: %v", forToken)
		l.Emit(forToken)
		if wasQouted {
			// need to skip last character bc it was quoted
			l.Next()
			l.ignore()
		}

		//u.Debugf("about to return:  %v", nextFn)
		return nil // pop up to parent
	}
}

// Look for end of statement defined by either a semicolon or end of file
func LexEndOfStatement(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	r := l.Next()
	//u.Debugf("sqlend of statement  '%s' r=%d", string(r), r)
	if r == ';' {
		l.Emit(TokenEOS)
	}
	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	u.Warnf("error looking for end of statement: '%v'", l.remainder())
	return l.errorToken("Unexpected token:" + l.current())
}

// Handle start of select statements, specifically looking for
//    @@variables, *, or else we drop into <select_list>
//
//     <SELECT> :==
//         (DISTINCT|ALL)? ( <sql_variable> | * | <select_list> ) [FROM <source_clause>]
//
//     <sql_variable> = @@stuff
//
func LexSelectClause(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	first := strings.ToLower(l.peekX(2))

	//u.Debugf("LexSelectStart  '%v'", first)

	switch first {
	case "al": //ALL?
		word := strings.ToLower(l.peekX(3))
		if word == "all" {
			l.ConsumeWord(word)
			l.Emit(TokenAll)
		}
	case "di": //Distinct?
		word := strings.ToLower(l.peekX(len("DISTINCT")))
		if word == "distinct" {
			l.ConsumeWord(word)
			l.Emit(TokenDistinct)
		} // DISTINCTROW?
	case "* ":
		// Look for keyword, ie something like FROM, or possibly end of statement
		l.Next()           // consume the *
		pw := l.PeekWord() // this will skip whitespace
		//u.Debugf("* ?'%v'  keyword='%v'", first, pw)
		if l.isNextKeyword(pw) {
			//   select * from
			l.Emit(TokenStar)
			return nil
		}
		l.backup()
		u.Errorf("What is this? %v", l.peekX(10))
	case "@@": //  mysql system variables start with @@
		// Should we handle these here?
		u.Warnf("Found Sql Variable but not handling: %v", l.peekX(15))
	}

	// Since we did Not find anything it, start lexing normal SelectList
	// and set that as the entry function
	l.entryStateFn = LexSelectList
	return LexSelectList
}

// Handle recursive subqueries
//
func LexSubQuery(l *Lexer) StateFn {

	//u.Debugf("LexSubQuery  '%v'", l.peekX(10))
	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}

	/*
		TODO:   this is a hack because the LexDialect from above should be recursive,
		 	ie support sub-queries, but doesn't currently
	*/
	word := strings.ToLower(l.PeekWord())
	switch word {
	case "select":
		l.ConsumeWord(word)
		l.Emit(TokenSelect)
		return LexSubQuery
	case "where":
		l.ConsumeWord(word)
		l.Emit(TokenWhere)
		return LexConditionalClause
	case "from":
		l.ConsumeWord(word)
		l.Emit(TokenFrom)
		l.Push("LexSubQuery", LexSubQuery)
		l.Push("LexConditionalClause", LexConditionalClause)
		return LexTableReferences
	default:
	}

	l.Push("LexSubQuery", LexSubQuery)
	return LexSelectClause
}

// Handle prepared statements
//
func LexPreparedStatement(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	//u.Debugf("LexPreparedStatement  '%v'", l.peekX(10))

	/*
		TODO:   this is a bit different from others, as after we get FROM
		 we are going to create a new lexer?  and forward over?  or reset?
	*/
	word := strings.ToLower(l.PeekWord())
	switch word {
	case "from":
		l.ConsumeWord(word)
		l.Emit(TokenFrom)
		l.Push("LexPreparedStatement", LexPreparedStatement)
		return LexValue
	default:
		r := l.Peek()
		if r == ';' {
			l.Next()
			l.Emit(TokenEOS)
			return nil
		}
	}

	l.Push("LexPreparedStatement", LexPreparedStatement)
	return LexSelectClause
}

// Handle repeating Select List for columns
//
//     SELECT ( * | <select_list> )
//
//     <select_list> := <select_col> [, <select_col>]*
//
func LexSelectList(l *Lexer) StateFn {
	//u.Debugf("LexSelectList  '%v'", l.peekX(10))
	return LexColumns
}

// Handle Table References ie From table, and SubSelects, Joins
//
//    SELECT ...  [FROM <table_references>]
//
//    <table_references> :== ( <from_clause> | '(' <subselect>')' [AS <identifier>] | <join_reference> )
//    <from_clause> ::= FROM <source_clause>
//    <source_clause> :== <identifier> [AS <identifier>]
//    <join_reference> :== (INNER | LEFT | OUTER)? JOIN [ON <conditional_clause>] <source_clause> )
//    <subselect> :==
//             FROM '(' <select_stmt> ')'
//
func LexTableReferences(l *Lexer) StateFn {

	// From has already been consumed

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	r := l.Peek()

	//u.Debugf("LexTableReferences  peek2= '%v'", l.peekX(2))

	// Cover the grouping, ie recursive/repeating nature of subqueries
	switch r {
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		// subquery?
		l.Push("LexTableReferences", LexTableReferences)
		l.entryStateFn = LexSelectClause
		return LexSelectClause
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		// end of subquery?
		//l.Push("LexTableReferences", LexTableReferences)
		l.entryStateFn = nil
		return LexSelectClause
		// case ',':
		// 	l.Next()
		// 	l.Emit(TokenComma)
		// 	return l.entryStateFn
	}

	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexTableReferences looking for operator:  word=%s", word)
	switch word {
	case "from", "select", "where":
		//u.Warnf("emit from")
		// l.ConsumeWord("FROM")
		// l.Emit(TokenFrom)
		// l.Push("LexTableReferences", LexTableReferences)
		// l.Push("LexIdentifier", LexIdentifier)
		return nil
	case "as":
		u.Debug("emit as")
		l.ConsumeWord("AS")
		l.Emit(TokenAs)
		l.Push("LexTableReferences", LexTableReferences)
		l.Push("LexIdentifier", LexIdentifier)
		return nil
	case "inner":
		word = strings.ToLower(l.peekX(len("inner join")))
		if word == "inner join" {
			l.ConsumeWord("INNER JOIN")
			l.Emit(TokenInnerJoin)
			l.Push("LexTableReferences", LexTableReferences)
			//l.Push("LexExpression", LexExpression)
			return nil
		}
	case "outer":
		word = strings.ToLower(l.peekX(len("outer join")))
		if word == "outer join" {
			l.ConsumeWord("OUTER JOIN")
			l.Emit(TokenOuterJoin)
			l.Push("LexTableReferences", LexTableReferences)
			//l.Push("LexExpression", LexExpression)
			return nil
		}
	case "left":
		word = strings.ToLower(l.peekX(len("left join")))
		if word == "left join" {
			l.ConsumeWord("LEFT JOIN")
			l.Emit(TokenLeftJoin)
			l.Push("LexTableReferences", LexTableReferences)
			//l.Push("LexExpression", LexExpression)
			return nil
		}
	case "join":
		l.ConsumeWord("JOIN")
		l.Emit(TokenJoin)
		l.Push("LexTableReferences", LexTableReferences)
		//l.Push("LexExpression", LexExpression)
		return nil
	case "on": //
		l.ConsumeWord(word)
		l.Emit(TokenOn)
		l.Push("LexTableReferences", LexTableReferences)
		return LexConditionalClause
	case "in": // what is complete list here?
		l.ConsumeWord(word)
		l.Emit(TokenIN)
		l.Push("LexTableReferences", LexTableReferences)
		l.Push("LexListOfArgs", LexListOfArgs)
		return nil

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexTableReferences", LexTableReferences)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			//u.Warnf("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexTableReferences = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexTableReferences", LexTableReferences)
	} else {
		u.Errorf("Gracefully refusing to add more LexTableReferences: ")
	}

	// Since we did Not find anything, we are going to go for a Expression or Identity
	return LexExpressionOrIdentity
}

// Handle logical Conditional Clause used for [WHERE, WITH, JOIN ON]
// logicaly grouped with parens and/or seperated by commas or logic (AND/OR/NOT)
//
//     SELECT ... WHERE <conditional_clause>
//
//  <conditional_clause> ::= <expr> [( AND <expr> | OR <expr> | '(' <expr> ')' )]
//  <expr> ::= <predicatekw> '('? <expr> [, <expr>] ')'? | <func> | <subselect>
//  <func> ::= <identity>'(' <expr> ')'
//  <predicatekw> ::= (IN | CONTAINS | RANGE | LIKE | EQUALS )
func LexConditionalClause(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	r := l.Next()

	//u.Debugf("LexConditionalClause  r= '%v'", string(r))

	// characters that indicate start of an identity, we can short circuit the
	// rest because we know its an identity (or error)
	if isIdentityQuoteMark(r) {
		l.backup()
		l.Push("LexConditionalClause", LexConditionalClause)
		return LexExpressionOrIdentity
	}

	// Cover the logic and grouping
	switch r {
	case '!', '=', '>', '<', '(', ')', ',', ';', '-', '*', '+', '%', '/':
		foundLogical := false
		foundOperator := false
		switch r {
		case '-': // comment?  or minus?
			p := l.Peek()
			if p == '-' {
				l.backup()
				l.Push("LexConditionalClause", LexConditionalClause)
				return LexInlineComment
			} else {
				l.Emit(TokenMinus)
				return LexConditionalClause
			}
		case ';':
			l.backup()
			return nil
		case '(': // this is a logical Grouping/Ordering
			//l.Push("LexParenEnd", LexParenEnd)
			l.Emit(TokenLeftParenthesis)
			return LexConditionalClause
		case ')': // this is a logical Grouping/Ordering
			l.Emit(TokenRightParenthesis)
			return LexConditionalClause
		case ',':
			l.Emit(TokenComma)
			return LexConditionalClause
		case '*':
			l.Emit(TokenMultiply)
			// WHERE x = 5 * 5
			return LexConditionalClause
		case '!': //  !=
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenNE)
				foundLogical = true
			} else {
				//u.Error("Found ! without equal")
				l.Emit(TokenNegate)
				return LexConditionalClause
			}
		case '=':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenEqualEqual)
				foundOperator = true
			} else {
				l.Emit(TokenEqual)
				foundOperator = true
			}
		case '>':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenGE)
			} else {
				l.Emit(TokenGT)
			}
			foundLogical = true
		case '<':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenLE)
				foundLogical = true
			} else if r2 == '>' { //   <>
				l.Next()
				l.Emit(TokenNE)
				foundOperator = true
			}
		case '+':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenPlusEquals)
				foundOperator = true
			} else if r2 == '+' {
				l.Next()
				l.Emit(TokenPlusPlus)
				foundOperator = true
			} else {
				l.Emit(TokenPlus)
				foundLogical = true
			}
		case '%':
			l.Emit(TokenModulus)
			foundOperator = true
		case '/':
			l.Emit(TokenDivide)
			foundOperator = true
		}
		if foundLogical == true {
			//u.Debugf("found LexConditionalClause = '%v'", string(r))
			// There may be more than one item here
			l.Push("LexConditionalClause", LexConditionalClause)
			return LexExpressionOrIdentity
		} else if foundOperator {
			//u.Debugf("found LexConditionalClause = '%v'", string(r))
			// There may be more than one item here
			l.Push("LexConditionalClause", LexConditionalClause)
			return LexExpressionOrIdentity
		}
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexConditionalClause looking for word=%s", word)
	switch word {
	case "in", "like": // what is complete list here?
		switch word {
		case "in": // IN
			l.skipX(2)
			l.Emit(TokenIN)
			l.Push("LexConditionalClause", LexConditionalClause)
			l.Push("LexListOfArgs", LexListOfArgs)
			return nil
		case "like": // like
			l.skipX(4)
			l.Emit(TokenLike)
			//u.Debugf("like?  %v", l.peekX(10))
			l.Push("LexConditionalClause", LexConditionalClause)
			l.Push("LexExpressionOrIdentity", LexExpressionOrIdentity)
			return nil
		}
	case "and", "or":
		// this marks beginning of new related column
		switch word {
		case "and":
			l.ConsumeWord(word)
			l.Emit(TokenLogicAnd)
		case "or":
			l.ConsumeWord(word)
			l.Emit(TokenLogicOr)
			// case "not":
			// 	l.skipX(3)
			// 	l.Emit(TokenLogicAnd)
		}
		//l.Push("LexConditionalClause", LexConditionalClause)
		return LexConditionalClause
	case "select", "where", "from":
		return LexSubQuery
	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexConditionalClause", l.entryStateFn)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			//u.Warnf("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexConditionalClause = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexConditionalClause", LexConditionalClause)
	} else {
		u.Errorf("Gracefully refusing to add more LexConditionalClause: ")
	}

	// Since we did Not find anything, we are going to go for a Expression or Identity
	return LexExpressionOrIdentity
}

// Handle logical columns/expressions which may be nested
// Expression or Column, most noteable used for [SELECT, GROUP BY, WHERE, WITH]
// logicaly grouped with parens and/or seperated by commas or logic (AND/OR/NOT)
//
//     SELECT [    ,[ ]] FROM
//     GROUP BY x, [y]
//
//  a column can have a AS statement
//       REPLACE(LOWER(x),"xyz")
//       email_address AS email
//
//  and multiple columns separated by commas, or logic statements
//      LOWER(cola), UPPER(colb)
//      key = value, key2 = value
//      key = value AND key2 = value
//
// Examples:
//
//   *
//  (colx = y OR colb = b)
//  cola = 'a5'
//  cola != "a5", colb = "a6"
//  REPLACE(cola,"stuff") != "hello"
//  FirstName = REPLACE(LOWER(name," "))
//  cola IN (1,2,3)
//  cola LIKE "abc"
//  eq(name,"bob") AND age > 5
//  time > now() -1h
//
func LexColumns(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	r := l.Next()

	//u.Debugf("LexColumn  r= '%v'", string(r))

	// characters that indicate start of an identity, we can short circuit the
	// rest because we know its an identity (or error)
	if isIdentityQuoteMark(r) {
		l.backup()
		l.Push("LexColumns", LexColumns)
		return LexExpressionOrIdentity
	}

	// Cover the logic and grouping
	switch r {
	case '!', '=', '>', '<', '(', ')', ',', ';', '-', '*', '+', '%', '/':
		foundLogical := false
		foundOperator := false
		switch r {
		case '-': // comment?  or minus?
			p := l.Peek()
			if p == '-' {
				l.backup()
				l.Push("LexColumns", LexColumns)
				return LexInlineComment
			} else {
				l.Emit(TokenMinus)
				return LexColumns
			}
		case ';':
			l.backup()
			return nil
		case '(': // this is a logical Grouping/Ordering
			//l.Push("LexParenEnd", LexParenEnd)
			l.Emit(TokenLeftParenthesis)
			return LexColumns
		case ')': // this is a logical Grouping/Ordering
			l.Emit(TokenRightParenthesis)
			return LexColumns
		case ',':
			l.Emit(TokenComma)
			return LexColumns
		case '*':
			//pw := l.PeekWord()
			//u.Debugf("pw?'%v'    r=%v", pw, string(r))
			// if l.isNextKeyword(pw) {
			// 	//   select * from
			// 	//u.Infof("EmitStar?")
			// 	l.Emit(TokenStar)
			// 	return nil
			// } else {
			//u.Warnf("is not keyword? %v", pw)
			l.Emit(TokenMultiply)
			//foundOperator = true
			//}
			// WHERE x = 5 * 5
			return LexColumns
		case '!': //  !=
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenNE)
				foundLogical = true
			} else {
				//u.Error("Found ! without equal")
				l.Emit(TokenNegate)
				foundLogical = true
				return LexColumns
			}
		case '=':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenEqualEqual)
				foundOperator = true
			} else {
				l.Emit(TokenEqual)
				foundOperator = true
			}
		case '>':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenGE)
			} else {
				l.Emit(TokenGT)
			}
			foundLogical = true
		case '<':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenLE)
				foundLogical = true
			} else if r2 == '>' { //   <>
				l.Next()
				l.Emit(TokenNE)
				foundOperator = true
			}
		case '+':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenPlusEquals)
				foundOperator = true
			} else if r2 == '+' {
				l.Next()
				l.Emit(TokenPlusPlus)
				foundOperator = true
			} else {
				l.Emit(TokenPlus)
				foundLogical = true
			}
		case '%':
			l.Emit(TokenModulus)
			foundOperator = true
		case '/':
			l.Emit(TokenDivide)
			foundOperator = true
		}
		if foundLogical == true {
			//u.Debugf("found LexColumns = '%v'", string(r))
			// There may be more than one item here
			l.Push("LexColumns", LexColumns)
			return LexExpressionOrIdentity
		} else if foundOperator {
			//u.Debugf("found LexColumns = '%v'", string(r))
			// There may be more than one item here
			l.Push("LexColumns", LexColumns)
			return LexExpressionOrIdentity
		}
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexColumn looking for operator:  word=%s", word)
	switch word {
	case "values":
		l.ConsumeWord("values")
		l.Emit(TokenValues)
		return LexColumns
	case "as":
		l.skipX(2)
		l.Emit(TokenAs)
		l.Push("LexColumns", LexColumns)
		l.Push("LexIdentifier", LexIdentifier)
		return nil
	case "if":
		l.skipX(2)
		l.Emit(TokenIf)
		l.Push("LexColumns", LexColumns)
		//l.Push("LexExpression", LexExpression)
		return nil
	case "in", "like": // what is complete list here?
		switch word {
		case "in": // IN
			l.skipX(2)
			l.Emit(TokenIN)
			l.Push("LexColumns", LexColumns)
			l.Push("LexListOfArgs", LexListOfArgs)
			return nil
		case "like": // like
			l.skipX(4)
			l.Emit(TokenLike)
			//u.Debugf("like?  %v", l.peekX(10))
			l.Push("LexColumns", LexColumns)
			l.Push("LexExpressionOrIdentity", LexExpressionOrIdentity)
			return nil
		}
	case "and", "or":
		// this marks beginning of new related column
		switch word {
		case "and":
			l.skipX(3)
			l.Emit(TokenLogicAnd)
		case "or":
			l.skipX(2)
			l.Emit(TokenLogicOr)
			// case "not":
			// 	l.skipX(3)
			// 	l.Emit(TokenLogicAnd)
		}
		//l.Push("LexColumns", LexColumns)
		return LexColumns

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexColumns", LexColumns)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			//u.Warnf("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexColumns = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexColumns", LexColumns)
	} else {
		u.Errorf("Gracefully refusing to add more LexColumns: ")
	}

	// Since we did Not find anything, we are going to go for a Expression or Identity
	return LexExpressionOrIdentity
}

// Handle single logical expression which may be nested and  has
//  udf names that are not validated by lexer
//
// Examples:
//
//  (colx = y OR colb = b)
//  cola = 'a5'
//  cola != "a5", colb = "a6"
//  REPLACE(cola,"stuff") != "hello"
//  FirstName = REPLACE(LOWER(name," "))
//  cola IN (1,2,3)
//  cola LIKE "abc"
//  eq(name,"bob") AND age > 5
//  time > now() -1h
//  (4 + 5) > 10
//
func LexExpression(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}
	r := l.Next()

	//u.Debugf("LexExpression  r= '%v'", string(r))

	// Cover the logic and grouping
	switch r {
	case '!', '=', '>', '<', '(', ')', ',', ';', '-', '*', '+', '%', '&', '/', '|':
		foundLogical := false
		foundOperator := false
		switch r {
		case '-': // comment?  or minus?
			p := l.Peek()
			if p == '-' {
				l.backup()
				l.Push("LexExpression", l.entryStateFn)
				return LexInlineComment
			} else {
				l.Emit(TokenMinus)
				return l.entryStateFn
			}
		case ';':
			l.backup()
			return nil
		case '(': // this is a logical Grouping/Ordering
			//l.Push("LexParenEnd", LexParenEnd)
			l.Emit(TokenLeftParenthesis)
			return l.entryStateFn
		case ')': // this is a logical Grouping/Ordering
			l.Emit(TokenRightParenthesis)
			return l.entryStateFn
		case ',':
			l.Emit(TokenComma)
			return l.entryStateFn
		case '!': //  !=
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenNE)
				foundLogical = true
			} else {
				l.Emit(TokenNegate)
				//u.Debugf("Found ! Negate")
				return nil
			}
		case '=':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenEqualEqual)
				//u.Infof("found ==  peek5='%v'", string(l.peekX(5)))
				foundOperator = true
			} else {
				l.Emit(TokenEqual)
				foundOperator = true
			}
		case '|':
			if r2 := l.Peek(); r2 == '|' {
				l.Next()
				l.Emit(TokenOr)
				foundOperator = true
			}
		case '&':
			if r2 := l.Peek(); r2 == '&' {
				l.Next()
				l.Emit(TokenAnd)
				foundOperator = true
			}
		case '>':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenGE)
			} else {
				l.Emit(TokenGT)
			}
			foundLogical = true
		case '<':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenLE)
				foundLogical = true
			} else if r2 == '>' { //   <>
				l.Next()
				l.Emit(TokenNE)
				foundOperator = true
			}
		case '*':
			l.Emit(TokenMultiply)
			// x = 5 * 5
			foundOperator = true
		case '+':
			if r2 := l.Peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenPlusEquals)
				foundOperator = true
			} else if r2 == '+' {
				l.Next()
				l.Emit(TokenPlusPlus)
				foundOperator = true
			} else {
				l.Emit(TokenPlus)
				foundLogical = true
			}
		case '%':
			l.Emit(TokenModulus)
			foundOperator = true
		case '/':
			l.Emit(TokenDivide)
			foundOperator = true
		}
		if foundLogical == true {
			//u.Debugf("found LexExpression = '%v'", string(r))
			// There may be more than one item here
			//l.Push("l.entryStateFn", l.entryStateFn)
			return LexExpression
		} else if foundOperator {
			//u.Debugf("found LexExpression = peek5='%v'", string(l.peekX(5)))
			// There may be more than one item here
			//l.Push("l.entryStateFn", l.entryStateFn)
			return LexExpression
		}
	}

	l.backup()
	op := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for operator:  word=%s", op)
	switch op {
	case "in", "like": // what is complete list here?
		switch op {
		case "in": // IN
			l.skipX(2)
			l.Emit(TokenIN)
			l.Push("LexExpression", l.entryStateFn)
			l.Push("LexListOfArgs", LexListOfArgs)
			return nil
		case "like": // like
			l.skipX(4)
			l.Emit(TokenLike)
			u.Debugf("like?  %v", l.peekX(10))
			l.Push("LexExpression", l.entryStateFn)
			l.Push("LexExpressionOrIdentity", LexExpressionOrIdentity)
			return nil
		}
	case "and", "or":
		// this marks beginning of new related column
		switch op {
		case "and":
			l.skipX(3)
			l.Emit(TokenLogicAnd)
		case "or":
			l.skipX(2)
			l.Emit(TokenLogicOr)
			// case "not":
			// 	l.skipX(3)
			// 	l.Emit(TokenLogicAnd)
		}
		l.Push("LexExpression", l.entryStateFn)
		return LexExpressionOrIdentity

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexExpression", l.entryStateFn)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(op) {
			u.Debugf("found keyword? %v ", op)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexExpression = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexExpression", l.entryStateFn)
	} else {
		u.Errorf("Gracefully refusing to add more LexExpression: ")
	}
	return LexExpressionOrIdentity
}

// Handle columnar identies with keyword appendate (ASC, DESC)
//
//     [ORDER BY] abc, def ASC
//
func LexOrderByColumn(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.isEnd() {
		return nil
	}

	r := l.Peek()
	//u.Debugf("LexOrderBy  r= '%v'", string(r))

	switch r {
	case ';':
		return nil
	case ',':
		l.Next()
		l.Emit(TokenComma)
		l.Push("LexOrderByColumn", LexOrderByColumn)
		return LexExpressionOrIdentity
	}

	op := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for operator:  word=%s", op)
	switch op {
	case "asc":
		l.ConsumeWord("asc")
		l.Emit(TokenAsc)
		return nil
	case "desc":
		l.ConsumeWord("desc")
		l.Emit(TokenDesc)
		return nil
	default:
		if len(l.stack) < 2 {
			l.Push("LexOrderByColumn", LexOrderByColumn)
			return LexExpressionOrIdentity
		} else {
			u.Errorf("Gracefully refusing to add more LexOrderByColumn: ")
		}
	}

	// Since we did Not find anything, we are in error?
	return nil
}

// data definition language column
//
//   CHANGE col1_old col1_new varchar(10),
//   CHANGE col2_old col2_new TEXT
//   ADD col3 BIGINT AFTER col1_new
//   ADD col2 TEXT FIRST,
//
func LexDdlColumn(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	r := l.Next()

	//u.Debugf("LexDdlColumn  r= '%v'", string(r))

	// Cover the logic and grouping
	switch r {
	case '-', '/': // comment?
		p := l.Peek()
		if p == '-' {
			l.backup()
			l.Push("entryStateFn", l.entryStateFn)
			return LexInlineComment
			//return nil
		}
	case ';':
		l.backup()
		return nil
	case ',':
		l.Emit(TokenComma)
		return l.entryStateFn
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for operator:  word=%s", word)
	switch word {
	case "change":
		l.ConsumeWord(word)
		l.Emit(TokenChange)
		return LexDdlColumn
	case "add":
		l.ConsumeWord(word)
		l.Emit(TokenAdd)
		return LexDdlColumn
	case "after":
		l.ConsumeWord(word)
		l.Emit(TokenAfter)
		return LexDdlColumn
	case "first":
		l.ConsumeWord(word)
		l.Emit(TokenFirst)
		return LexDdlColumn

	// Character set is end of ddl column
	case "character": // character set
		cs := strings.ToLower(l.peekX(len("character set")))
		if cs == "character set" {
			l.ConsumeWord(cs)
			l.Emit(TokenCharacterSet)
			l.Push("LexDdlColumn", l.entryStateFn)
			return nil
		}

	// Below here are Data Types
	case "text":
		l.ConsumeWord(word)
		l.Emit(TokenText)
		return l.entryStateFn
	case "bigint":
		l.ConsumeWord(word)
		l.Emit(TokenBigInt)
		return l.entryStateFn
	case "varchar":
		l.ConsumeWord(word)
		l.Emit(TokenVarChar)
		l.Push("LexDdlColumn", l.entryStateFn)
		return LexListOfArgs

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexDdlColumn", l.entryStateFn)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			u.Infof("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Infof("LexDdlColumn = '%v'", string(r))

	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexDdlColumn", l.entryStateFn)
	} else {
		u.Errorf("Gracefully refusing to add more LexDdlColumn: ")
	}
	return LexExpressionOrIdentity
}

// LexComment looks for valid comments which are any of the following
//   including the in-line comment blocks
//
//  /* hello */
//  //  hello
//  -- hello
//  # hello
//  SELECT name --name is the combined first-last name
//         , age FROM `USER` ...
//
func LexComment(l *Lexer) StateFn {
	//u.Debugf("checking comment: '%s' ", l.input[l.pos:l.pos+2])
	// TODO:  switch statement instead of strings has prefix
	if strings.HasPrefix(l.input[l.pos:], "/*") {
		return LexMultilineComment(l)
	} else if strings.HasPrefix(l.input[l.pos:], "//") {
		//u.Debugf("found single line comment:  // ")
		return LexInlineComment(l)
	} else if strings.HasPrefix(l.input[l.pos:], "--") {
		//u.Debugf("found single line comment:  -- ")
		return LexInlineComment(l)
	} else if strings.HasPrefix(l.input[l.pos:], "#") {
		//u.Debugf("found single line comment:  # ")
		return LexInlineComment(l)
	}
	return nil
}

// A multi-line comment of format /* comment */
// it does not have to actually be multi-line, just surrounded by those comments
func LexMultilineComment(l *Lexer) StateFn {
	// Consume opening "/*"
	l.ignoreWord("/*")
	for {
		if strings.HasPrefix(l.input[l.pos:], "*/") {
			break
		}
		r := l.Next()
		if eof == r {
			return l.errorf("unexpected eof in comment: %q", l.input)
		}
	}
	l.Emit(TokenCommentML)
	// Consume trailing "*/"
	l.ignoreWord("*/")
	return nil
}

// Comment begining with //, # or --
func LexInlineComment(l *Lexer) StateFn {

	// We are going to Find the start of the Comments
	p2 := l.peekX(2)
	r := l.Next()

	// Should we be emitting the --, #, // ?  is that meaningful?
	if r == '-' && p2 == "--" {
		l.Next()
		l.Emit(TokenCommentSingleLine)
	} else if r == '/' && p2 == "//" {
		l.Next()
		l.Emit(TokenCommentSlashes)
	} else if r == '#' {
		l.Emit(TokenCommentHash)
	}

	return lexSingleLineComment
}

// Comment begining with //, # or -- but do not emit the tag just text comment
func LexInlineCommentNoTag(l *Lexer) StateFn {

	// We are going to Find the start of the Comments
	p2 := l.peekX(2)
	r := l.Next()

	// Should we be emitting the --, #, // ?  is that meaningful?
	if r == '-' && p2 == "--" {
		l.Next()
	} else if r == '/' && p2 == "//" {
		l.Next()
	} else if r == '#' {
		// we have consumed it
	}
	// Consume the word
	l.start = l.pos

	// Should we actually be consuming Whitespace? or is it meaningful?
	// l.SkipWhiteSpaces()

	// for {
	// 	r = l.Next()
	// 	if r == '\n' || r == eof {
	// 		l.backup()
	// 		break
	// 	}
	// }
	// l.Emit(TokenComment)
	return nil
}

// the text/contents of a single line comment
func lexSingleLineComment(l *Lexer) StateFn {
	// Should we consume whitespace?
	//l.SkipWhiteSpaces()
	for {
		r := l.Next()
		if r == '\n' || r == eof {
			l.backup()
			break
		}
	}
	l.Emit(TokenComment)
	return nil
}

// looks for table name, then optional SET, then columns
//
func LexTableNameColumns(l *Lexer) StateFn {
	//u.Infof("LexTableNameColumns: %v", l.peekX(10))
	// Lets update the re-entrant keyword entry point after consuming table name
	l.entryStateFn = LexColumns
	l.Push("LexTableColumns", LexTableColumns)
	return LexIdentifierOfType(TokenTable)
}

func LexTableColumns(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	r := l.Peek()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for tablecolumns:  word=%s r=%s", word, string(r))
	switch r {
	case 's', 'S':
		if word == "set" {
			l.ConsumeWord("set")
			l.Emit(TokenSet)
			return LexColumns
		}
	case '(':
		return LexColumns
	}
	return l.errorf("unrecognized keyword: %q", word)
}

// LexNumber floats, integers, hex, exponential, signed
//
//  1.23
//  100
//  -827
//  6.02e23
//  0X1A2B,  0x1a2b, 0x1A2B.2B
//
// Floats must be in decimal and must either:
//
//     - Have digits both before and after the decimal point (both can be
//       a single 0), e.g. 0.5, -100.0, or
//     - Have a lower-case e that represents scientific notation,
//       e.g. -3e-3, 6.02e23.
//
// Integers can be:
//
//     - decimal (e.g. -827)
//     - hexadecimal (must begin with 0x and must use capital A-F, e.g. 0x1A2B)
//
func LexNumber(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	typ, ok := scanNumericOrDuration(l, SUPPORT_DURATION)
	//u.Debugf("typ  %v   %v", typ, ok)
	if !ok {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	// Emits tokenFloat or tokenInteger.
	l.Emit(typ)
	return nil
}

// LexNumberOrDuration floats, integers, hex, exponential, signed
//
//  1.23
//  100
//  -827
//  6.02e23
//  0X1A2B,  0x1a2b, 0x1A2B.2B
//
// durations:   45m, 2w, 20y, 22d, 40ms, 100ms, -100ms
//
// Floats must be in decimal and must either:
//
//     - Have digits both before and after the decimal point (both can be
//       a single 0), e.g. 0.5, -100.0, or
//     - Have a lower-case e that represents scientific notation,
//       e.g. -3e-3, 6.02e23.
//
// Integers can be:
//
//     - decimal (e.g. -827)
//     - hexadecimal (must begin with 0x and must use capital A-F, e.g. 0x1A2B)
//
func LexNumberOrDuration(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	typ, ok := scanNumericOrDuration(l, true)
	u.Debugf("typ%T   %v", typ, ok)
	if !ok {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	l.Emit(typ)
	return nil
}

// LexDuration floats, integers time-durations
//
// durations:   45m, 2w, 20y, 22d, 40ms, 100ms, -100ms
//
func LexDuration(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	typ, ok := scanNumericOrDuration(l, true)
	if !ok {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	l.Emit(typ)
	return nil
}

// scan for a number
//
// It returns the scanned tokenType (tokenFloat or tokenInteger) and a flag
// indicating if an error was found.
//
func scanNumber(l *Lexer) (typ TokenType, ok bool) {
	return scanNumericOrDuration(l, false)
}

// scan for a number
//
// It returns the scanned tokenType (tokenFloat or tokenInteger) and a flag
// indicating if an error was found.
//
func scanNumericOrDuration(l *Lexer, doDuration bool) (typ TokenType, ok bool) {
	typ = TokenInteger
	// Optional leading sign.
	hasSign := l.accept("+-")
	peek2 := l.peekX(2)
	//u.Debugf("scanNumericOrDuration?  '%v'", string(peek2))
	if peek2 == "0x" {
		// Hexadecimal.
		if hasSign {
			// No signs for hexadecimals.
			return
		}
		l.acceptRun("0x")
		if !l.acceptRun(hexDigits) {
			// Requires at least one digit.
			return
		}
		if l.accept(".") {
			// No dots for hexadecimals.
			return
		}
	} else {
		// Decimal
		if !l.acceptRun(decDigits) {
			// Requires at least one digit
			return
		}
		if l.accept(".") {
			// Float
			if !l.acceptRun(decDigits) {
				// Requires a digit after the dot.
				return
			}
			typ = TokenFloat
		} else {
			if (!hasSign && l.input[l.start] == '0') ||
				(hasSign && l.input[l.start+1] == '0') {
				if peek2 == "0 " || peek2 == "0," {
					return typ, true
				}
				// Integers can't start with 0.
				return
			}
		}
		if l.accept("e") {
			l.accept("+-")
			if !l.acceptRun(decDigits) {
				// A digit is required after the scientific notation.
				return
			}
			typ = TokenFloat
		}
	}

	if doDuration {
		if l.acceptRun("yYmMdDuUsSwW") {
			// duration was found
			typ = TokenDuration
		}
	} else {
		// Next thing must not be alphanumeric.
		if isAlNum(l.Peek()) {
			l.Next()
			return
		}
	}

	ok = true
	return
}

// Helpers --------------------------------------------------------------------

// is Alpha Numeric reports whether r is an alphabetic, digit, or underscore.
func isAlNum(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// is Alpha reports whether r is an alphabetic, or underscore or period
func isAlpha(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || r == '.'
}

// is Alpha Numeric reports whether r is an alphabetic, digit, or underscore, or period
func isAlNumOrPeriod(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.'
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}
func isWhiteSpace(r rune) bool {
	switch r {
	case '\r', '\n', '\t', ' ':
		return true
	}
	return false
}

// Is the given rune valid in an identifier?
func isIdentCh(r rune) bool {
	switch {
	case isAlNum(r):
		return true
	case r == '_':
		return true
	}
	return false
}

func isIdentifierRune(r rune) bool {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return true
	}
	for _, allowedRune := range IDENTITY_CHARS {
		if allowedRune == r {
			return true
		}
	}
	return false
}

func isIdentifierFirstRune(r rune) bool {
	if r == '\'' {
		return false
	} else if isDigit(r) {
		return false
	} else if isAlpha(r) {
		return true
	} else if r == '@' {
		// are we really going to support this globaly as identity?
		return true
	}
	return false
}

func isLaxIdentifierRune(r rune) bool {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return true
	}
	for _, allowedRune := range IDENTITY_LAX_CHARS {
		if allowedRune == r {
			return true
		}
	}
	return false
}

// Uses the identity escaping/quote characters
func isIdentityQuoteMark(r rune) bool {
	return bytes.IndexByte(IdentityQuoting, byte(r)) >= 0
}
