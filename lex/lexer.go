// Lexing for QLBridge implements 4 Dialects
//  - SQL
//  - FilterQL a Where filtering language
//  - Json
//  - Expression - simple native logical/functional expression evaluator
package lex

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	u "github.com/araddon/gou"
)

var _ = u.EMPTY

var (
	// FEATURE FLAGS
	SUPPORT_DURATION = true
	// Identity Quoting
	//  http://stackoverflow.com/questions/1992314/what-is-the-difference-between-single-and-double-quotes-in-sql
	// you might want to set this to not include single ticks
	//  http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
	//IdentityQuoting = []byte{'[', '`', '"'} // mysql ansi-ish, no single quote identities, and allowing double-quote
	IdentityQuotingWSingleQuote = []byte{'[', '`', '\''} // more ansi-ish, allow single quotes around identities
	IdentityQuoting             = []byte{'[', '`'}       // no single quote around identities bc effing mysql uses single quote for string literals
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
	if len(dialect.IdentityQuoting) > 0 {
		l.identityRunes = dialect.IdentityQuoting
	} else {
		l.identityRunes = IdentityQuoting
	}
	l.init()
	return l
}

// Creates a new json dialect lexer for the input string
//
func NewJsonLexer(input string) *Lexer {
	return NewLexer(input, JsonDialect)
}

// creates a new lexer for the input string using SqlDialect
//  this is sql(ish) compatible parser
//
func NewSqlLexer(input string) *Lexer {
	return NewLexer(input, SqlDialect)
}

// Lexer holds the state of the lexical scanning.
//
//  Holds a *Dialect* which gives much of the
//    rules specific to this language
//
// many-generations removed from that Based on the lexer from the "text/template" package.
// See http://www.youtube.com/watch?v=HxaD_trXwRE
type Lexer struct {
	input         string     // the string being scanned
	state         StateFn    // the next lexing function to enter
	identityRunes []byte     // List of legal identity escape bytes
	pos           int        // current position in the input
	start         int        // start position of this token
	width         int        // width of last rune read from input
	line          int        // Line we are currently on
	linepos       int        // Position of start of current line
	lastToken     Token      // last token we emitted
	tokens        chan Token // channel of scanned tokens we output on
	doubleDelim   bool       // flag for tags starting with double braces
	dialect       *Dialect   // Dialect is the syntax-rules for all statement-types of this language
	statement     *Clause    // Statement type we are lexing
	curClause     *Clause    // Current clause we are lexing, we descend, ascend, iter()
	descent       *Clause    // Clause we have descended to
	peekedWordPos int
	peekedWord    string
	lastQuoteMark byte

	// Due to nested Expressions and evaluation this allows us to descend/ascend
	// during lex, using push/pop to add and remove states needing evaluation
	stack []NamedStateFn
}

func (l *Lexer) init() {
	//l.dialect.Init()
	l.ReverseTrim()
}

// returns the next token from the input
func (l *Lexer) NextToken() Token {

	for {
		//u.Debugf("token: start=%v  pos=%v  peek5=%s", l.start, l.pos, l.PeekX(5))
		select {
		case token := <-l.tokens:
			return token
		default:
			if l.state == nil && len(l.stack) > 0 {
				l.state = l.pop()
			} else if l.state == nil {
				return Token{T: TokenEOF, V: ""}
			}
			l.state = l.state(l)
		}
	}
	panic("not reachable")
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

// next returns the next rune in the input
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

func (l *Lexer) RawInput() string {
	return l.input
}

// SQL and other string expressions may contain more than one
//  statement such as:
//
//    use schema_x;  show tables;
//
//    set @my_var = "value"; select a,b from `users` where name = @my_var;
//
func (l *Lexer) Remainder() (string, bool) {
	l.SkipWhiteSpaces()
	if r := l.Peek(); r == ';' {
		l.Next()
	}
	l.SkipWhiteSpaces()
	if l.pos >= len(l.input) {
		return "", false
	}
	l.input = l.input[l.pos:]
	if len(l.input) < 5 {
		return "", false
	}
	return l.input, true
}

// peek returns but does not consume the next rune in the input.
func (l *Lexer) Peek() rune {
	r := l.Next()
	l.backup()
	return r
}

// grab the next x characters without consuming
func (l *Lexer) PeekX(x int) string {
	if l.pos+x > len(l.input) {
		return l.input[l.pos:]
	}
	return l.input[l.pos : l.pos+x]
}

// get single character
func (l *Lexer) peekXrune(x int) rune {
	if l.pos+x > len(l.input) {
		return rune(0)
	}
	return rune(l.input[l.pos+x])
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
		if unicode.IsSpace(r) || !IsIdentifierRune(r) {
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

	if l.pos == l.peekedWordPos && l.peekedWordPos > 0 {
		return l.peekedWord
	}
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
	i := skipWs
	for ; i < len(l.input)-l.pos; i++ {
		r, ri := utf8.DecodeRuneInString(l.input[l.pos+i:])
		//u.Debugf("r: %v  identifier?%v", string(r), IsIdentifierRune(r))
		if ri != 1 {
			//i += (ri - 1)
		}
		if unicode.IsSpace(r) || (!IsIdentifierRune(r) && r != '@') || r == '(' {
			if i > 0 {
				//u.Infof("hm:   '%v'", l.input[l.pos+skipWs:l.pos+i])
				l.peekedWordPos = l.pos
				l.peekedWord = l.input[l.pos+skipWs : l.pos+i]
				return l.peekedWord
			} else if r == '(' {
				// regardless of being short, lets treat like word
				return string(r)
			}
		}
	}
	//u.Infof("hm:   '%v'", l.input[l.pos+skipWs:l.pos+i])
	l.peekedWordPos = l.pos
	l.peekedWord = l.input[l.pos+skipWs : l.pos+i]
	return l.peekedWord
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
func (l *Lexer) IsEnd() bool {
	//u.Infof("isEnd? %v:%v", l.pos, len(l.input))
	if l.pos >= len(l.input) {
		return true
	}
	// if l.Peek() == ';' {
	// 	return true
	// }
	return false
}

// Is this a comment?
func (l *Lexer) IsComment() bool {
	r := l.Peek()
	switch r {
	case '#':
		return true
	case '/', '-':
		// continue on, might be, check 2nd character
		cv := l.PeekX(2)
		switch cv {
		case "//":
			return true
		case "--":
			return true
		}
	default:
		return false
	}
	return false
}

// emit passes an token back to the client.
func (l *Lexer) Emit(t TokenType) {
	//u.Debugf("emit: %s  '%s'  stack=%v start=%d pos=%d", t, l.input[l.start:l.pos], len(l.stack), l.start, l.pos)

	// We are going to use 1 based indexing (not 0 based) for lines
	// because humans don't think that way
	if l.lastQuoteMark != 0 {
		l.lastToken = Token{T: t, V: l.input[l.start:l.pos], Quote: l.lastQuoteMark, Line: l.line + 1, Column: l.columnNumber()}
		l.lastQuoteMark = 0
	} else {
		l.lastToken = Token{T: t, V: l.input[l.start:l.pos], Line: l.line + 1, Column: l.columnNumber()}
	}
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
	if l.pos <= l.start {
		return ""
	}
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
	//return 1 + strings.Count(l.input[:l.pos], "\n")
	return l.line
}

// columnNumber reports which column in the current line we're on.
func (l *Lexer) columnNumber() int {
	// n := strings.LastIndex(l.input[:l.pos], "\n")
	// if n == -1 {
	// 	n = 0
	// }
	// return l.pos - n
	return l.pos - l.linepos
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
		if rune == '\n' {
			// New line, lets keep track of line position
			l.line++
			l.linepos = l.pos
		}
	}
	l.backup()
	l.ignore()
}

// Skips white space characters in the input, returns bool
//  for if it contained new line
func (l *Lexer) SkipWhiteSpacesNewLine() bool {
	rune := l.Next()
	hasNewLine := false
	for {
		if rune == '\n' {
			hasNewLine = true
			// New line, lets keep track of line position
			l.line++
			l.linepos = 0
		} else if !unicode.IsSpace(rune) {
			break
		}
		rune = l.Next()
	}
	l.backup()
	l.ignore()
	return hasNewLine
}

// Skips white space characters at end by trimming so we can recognize the end
//  more easily
func (l *Lexer) ReverseTrim() {
	for i := len(l.input) - 1; i >= 0; i-- {
		if !unicode.IsSpace(rune(l.input[i])) {
			if i < (len(l.input) - 1) {
				//u.Warnf("trim: '%v'", l.input[:i+1])
				l.input = l.input[:i+1]
			}
			break
		}
	}
}

// Scans input and matches against the string.
// Returns true if the expected string was matched.
// expects matchTo to be a lower case string
func (l *Lexer) match(matchTo string, skip int) bool {

	//u.Debugf("match(%q)  peek:%q ", matchTo, l.PeekWord())
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
	if l.IsEnd() {
		return true
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
//    - (   left-paren
//
func (l *Lexer) isExpr() bool {
	// Expressions are strings not values, so quoting them means no
	r := l.Peek()
	switch {
	case r == '\'':
		return false
	case isDigit(r):
		// first character of expression cannot be digit
		return false
	case r == '!':
		//u.Debugf("found negation! : %v", string(r))
		// Negation is possible?
		l.Next()
		if l.isExpr() {
			l.backup()
			return true
		}
		l.backup()
	case r == '(':
		// ??? paran's wrapping sub-expressions?
		return true
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
	//u.Debugf("isNextKeyword?  '%s'   len:%v", kwMaybe, len(l.statement.Clauses))

	clause := l.curClause.next
	if clause == nil {
		clause = l.curClause.parent
	}
	//u.Infof("clause: %s", clause)

	for {
		if clause == nil {
			//u.Warnf("returning, not keyword")
			break
		}
		//clause = l.statement.Clauses[i]
		//u.Infof("clause: %+v", clause)
		//u.Debugf("clause next keyword?    peek=%s cname=%q keyword=%v multi?%v children?%v", kwMaybe, clause.Name, clause.keyword, clause.multiWord, len(clause.Clauses))
		if clause.keyword == kwMaybe || (clause.multiWord && strings.ToLower(l.PeekX(len(clause.fullWord))) == clause.fullWord) {
			//u.Infof("return true:  %v", strings.ToLower(l.PeekX(len(clause.fullWord))))
			return true
		}
		// TODO:  allow clauses to reserve keywords, or sub-clause
		switch kwMaybe {
		case "select", "insert", "delete", "update", "from", "inner", "outer":
			//u.Warnf("doing true: %v", kwMaybe)
			return true
		}
		if !clause.Optional {
			return false
		}
		clause = clause.next
	}

	return false
}

// non-consuming isIdentity
//  Identities are non-numeric string values that are not quoted
func (l *Lexer) isIdentity() bool {
	// Identity are strings not values
	r := l.Peek()
	switch {
	case r == '[':
		// This character [ is a little special
		// as it is going to look to see if the 2nd character is
		//  valid identity character so ie alpha/numeric
		peek2 := l.PeekX(2)
		if len(peek2) == 2 {
			return isIdentifierFirstRune(rune(peek2[1]))
		}
		return true
	case l.isIdentityQuoteMark(r):
		// are these always identities?  or do we need
		// to also check first identifier?
		// peek2 := l.PeekX(2)
		// if len(peek2) == 2 {
		// 	return isIdentifierFirstRune(rune(peek2[1]))
		// }
		return true
	}
	return isIdentifierFirstRune(r)
}

// Uses the identity escaping/quote characters
func (l *Lexer) isIdentityQuoteMark(r rune) bool {
	return bytes.IndexByte(l.identityRunes, byte(r)) >= 0
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

// current clause state function, used for repeated clauses
func (l *Lexer) clauseState() StateFn {
	if l.curClause != nil {
		if len(l.curClause.Clauses) > 0 {
			return l.curClause.Clauses[0].Lexer
		}
		return l.curClause.Lexer
	}
	u.Debugf("curClause? %v", l.curClause)
	//u.Debugf("curClause: %v", len(l.curClause.Clauses))
	u.Warnf("empty lex fn? %v", l.PeekX(10))
	return emptyLexFn
}

var emptyLexFn = func(*Lexer) StateFn { u.Debugf("empty statefun"); return nil }

// matches expected tokentype emitting the token on success
// and returning passed state function.
func LexMatchClosure(tok TokenType, nextFn StateFn) StateFn {
	return func(l *Lexer) StateFn {
		//u.Debugf("%p lexMatch   t=%s peek=%s", l, tok, l.PeekWord())
		if l.match(tok.String(), 0) {
			//u.Debugf("found match: %s   %v", tok, nextFn)
			l.Emit(tok)
			return nextFn
		}
		u.Warnf("unexpected token: %v   peek:%s", tok, l.PeekX(20))
		return l.errorToken("Unexpected token:" + l.current())
	}
}

// State functions ------------------------------------------------------------

// Find first keyword in the current queryText, then find appropriate statement in dialect.
// ie [SELECT, ALTER, CREATE, INSERT] in sql
func LexDialectForStatement(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	r := l.Peek()

	switch r {
	case '/', '-', '#':
		// ensure we have consumed all initial pre-statement comments
		l.Push("LexDialectForStatement", LexDialectForStatement)
		return LexComment(l)
	default:
		peekWord := strings.ToLower(l.PeekWord())
		for _, stmt := range l.dialect.Statements {
			if l.IsEnd() {
				break
			}

			//u.Warnf("LexDialectForStatement peek=%s  keyword=%v ", peekWord, stmt.Token.String())
			if stmt.MatchesKeyword(peekWord, l) {
				// We aren't actually going to consume anything here, just find
				// the correct statement
				l.statement = stmt
				l.curClause = stmt
				if len(stmt.Clauses) > 0 {
					l.curClause = stmt.Clauses[0]
				}
				//u.Infof("statement: %s  curClause %s", l.statement, l.curClause)
				return LexStatement
			} else if stmt.Token == TokenNil {
				if len(stmt.Clauses) == 1 {
					l.statement = stmt
					l.curClause = stmt
					if len(stmt.Clauses) > 0 {
						l.curClause = stmt.Clauses[0]
					}
					return l.clauseState()
				}
				l.statement = stmt
				l.curClause = stmt
				if len(stmt.Clauses) > 0 {
					l.curClause = stmt.Clauses[0]
				}
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

		clause := l.curClause
		repeat := false
		peekWord := strings.ToLower(l.PeekWord())

		//u.Debugf("%p curClause %s", l, clause)

		// Before we move onto next clause, lets check and see if we need to descend
		//  into sub-clauses of current statement
		if len(clause.Clauses) > 0 {
			//u.Infof("%p has sub-clauses  kw=%-10s peek=%-10s", l, clause.keyword, peekWord)
			for _, sc := range clause.Clauses {
				//u.Infof("%p has sub-clauses  kw=%-10s peek=%-10s", l, sc.keyword, peekWord)
				if sc.MatchesKeyword(peekWord, l) {
					//u.Infof("matches Sub-Clause: %-10s", sc.keyword)
					l.curClause = sc
					clause = sc
					break
				} else if !sc.Optional {
					break // First non-optional we don't match we bail
				}
			}
		}

	ClauseIterator:
		for {
			if clause == nil || l.IsEnd() {
				break
			}

			// we only ever consume each clause once?
			//u.Debugf("%p:%p stmt.clause parser?  peek=%-10q  keyword=%-10q multi?%v name=%-10q", clause.parent, clause, peekWord, clause.keyword, clause.multiWord, clause.Name)
			if clause.Lexer != nil && clause.MatchesKeyword(peekWord, l) {

				// Set the default entry point for this keyword
				l.curClause = clause

				//u.Debugf("dialect clause:  '%v' LexerNil?%v \n\t %s ", clause.keyword, clause.Lexer == nil, l.input)
				//u.Infof("matched stmt.clause token=%-10q match %-10q  clausekw=%-10q multi?%v name=%-10q", clause.Token, peekWord, clause.keyword, clause.multiWord, clause.Name)
				l.Push("LexStatement", LexStatement)
				if int(clause.Token) == 0 {
					nextState := clause.Lexer(l)
					if nextState == nil {
						//u.Warnf("nil next state")
					} else {
						//u.Debugf("found next state")
						return nextState
					}
				} else if clause.Optional {
					return l.lexIfMatch(clause.Token, clause.Lexer)
				} else {
					return LexMatchClosure(clause.Token, clause.Lexer)
				}
			} else if clause.MatchesKeyword(peekWord, l) {
				//u.Debugf("nil lexer but matches? repeat?%v isrepeat?%v  name=%q", clause.Repeat, repeat, clause.Name)
				if !repeat || clause.Repeat {

					// Before we move into child clause, lets check and see if we need to descend
					//  into sub-clauses of current statement
					if len(clause.Clauses) > 0 {
						//u.Debugf("has sub-clauses  kw=%-10s peek=%-10s", clause.keyword, peekWord)
						for _, sc := range clause.Clauses {
							//u.Debugf("has sub-clauses  kw=%-10s peek=%-10s", sc.keyword, peekWord)
							if sc.MatchesKeyword(peekWord, l) {
								//u.Debugf("matches Sub-Clause: kw=%-15q  %-15q", sc.keyword, sc.Name)
								l.curClause = sc
								clause = sc
								repeat = true
								goto ClauseIterator
							} else if !sc.Optional {
								break // First non-optional we don't match we bail
							}
						}
					}
				}
			}

			// if we didn't match
			if clause.next == nil && clause.parent != nil {

				if clause.parent.Repeat && repeat == false {
					// we haven't tried to repeat yet
					repeat = true
					clause = clause.parent
					//u.Debugf("repeating clause: %v", clause.keyword)
					// Before we move onto next clause, lets check and see if we need to descend
					//  into sub-clauses of current statement
					if len(clause.Clauses) > 0 {
						//u.Debugf("has sub-clauses  kw=%-10s peek=%-10s", clause.keyword, peekWord)
						for _, sc := range clause.Clauses {
							//u.Infof("has sub-clauses  kw=%-10s peek=%-10s", sc.keyword, peekWord)
							if sc.MatchesKeyword(peekWord, l) {
								//u.Debugf("matches Sub-Clause: %-10s", sc.keyword)
								l.curClause = sc
								clause = sc
								break
							} else if !sc.Optional {
								break // First non-optional we don't match we bail
							}
						}
					}

				} else {

					if clause.parent.next == nil {
						//u.Warnf("nil?  %#v ", clause.parent)
						clause = clause.parent.next
						break
					}
					clause = clause.parent.next
					//u.Infof("moving to next parent clause: %v", clause.keyword)
				}

			} else {
				clause = clause.next
			}

		}
		// If we have consumed all clauses, we are ready to be done?
		//u.Debugf("not found? word? '%s' %v", peekWord, clause)
		if clause == nil {
			//u.Infof("%p Run End of statement", l)
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
//   ie, the full logical boolean logic
//
func LexLogical(l *Lexer) StateFn {

	//u.Debug("in lexLogical: ", l.PeekX(5))
	l.SkipWhiteSpaces()

	// r := l.Peek()
	// switch r {
	// case '/', '-', '#':
	// 	// ensure we have consumed all comments
	// 	l.Push("LexLogical", LexLogical)
	// 	return LexComment(l)
	// default:
	//}
	if l.IsEnd() {
		l.Emit(TokenEOF)
		return nil
	}

	l.Push("LexLogical", LexLogical)
	//u.Debugf("LexLogical:  %v", l.PeekWord())
	return LexExpression(l)
}

//Doesn't actually lex anything, used for single token clauses
func LexEmpty(l *Lexer) StateFn { return nil }

// lex a value:   string, integer, float
//
// - literal strings must be quoted
// - numerics with no period are integers
// - numerics with period are floats
//
//  "stuff"    -> [string] = stuff
//  'stuff'    -> [string] = stuff
//  "items's with quote" -> [string] = items's with quote
//  1.23  -> [float] = 1.23
//  100   -> [integer] = 100
//  ["hello","world"]  -> [array] {"hello","world"}
//
func LexValue(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return l.errorToken("expected value but got EOF")
	}
	rune := l.Next()
	typ := TokenValue

	//u.Debugf("LexValue: rune=%v  peek:%v", string(rune), l.PeekX(10))

	switch rune {
	case ')':
		// this is a mistake and should not happen
		u.Warnf("why did we get paren? going to panic")
		//panic("should not have paren")
		return nil
	case '[':
		if l.isIdentity() {
			l.backup()
			return nil
		}
		//l.backup()
		l.Emit(TokenLeftBracket)
		return LexJsonArray
		//return LexValue
	case '\'', '"':
		// quoted string, allows escaping
		firstRune := rune
		l.ignore() // consume the quote mark
		previousEscaped := rune == '\\'
		for rune = l.Next(); ; rune = l.Next() {

			//u.Debugf("LexValue rune=%v  end?%v  prevEscape?%v", string(rune), rune == eof, previousEscaped)
			if (rune == '\'' || rune == '"') && rune == firstRune && !previousEscaped {
				if !l.IsEnd() {
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
						l.lastQuoteMark = byte(firstRune)
						l.Emit(typ)
						// now ignore that single quote
						l.Next()
						l.ignore()
						return nil
					}
				} else {
					// at the very end
					l.backup()
					l.lastQuoteMark = byte(firstRune)
					l.Emit(typ)
					l.Next()
					return nil
				}
			} else if rune == eof {
				return l.errorToken("reached end without finding end for quoted value")
			}
			if rune == 0 {
				return l.errorToken("string value was not delimited")
			}
			previousEscaped = rune == '\\'
		}
	default:
		if rune == '*' {
			u.LogTracef(u.WARN, "why are we having a star here? %v", l.PeekX(10))
		}
		// Non-Quoted String?   Should this be a numeric?   or date or what?  duration?  what kinds are valid?
		//  A:   numbers
		l.backup()
		switch rune {
		case 't', 'T', 'F', 'f':
			// lets look for Booleans
			boolCandiate := strings.ToLower(l.PeekWord())
			switch boolCandiate {
			case "true", "t", "f", "false":
				l.ConsumeWord(boolCandiate)
				l.Emit(TokenBool)
				return nil
			}
		}
		//u.Debugf("lexNumber?  %v", string(l.PeekX(5)))
		return LexNumber(l)
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
	if l.IsEnd() {
		u.Error("wat?")
		return l.errorToken("expected value but got EOF")
	}

	rune := l.Next()
	if rune != '/' {
		u.Errorf("wat? %v", string(rune))
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
				//u.Debugf("LexRegex rune=%v  end?%v  prevEscape?%v", string(rune), rune == eof, previousEscaped)
				if rune == eof {
					l.Emit(TokenRegex)
					return nil
					//return l.errorToken("expected value but got EOF")
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

// lex the right side paren of something
func LexRightParen(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	r := l.Next()
	if r == ')' {
		l.Emit(TokenRightParenthesis)
		return nil
	}
	l.backup()
	return l.errorToken("expression must end with a paren: ) " + l.PeekX(5))
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

	r := l.Peek()
	if r == '(' {
		l.Next()
		l.Emit(TokenLeftParenthesis)
		return LexExpressionOrIdentity
	}
	//u.Debugf("LexExpressionOrIdentity identity?%v expr?%v %v peek5='%v'", l.isIdentity(), l.isExpr(), string(l.Peek()), string(l.PeekX(5)))
	// Expressions end in Parens:     LOWER(item)
	if l.isExpr() {
		return lexExpressionIdentifier(l)
	} else if l.isIdentity() {
		// Non Expressions are Identities, or Columns
		//u.Warnf("in expr is identity? %s", l.PeekWord())
		// by passing nil here, we are going to go back to Pull items off stack)
		return LexIdentifier(l)
	} else {
		//u.Warnf("LexExpressionOrIdentity ??? -> LexValue")
		return LexValue(l)
	}

	return nil
}

// look for either an Identity or Value
//
func LexIdentityOrValue(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	r := l.Peek()
	if r == '(' {
		return nil
	}
	//u.Debugf("LexIdentityOrValue identity?%v expr?%v %v peek5='%v'", l.isIdentity(), l.isExpr(), string(l.Peek()), string(l.PeekX(5)))
	// Expressions end in Parens:     LOWER(item)
	if l.isExpr() {
		return nil
	} else if l.isIdentity() {
		// Non Expressions are Identities, or Columns
		//u.Warnf("in expr is identity? %s", l.PeekWord())
		// by passing nil here, we are going to go back to Pull items off stack)
		return LexIdentifier(l)
	} else {
		//u.Warnf("LexIdentityOrValue ??? -> LexValue")
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
	for rune := l.Next(); IsIdentifierRune(rune); rune = l.Next() {
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
//       CAST(field AS int)
//
//       (a,b,c,d)   -- For Insert statment, list of columns
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
		//l.Push("LexRightParen", LexRightParen)
		return LexListOfArgs
	case ',':
		l.Emit(TokenComma)
		return LexListOfArgs
	case '*':
		if &l.lastToken != nil && l.lastToken.T == TokenLeftParenthesis {
			l.Emit(TokenStar)
			return LexRightParen
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
	case ']':
		return nil
	default:
		// So, not comma, * so either is Expression, Identity, Value
		l.backup()
		peekWord := strings.ToLower(l.PeekWord())
		//u.Debugf("in LexListOfArgs:  '%s'", peekWord)
		// First, lets ensure we haven't blown past into keyword?
		if peekWord == "as" {
			l.Next()
			l.Next()
			l.Emit(TokenAs)
			return LexListOfArgs
		}
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
var LexTableIdentifier = LexIdentifierOfType(TokenTable)

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
//  @@varname      select @@varname;
//
func LexIdentifierOfType(forToken TokenType) StateFn {

	return func(l *Lexer) StateFn {
		l.SkipWhiteSpaces()

		wasQouted := false
		// first rune has to be valid unicode letter or @@
		firstChar := l.Next()
		//u.Debugf("LexIdentifierOfType:   '%s' ='?%v peek6'%v'", string(firstChar), firstChar == '\'', l.PeekX(6))
		switch {
		case l.isIdentityQuoteMark(firstChar):
			// Fields can be bracket or single quote escaped
			//  [user]
			//  [email]
			//  'email'
			//  `email`
			l.ignore()
			l.lastQuoteMark = byte(firstChar)
			nextChar := l.Next()
			if !unicode.IsLetter(nextChar) {
				if nextChar == firstChar {
					// Empty Identity = value?  not really an identity is it?
					wasQouted = true
					return nil
				}
				//l.ignore()
				//u.Warnf("aborting LexIdentifierOfType: %v", l.PeekX(5))
				//return nil
				//return l.errorToken("identifier must begin with a letter " + l.PeekX(3))
			}
			// Since we escaped this with a quote we lex until unescaped end?
		identityForLoop:
			for {
				nextChar = l.Next()
				//isLaxIdentifierRune(nextChar)
				// TODO:  escaping?
				switch {
				case firstChar == '[' && nextChar == ']':
					if l.PeekX(2) == ".[" {
						// Identity of form   [schema].[table]
						//u.Warnf("%s", l.RawInput())
						l.Next()
						l.Next()
					} else {
						break identityForLoop
					}
				case firstChar == '\'' && nextChar == '\'':
					break identityForLoop
				case firstChar == '`' && nextChar == '`':
					if l.PeekX(2) == ".`" {
						// Identity of form   `schema`.`table`
						//u.Warnf("%s", l.RawInput())
						l.Next()
						l.Next()
					} else {
						break identityForLoop
					}

				case nextChar == eof:
					break identityForLoop
				}
			}
			// iterate until we find non-identifier, then make sure it is valid/end
			if firstChar == '[' && nextChar == ']' {
				// valid
			} else if firstChar == nextChar && l.isIdentityQuoteMark(nextChar) {
				// also valid
			} else {
				u.Errorf("unexpected character in identifier?  %v", string(nextChar))
				return l.errorToken("unexpected character in identifier:  " + string(nextChar))
			}
			wasQouted = true
			l.backup()
		default:
			if firstChar == '@' && l.Peek() == '@' {
				l.Next()
			}
			l.lastQuoteMark = 0
			if !isIdentifierFirstRune(firstChar) && !isDigit(firstChar) {
				//u.Warnf("aborting LexIdentifier: '%v'", string(firstChar))
				return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
			}
			allDigits := isDigit(firstChar)
			for rune := l.Next(); IsIdentifierRune(rune); rune = l.Next() {
				// iterate until we find non-identifer character
				if allDigits && !isDigit(rune) {
					allDigits = false
				}
			}
			if allDigits {
				return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
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

		return nil // pop up to parent
	}
}

var LexDataTypeIdentity = LexDataType(TokenDataType)

// LexDataType scans and finds datatypes
//
//   [] are valid inside of data types, no escaping such as ',"
//
//  []string       CREATE table( field []string )
//  map[string]int
//  int, string, etc
//
func LexDataType(forToken TokenType) StateFn {

	return func(l *Lexer) StateFn {
		l.SkipWhiteSpaces()

		//u.Debugf("LexDataType: %v", l.PeekX(5))

		// Since we escaped this with a quote we allow laxIdentifier characters
		for {
			r := l.Next()
			//u.Infof("r=%v %v    ws=%v", string(r), r, isWhiteSpace(r))
			switch {
			case r == '[' || r == ']':
				// ok, continue
			case isWhiteSpace(r):
				l.backup()
				l.Emit(forToken)
				return nil
			case isLaxIdentifierRune(r):
				//ok, continue
			case IsBreak(r):
				l.backup()
				l.Emit(forToken)
				return nil
			}
		}
		return nil // pop up to parent
	}
}

// Look for end of statement defined by either a semicolon or end of file
func LexEndOfStatement(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	r := l.Next()
	if r == ';' {
		l.Emit(TokenEOS)
		return nil
	}
	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	//l.backup()
	//u.Warnf("%p error looking for end of statement: '%v'", l, l.remainder())
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
	if l.IsEnd() {
		return nil
	}
	first := strings.ToLower(l.PeekX(2))

	//u.Debugf("LexSelectClause  '%v'  %v", first, l.PeekX(10))

	switch first {
	case "al": //ALL?
		word := strings.ToLower(l.PeekX(4))
		if word == "all " || word == "all\n" {
			l.ConsumeWord("all")
			l.Emit(TokenAll)
		}
	case "di": //Distinct?
		word := strings.ToLower(l.PeekX(len("DISTINCT ")))
		if word == "distinct " || word == "distinct\n" {
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
		//u.Warnf("What is this? %v", l.PeekX(10))
	case "@@": //  mysql system variables start with @@
		l.Next()
		l.Next()
		word := strings.ToLower(l.PeekWord())
		l.ConsumeWord(word)
		l.Emit(TokenIdentity)
		//u.Debugf("Found Sql Variable:  @@%v", word)
		return LexSelectList
	default:
		//u.Debugf("not found %v", first)
		if strings.HasPrefix(first, "@") {
			l.Next()
			word := strings.ToLower(l.PeekWord())
			l.ConsumeWord(word)
			l.Emit(TokenIdentity)
			//u.Debugf("Found Sql Variable:  @%v", word)
			return LexSelectList
		}
	}

	word := l.PeekWord()
	if l.isNextKeyword(word) {
		return nil
	}

	// Since we did Not find anything it, start lexing normal SelectList
	return LexSelectList
}

// Handle start of insert, Upsert statements
//
func LexUpsertClause(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	word := strings.ToLower(l.PeekWord())

	//u.Debugf("LexUpsertClause  '%v'  %v", word, l.PeekX(10))

	switch word {
	case "insert":
		l.ConsumeWord(word)
		l.Emit(TokenInsert)
		return LexUpsertClause
	case "upsert":
		l.ConsumeWord(word)
		l.Emit(TokenUpsert)
		return LexUpsertClause
	case "into":
		l.ConsumeWord(word)
		l.Emit(TokenInto)
		return LexUpsertClause
	default:
		if l.isIdentity() {
			return LexTableIdentifier
		}
	}

	return nil
}

// Handle recursive subqueries
//
func LexSubQuery(l *Lexer) StateFn {

	//u.Debugf("LexSubQuery  '%v'", l.PeekX(10))
	l.SkipWhiteSpaces()
	if l.IsEnd() {
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
// <PREPARE_STMT> := PREPARE <identity>	FROM <string_value>
//
func LexPreparedStatement(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	//u.Debugf("LexPreparedStatement  '%v'", l.PeekX(10))

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
//     SELECT <select_list>
//
//     <select_list> := <select_col> [, <select_col>]*
//
//     <select_col> :== ( <identifier> | <expression> | '*' ) [AS <identifier>] [IF <expression>] [<comment>]
//
//  Note, our Columns support a non-standard IF guard at a per column basis
//
func LexSelectList(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexSelectList looking for operator:  word=%q", word)
	switch word {
	case "as":
		l.ConsumeWord(word)
		l.Emit(TokenAs)
		l.Push("LexSelectList", LexSelectList)
		return LexIdentifier
	case "if":
		l.skipX(2)
		l.Emit(TokenIf)
		l.Push("LexSelectList", LexSelectList)
		return LexExpression
	}
	return LexExpression
}

// Handle Source References ie [From table], [SubSelects], Joins
//
//    SELECT ...  FROM <sources>
//
//    <sources>      := <source> [, <join_clause> <source>]*
//    <source>       := ( <table_source> | <subselect> ) [AS <identifier>]
//    <table_source> := <identifier>
//    <join_clause>  := (INNER | LEFT | OUTER)? JOIN [ON <conditional_clause>]
//    <subselect>    := '(' <select_stmt> ')'
//
func LexTableReferenceFirst(l *Lexer) StateFn {

	// From has already been consumed

	l.SkipWhiteSpaces()

	//u.Debugf("LexTableReferenceFirst  peek2= '%v'  isEnd?%v", l.PeekX(2), l.IsEnd())

	if l.IsEnd() {
		return nil
	}
	r := l.Peek()

	// Cover the grouping, ie recursive/repeating nature of subqueries
	switch r {
	case ';':
		l.Next()
		l.Emit(TokenEOS)
		return nil
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		// subquery?
		l.Push("LexTableReferenceFirst", LexTableReferenceFirst)
		//l.clauseState() = LexSelectClause
		return LexSelectClause
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		return LexSelectClause
	}

	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexTableReferenceFirst looking for operator:  word=%s", word)
	switch word {
	case "select":
		// nice, this is what we are looking for, let dialect take over
		return nil
	case "as":
		l.ConsumeWord("AS")
		l.Emit(TokenAs)
		l.Push("LexTableReferenceFirst", LexTableReferenceFirst)
		l.Push("LexIdentifier", LexIdentifier)
		return nil
	case "in": // are there other functions besides in?
		l.ConsumeWord(word)
		l.Emit(TokenIN)
		l.Push("LexTableReferenceFirst", LexTableReferenceFirst)
		l.Push("LexListOfArgs", LexListOfArgs)
		return nil

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexTableReferenceFirst", LexTableReferenceFirst)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			//u.Warnf("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexTableReferenceFirst = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexTableReferenceFirst", LexTableReferenceFirst)
	} else {
		u.Errorf("Gracefully refusing to add more LexTableReferenceFirst: ")
	}

	// Since we did Not find anything, we are going to go for a Expression or Identity
	return LexExpressionOrIdentity
}

// Handle Source References ie [From table], [SubSelects], Joins
//
//    SELECT ...  FROM <sources>
//
//    <sources>      := <source> [, <join_clause> <source>]*
//    <source>       := ( <table_source> | <subselect> ) [AS <identifier>]
//    <table_source> := <identifier>
//    <join_clause>  := (INNER | LEFT | OUTER)? JOIN [ON <conditional_clause>]
//    <subselect>    := '(' <select_stmt> ')'
//
func LexTableReferences(l *Lexer) StateFn {

	// From has already been consumed

	l.SkipWhiteSpaces()

	//u.Debugf("LexTableReferences  peek2= '%v'  isEnd?%v", l.PeekX(2), l.IsEnd())

	if l.IsEnd() {
		return nil
	}
	r := l.Peek()

	// Cover the grouping, ie recursive/repeating nature of subqueries
	switch r {
	case ';':
		l.Next()
		l.Emit(TokenEOS)
		return nil
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		// subquery?
		l.Push("LexTableReferences", LexTableReferences)
		return LexSelectClause
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		return LexSelectClause
	}

	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexTableReferences looking for operator:  word=%s", word)
	switch word {
	case "select":
		// TODO:  need to allow the Dialect Statements to be recursive/nested
		// l.ConsumeWord("SELECT")
		// l.Emit(TokenSelect)
		return nil
	case "from", "where":
		//u.Warnf("emit from")
		// l.ConsumeWord("FROM")
		// l.Emit(TokenFrom)
		// l.Push("LexTableReferences", LexTableReferences)
		// l.Push("LexIdentifier", LexIdentifier)
		return nil
	case "as":
		l.ConsumeWord("AS")
		l.Emit(TokenAs)
		l.Push("LexTableReferences", LexTableReferences)
		l.Push("LexIdentifier", LexIdentifier)
		return nil
	case "outer":
		l.ConsumeWord(word)
		l.Emit(TokenOuter)
		return LexTableReferences
	case "inner":
		l.ConsumeWord(word)
		l.Emit(TokenInner)
		return LexTableReferences
	case "left":
		l.ConsumeWord(word)
		l.Emit(TokenLeft)
		return LexTableReferences
	case "right":
		l.ConsumeWord(word)
		l.Emit(TokenRight)
		return LexTableReferences
	case "join":
		l.ConsumeWord(word)
		l.Emit(TokenJoin)
		//l.Push("LexTableReferences", LexTableReferences)
		//l.Push("LexExpression", LexExpression)
		return LexTableReferences
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
		if l.isIdentity() {
			//return LexExpressionOrIdentity
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

// Handle Source References ie [From table], [SubSelects], Joins
//
//    SELECT ...  FROM <sources>
//
//    <sources>      := <source> [, <join_clause> <source>]*
//    <source>       := ( <table_source> | <subselect> ) [AS <identifier>]
//    <table_source> := <identifier>
//    <join_clause>  := (INNER | LEFT | OUTER)? JOIN [ON <conditional_clause>]
//    <subselect>    := '(' <select_stmt> ')'
//
func LexJoinEntry(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	//u.Debugf("LexJoinEntry  peek2= '%v'  isEnd?%v", l.PeekX(2), l.IsEnd())

	if l.IsEnd() {
		return nil
	}
	r := l.Peek()

	// Cover the grouping, ie recursive/repeating nature of subqueries
	switch r {
	case ';':
		l.Next()
		l.Emit(TokenEOS)
		return nil
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		// subquery?
		l.Push("LexJoinEntry", LexJoinEntry)
		return LexSelectClause
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		return LexSelectClause
	}

	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexJoinEntry looking for operator:  word=%s", word)
	switch word {
	case "select":
		return nil
	case "from", "where":
		return nil
	case "outer":
		l.ConsumeWord(word)
		l.Emit(TokenOuter)
		return LexJoinEntry
	case "inner":
		l.ConsumeWord(word)
		l.Emit(TokenInner)
		return LexJoinEntry
	case "left":
		l.ConsumeWord(word)
		l.Emit(TokenLeft)
		return LexJoinEntry
	case "right":
		l.ConsumeWord(word)
		l.Emit(TokenRight)
		return LexJoinEntry
	case "join":
		l.ConsumeWord(word)
		l.Emit(TokenJoin)
		//l.Push("LexJoinEntry", LexJoinEntry)
		//l.Push("LexExpression", LexExpression)
		return LexJoinEntry
	// case "in":
	// 	return nil

	default:
		r = l.Peek()
		if l.isNextKeyword(word) {
			//u.Warnf("found keyword? %v ", word)
			return nil
		}
		if l.isIdentity() {
			//u.Debugf("expression or identity?")
			return LexExpressionOrIdentity
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexJoinEntry = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexJoinEntry", LexJoinEntry)
	} else {
		u.Errorf("Gracefully refusing to add more LexJoinEntry: ")
	}

	// Since we did Not find anything, we are going to go for a Expression or Identity
	return LexExpressionOrIdentity
}

// Handle list of column names on insert/update statements
//
//     <insert_into> <col_names> VALUES <col_value_list>
//
//     <col_names> := '(' <identity> [, <identity>]* ')'
//
func LexColumnNames(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	r := l.Peek()
	//u.Debugf("LexColumnNames lr=%s  word=%q", string(r), l.PeekWord())
	switch r {
	case ',':
		l.Next()
		l.Emit(TokenComma)
		return LexColumnNames
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		return nil
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		return LexColumnNames
	default:
		l.Push("LexColumnNames", LexColumnNames)
		return LexIdentifier
	}
	return nil
}

// Handle repeating Insert/Upsert/Update statements
//
//     <insert_into> <col_names> VALUES <col_value_list>
//     <set> <upsert_cols> VALUES <col_value_list>
//
//     <upsert_cols> := <upsert_col> [, <upsert_col>]*
//     <upsert_col> := <identity> = <expr>
//
//     <col_names> := <identity> [, <identity>]*
//     <col_value_list> := <col_value_row> [, <col_value_row>] *
//
//     <col_value_row> := '(' <expr> [, <expr>]* ')'
//
func LexTableColumns(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	r := l.Peek()
	switch r {
	case ',':
		l.Next()
		l.Emit(TokenComma)
		return LexTableColumns
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		return LexTableColumns
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		return LexTableColumns
	}
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("looking for tablecolumns:  word=%s r=%s", word, string(r))
	switch word {
	case "values":
		l.ConsumeWord(word)
		l.Emit(TokenValues)
		return LexColumns
	case "set":
		l.ConsumeWord(word)
		l.Emit(TokenSet)
		return LexColumns
	default:
		switch l.lastToken.T {
		case TokenLeftParenthesis:
			l.Push("LexTableColumns", LexTableColumns)
			return LexListOfArgs
		case TokenSet:
			//l.Push("LexTableColumns", LexTableColumns)
			return LexColumns
		default:
			// TODO:  this is returning because l.clauseState()
			return LexColumns
		}
	}
	return l.errorf("unrecognized keyword: %q", word)
}

// Handle logical Conditional Clause used for [WHERE, WITH, JOIN ON]
// logicaly grouped with parens and/or seperated by commas or logic (AND/OR/NOT)
//
//     SELECT ... WHERE <conditional_clause>
//
//     <conditional_clause> ::= <expr> [( AND <expr> | OR <expr> | '(' <expr> ')' )]
//
//     <expr> ::= <predicatekw> '('? <expr> [, <expr>] ')'? | <func> | <subselect>
//
// SEE:  <expr> = LexExpression
//
func LexConditionalClause(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	//u.Debugf("lexConditional: %v", l.PeekX(14))
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	switch r {
	case ';':
		return nil
	case ',':
		l.Next()
		l.Emit(TokenComma)
		return LexConditionalClause
	case ')':
		l.Next()
		l.Emit(TokenRightParenthesis)
		return LexConditionalClause
	case '(':
		l.Next()
		l.Emit(TokenLeftParenthesis)
		return LexConditionalClause
	}
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("lexConditional word: %v", word)
	switch word {
	case "select", "where", "from":
		//u.LogThrottle(u.WARN, 5, "sure we want subQuery here? %v", word)
		return LexSubQuery
	}
	if l.isNextKeyword(word) {
		//u.Infof("is keyword %v", word)
		return nil
	}
	l.Push("LexConditionalClause", LexConditionalClause)
	//u.Debugf("go to lex expression: %v", l.PeekX(20))
	return LexExpression(l)
}

// Alias for Expression
func LexColumns(l *Lexer) StateFn {
	return LexExpression(l)
}

// <expr>   Handle single logical expression which may be nested and  has
//           user defined function names that are NOT validated by lexer
//
// <expr> ::= <predicatekw> '('? <expr> [, <expr>] ')'? | <func> | <subselect>
//  <func> ::= <identity>'(' <expr> ')'
//  <predicatekw> ::= [NOT] (IN | INTERSECTS | CONTAINS | RANGE | LIKE | EQUALS )
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
//  reg_date BETWEEN x AND y
//
func LexExpression(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	if l.IsComment() {
		l.Push("LexExpression", LexExpression)
		return LexComment
	}

	//u.Debugf("LexExpression  r='%v' word=%q", string(l.Peek()), l.PeekX(20))

	r := l.Next()
	// Cover the logic and grouping
	switch r {
	case '"':
		l.backup()
		l.Push("LexExpression", l.clauseState())
		return LexValue
	case '`':
		l.backup()
		l.Push("LexExpression", l.clauseState())
		return LexIdentifier
	case '@':
		if l.Peek() == '@' {
			//l.Next()
			l.backup()
			l.Push("LexExpression", l.clauseState())
			return LexIdentifier
		}
	case '!', '=', '>', '<', '(', ')', ',', ';', '-', '*', '+', '%', '&', '/', '|':
		foundLogical := false
		foundOperator := false
		switch r {
		case '-': // comment?  or minus?
			p := l.Peek()
			if p == '-' {
				l.backup()
				l.Push("LexExpression", LexExpression)
				return LexInlineComment
			} else {
				l.Emit(TokenMinus)
				return l.clauseState()
			}
		case ';':
			l.backup()
			return nil
		case '(': // this is a logical Grouping/Ordering
			//l.Push("LexParenEnd", LexParenEnd)
			l.Emit(TokenLeftParenthesis)
			//u.Debugf("return from left paren %v", l.PeekX(5))
			//l.Push("LexExpression", LexExpression)
			//l.Push("LexExpression", l.clauseState())
			return LexExpression //l.clauseState()
		case ')': // this is a logical Grouping/Ordering
			//u.Debugf("emit right paren")
			l.Emit(TokenRightParenthesis)
			return nil
		case ',':
			l.Emit(TokenComma)
			return l.clauseState()
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
				//u.Infof("found ==  peek5='%v'", string(l.PeekX(5)))
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
			} else {
				l.Emit(TokenLT)
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
			//l.Push("l.clauseState()", l.clauseState())
			return LexExpression
		} else if foundOperator {
			//u.Debugf("found LexExpression = peek5='%v'", string(l.PeekX(5)))
			// There may be more than one item here
			//l.Push("l.clauseState()", l.clauseState())
			return LexExpression
		}
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexExpression operator:  word=%q  kw?%v", word, l.isNextKeyword(word))
	switch word {
	case "as":
		return nil
	case "in", "intersects", "like", "between", "contains": // what is complete list here?
		switch word {
		case "in":
			l.ConsumeWord(word)
			l.Emit(TokenIN)
			l.SkipWhiteSpaces()
			if l.PeekX(1) == "(" {
				return LexListOfArgs
			}
			return LexExpressionOrIdentity
		case "intersects":
			l.ConsumeWord(word)
			l.Emit(TokenIntersects)
			l.SkipWhiteSpaces()
			if l.PeekX(1) == "(" {
				return LexListOfArgs
			}
			return LexExpressionOrIdentity
		case "like":
			l.ConsumeWord(word)
			l.Emit(TokenLike)
			return LexExpressionOrIdentity
		case "contains":
			l.ConsumeWord(word)
			if l.Peek() == '(' {
				l.Emit(TokenUdfExpr)
				l.Push("LexExpression", l.clauseState())
				return LexExpressionParens
			}
			l.Emit(TokenContains)
			return LexExpressionOrIdentity
		case "between":
			l.ConsumeWord(word)
			l.Emit(TokenBetween)
			l.Push("LexExpression", LexExpression)
			l.Push("LexExpressionOrIdentity", LexExpressionOrIdentity)
			return nil
		}
	case "exists":
		l.ConsumeWord(word)
		r = l.Peek()
		if r == '(' {
			l.Emit(TokenUdfExpr)
			return LexExpression
		}
		l.Emit(TokenExists)
		return LexExpression
	case "is":
		l.ConsumeWord(word)
		l.Emit(TokenIs)
		return LexExpression
	case "null":
		l.ConsumeWord(word)
		l.Emit(TokenNull)
		return LexExpression
	case "not":
		// somewhat weird edge case, not is either word not, or expression
		// not exactly context-free
		pr := l.peekXrune(len(word))
		//u.Infof("not?  %v", string(pr))
		if pr != '(' {
			l.ConsumeWord(word)
			l.Emit(TokenNegate)
			return LexExpression
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
		//l.Push("LexExpression", l.clauseState())
		return LexExpression

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexExpression", l.clauseState())
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(word) {
			//u.Debugf("found keyword? %v ", word)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	//u.Debugf("LexExpression = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		//l.Push("LexExpression", LexExpression)
		l.Push("LexExpression", l.clauseState())
	} else {
		u.Warnf("Gracefully refusing to add more LexExpression: ")
	}
	return LexExpressionOrIdentity
}

// <name_value_args>   Handle comma delimited list of name = value args
//
// Examples:
//
//  colx = y OR colb = b
//  cola = 'a5'
//  cola != "a5", colb = "a6"
//
func LexNameValueArgs(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	if l.IsComment() {
		l.Push("LexNameValueArgs", LexNameValueArgs)
		return LexComment
	}

	//u.Debugf("LexNameValueArgs  r='%v' word=%q", string(l.Peek()), l.PeekX(20))

	r := l.Next()
	// Cover the logic and grouping
	switch r {
	case '`':
		l.backup()
		l.Push("LexNameValueArgs", l.clauseState())
		return LexIdentifier
	case '@':
		if l.Peek() == '@' {
			//l.Next()
			l.backup()
			l.Push("LexNameValueArgs", l.clauseState())
			return LexIdentifier
		}
	case '!', '=', '>', '<', '(', ')', ',', ';', '-', '*', '+', '%', '&', '/', '|':
		foundLogical := false
		foundOperator := false
		switch r {
		case '-': // comment?  or minus?
			p := l.Peek()
			if p == '-' {
				l.backup()
				l.Push("LexExpression", LexExpression)
				return LexInlineComment
			} else {
				l.Emit(TokenMinus)
				return l.clauseState()
			}
		case ';':
			l.backup()
			return nil
		case '(': // this is a logical Grouping/Ordering
			//l.Push("LexParenEnd", LexParenEnd)
			l.Emit(TokenLeftParenthesis)
			//u.Debugf("return from left paren %v", l.PeekX(5))
			return LexExpression //l.clauseState()
		case ')': // this is a logical Grouping/Ordering
			//u.Debugf("emit right paren")
			l.Emit(TokenRightParenthesis)
			return nil
		case ',':
			l.Emit(TokenComma)
			return l.clauseState()
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
				//u.Infof("found ==  peek5='%v'", string(l.PeekX(5)))
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
			} else {
				l.Emit(TokenLT)
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
			//u.Debugf("found LexNameValueArgs = '%v'", string(r))
			// There may be more than one item here
			return LexNameValueArgs
		} else if foundOperator {
			//u.Debugf("found LexNameValueArgs = peek5='%v'", string(l.PeekX(5)))
			// There may be more than one item here
			return LexNameValueArgs
		}
	}

	l.backup()
	word := strings.ToLower(l.PeekWord())
	//u.Debugf("LexNameValueArgs operator:  word=%q  kw?%v", word, l.isNextKeyword(word))
	r = l.Peek()
	if r == ',' {
		l.Emit(TokenComma)
		l.Push("LexNameValueArgs", l.clauseState())
		return LexIdentityOrValue
	}
	if l.isNextKeyword(word) || l.isExpr() {
		//u.Debugf("found keyword? %v ", word)
		return nil
	}

	//u.Debugf("LexNameValueArgs = '%v'", string(r))
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexNameValueArgs", l.clauseState())
	} else {
		u.Warnf("Gracefully refusing to add more LexNameValueArgs: ")
	}
	return LexIdentityOrValue
}

// Handle columnar identies with keyword appendate (ASC, DESC)
//
//     [ORDER BY] ( <identity> | <expr> ) [(ASC | DESC)]
//
func LexOrderByColumn(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}

	r := l.Peek()
	//u.Debugf("LexOrderBy  r= '%v'  %v", string(r), l.PeekX(10))

	switch r {
	case '`':
		l.Push("LexOrderByColumn", LexOrderByColumn)
		return LexIdentifier
	case ';':
		return nil
	case ',':
		l.Next()
		l.Emit(TokenComma)
		l.Push("LexOrderByColumn", LexOrderByColumn)
		return LexExpressionOrIdentity
	}

	word := strings.ToLower(l.PeekWord())
	//u.Debugf("word: %v", word)
	if l.isNextKeyword(word) {
		return nil
	}
	//u.Debugf("looking for operator:  word=%s", word)
	switch word {
	case "asc":
		l.ConsumeWord(word)
		l.Emit(TokenAsc)
		return LexOrderByColumn
	case "desc":
		l.ConsumeWord(word)
		l.Emit(TokenDesc)
		return LexOrderByColumn
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
			l.Push("entryStateFn", l.clauseState())
			return LexInlineComment
			//return nil
		}
	case ';':
		l.backup()
		return nil
	case ',':
		l.Emit(TokenComma)
		return l.clauseState()
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
		cs := strings.ToLower(l.PeekX(len("character set")))
		if cs == "character set" {
			l.ConsumeWord(cs)
			l.Emit(TokenCharacterSet)
			l.Push("LexDdlColumn", l.clauseState())
			return nil
		}

	// Below here are Data Types
	case "text":
		l.ConsumeWord(word)
		l.Emit(TokenText)
		return l.clauseState()
	case "bigint":
		l.ConsumeWord(word)
		l.Emit(TokenBigInt)
		return l.clauseState()
	case "varchar":
		l.ConsumeWord(word)
		l.Emit(TokenVarChar)
		l.Push("LexDdlColumn", l.clauseState())
		return LexListOfArgs

	default:
		r = l.Peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexDdlColumn", l.clauseState())
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
		l.Push("LexDdlColumn", l.clauseState())
	} else {
		u.Errorf("Gracefully refusing to add more LexDdlColumn: ")
	}
	return LexExpressionOrIdentity
}

// Lex either Json or Key/Value pairs
//
//    Must start with { or [ for json
//    Start with identity for key/value pairs
//
func LexJsonOrKeyValue(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	//u.Debugf("LexJsonOrKeyValue  '%v'  %v", string(r), l.PeekX(10))
	switch r {
	case '{', '[':
		return LexJsonValue
	case '=':
		return LexExpression
	}
	if l.isIdentity() {
		l.Push("LexExpression", LexExpression)
		return LexExpression
	}
	//u.Warnf("Did not find json? %v", l.PeekX(20))
	return nil
}

// Lex Valid Json
//
//    Must start with { or [
//
func LexJson(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	//u.Debugf("LexJson  '%v'  %v", string(r), l.PeekX(10))
	switch r {
	case '{', '[':
		return LexJsonValue
	}
	//u.Warnf("Did not find json? %v", l.PeekX(20))
	return nil
}

/*
	TokenLeftBracket  TokenType = 23 // [
	TokenRightBracket TokenType = 24 // ]
	TokenLeftBrace    TokenType = 25 // {
	TokenRightBrace   TokenType = 26 // }
*/

// LexJsonValue:  Consume values, first consuming Colon
//
//  <jsonvalue> ::= ':' ( <value>, <array>, <jsonobject> ) [, ...]
//
func LexJsonValue(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	//u.Debugf("LexJsonValue  '%v'  %v", string(r), l.PeekX(10))
	switch r {
	case '}', ']':
		return nil // recurse back up one level
	case '{': //<object>
		l.Next()
		l.Emit(TokenLeftBrace)
		l.Push("LexJsonObject", LexJsonObject)
		return LexJsonIdentity // Key's must be strings
	case '[': //<array>
		l.Next()
		l.Emit(TokenLeftBracket)
		return LexJsonArray
	case ',':
		u.Warnf("Should not be possible to get comma here?")
	default:
		return LexValue(l)
	}
	return nil
}

// Lex Valid Json Array
//
//    Must End with ]
//
func LexJsonArray(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	//u.Debugf("LexJsonArray  '%v' peek:%v", string(r), l.PeekX(10))
	switch r {
	case ']':
		l.Next()
		l.Emit(TokenRightBracket)
		return nil
	case ',':
		l.Next() // consume ,
		l.Emit(TokenComma)
		return LexJsonArray
	case '{':
		l.Next() // consume {
		l.Emit(TokenLeftBrace)
		l.Push("LexJsonArray", LexJsonArray)
		l.Push("LexJsonObject", LexJsonObject)
		return LexJsonIdentity // Key's must be strings
	case '[':
		l.Next() // consume [
		l.Emit(TokenLeftBracket)
		l.Push("LexJsonArray", LexJsonArray)
		return LexJsonArray
	default:
		// value
		//u.Debugf("call lex value: %v", l.PeekX(10))
		l.Push("LexJsonArray", LexJsonArray)
		return LexValue(l)
	}

	//u.Warnf("Did not find json? %v", l.PeekX(20))
	return nil
}

// Lex Valid Json Object
//
//    Must End with }
//
func LexJsonObject(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return nil
	}
	r := l.Peek()
	//u.Debugf("LexJsonObject  '%v'  %v", string(r), l.PeekX(10))
	switch r {
	case '}':
		l.Next()
		l.Emit(TokenRightBrace)
		return nil
	case ':':
		l.Next() // consume :
		l.Emit(TokenColon)
		l.Push("LexJsonObject", LexJsonObject)
		return LexJsonValue
	case ',':
		l.Next() // consume ,
		l.Emit(TokenComma)
		l.Push("LexJsonObject", LexJsonObject)
		return LexJsonIdentity
	case '{':
		l.Next() // consume {
		l.Emit(TokenLeftBrace)
		l.Push("LexJsonObject", LexJsonObject)
		l.Push("LexJsonObject", LexJsonObject)
		return LexJsonIdentity // Key's must be strings
	case '[':
		l.Next() // consume [
		l.Emit(TokenLeftBracket)
		l.Push("LexJsonObject", LexJsonObject)
		return LexJsonArray
	}

	u.Warnf("Did not find json? %v", l.PeekX(20))
	return nil
}

// lex a string value value:
//
//  strings must be quoted
//
//  "stuff"    -> stuff
//  "items's with quote"
//
func LexJsonIdentity(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	if l.IsEnd() {
		return l.errorToken("expected value but got EOF")
	}
	rune := l.Next()

	typ := TokenIdentity
	//u.Debugf("in LexJsonIdentity: %v", string(rune))
	// quoted string
	if rune == '\'' || rune == '"' {
		firstRune := rune
		l.ignore() // consume the quote mark
		previousEscaped := rune == '\\'
		for rune = l.Next(); ; rune = l.Next() {

			//u.Debugf("LexValue rune=%v  end?%v  prevEscape?%v", string(rune), rune == eof, previousEscaped)
			if (rune == '\'' || rune == '"') && rune == firstRune && !previousEscaped {
				if !l.IsEnd() {
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
	}
	return nil
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
	p2 := l.PeekX(2)
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
	p2 := l.PeekX(2)
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
	//u.Debugf("typ  %v   %v  %q", typ, ok, l.input[l.start:l.pos])
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
	peek2 := l.PeekX(2)
	peek2nd := byte(0)
	if len(peek2) == 2 {
		peek2nd = peek2[1]
	}
	//u.Debugf("scanNumericOrDuration?  '%v' peek:%v ", string(peek2), len(peek2))
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
				switch peek2nd {
				case ' ', '\t', '\n', ',', ')', ';', byte(0):
					return typ, true
				}
				// Integers can't start with 0??
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

// A break, is some character such as comma, ;, etc
func IsBreak(r rune) bool {
	switch r {
	case '\'', ',', ';', '"':
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

func IsIdentifierRune(r rune) bool {
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
func isIdentifierLaxRune(r rune) bool {
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

func isIdentifierFirstRune(r rune) bool {
	if r == '\'' {
		return false
	} else if isDigit(r) {
		// Digits can not lead identities
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

func isJsonStart(r rune) bool {
	if r == '{' || r == '[' {
		return true
	}
	return false
}

func IdentityRunesOnly(identity string) bool {
	for _, r := range identity {
		if !IsIdentifierRune(r) {
			return false
		}
	}
	return true
}
