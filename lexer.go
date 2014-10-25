package qlparse

import (
	"fmt"
	u "github.com/araddon/gou"
	"strings"
	"unicode"
	"unicode/utf8"
)

var _ = u.EMPTY

// Tokens ---------------------------------------------------------------------

// TokenType identifies the type of lexical tokens.
type TokenType uint16

// token represents a text string returned from the lexer.
type Token struct {
	T TokenType // type
	V string    // value
}

const (
	// List of all TokenTypes
	TokenNil           TokenType = iota // not used
	TokenEOF                            // EOF
	TokenError                          // error occurred; value is text of error
	TokenText                           // plain text
	TokenComment                        // Comment  // or --)
	TokenMLComment                      // Comment  (/* ... */ or // or --)
	TokenSingleComment                  // Single Line comment:   -- hello
	// Primitive literals.
	TokenBool
	TokenFloat
	TokenInteger
	TokenString
	TokenList
	TokenMap

	// Logical Evaluation/expression inputs
	TokenStar             // *
	TokenEqual            // =
	TokenNE               // !=
	TokenGE               // >=
	TokenLE               // <=
	TokenGT               // >
	TokenLT               // <
	TokenLeftParenthesis  // (
	TokenRightParenthesis // )
	TokenComma            // ,
	TokenLogicOr          // OR
	TokenLogicAnd         // AND
	TokenIN               // IN
	TokenLike             // LIKE
	TokenNegate           // NOT

	// ql types
	TokenEOS                  // ;
	TokenUdfExpr              // User defined function, or pass through to source
	TokenSqlTable             // table name
	TokenSqlColumn            // column name
	TokenSqlInsert            // insert
	TokenSqlInto              // into
	TokenSqlUpdate            // update
	TokenSqlSet               // set
	TokenSqlAs                // as
	TokenSqlDelete            // delete
	TokenSqlFrom              // from
	TokenSqlSelect            // select
	TokenSqlSkip              // skip
	TokenSqlWhere             // where
	TokenSqlGroupBy           // group by
	TokenSqlValues            // values
	TokenSqlValue             // 'some string' string or continous sequence of chars delimited by WHITE SPACE | ' | , | ( | )
	TokenValueWithSingleQuote // '' becomes ' inside the string, parser will need to replace the string
	TokenSqlKey               // key
	TokenSqlTag               // tag

)

var (
	// Which Identity Characters are allowed?
	IDENTITY_CHARS = "_."
	// A much more lax identity char set rule
	IDENTITY_LAX_CHARS = "_./ "

	// Are we going to Support Unbounded USER DEFINED FUNCTIONS?

	// list of token-name
	TokenNameMap = map[TokenType]string{
		TokenEOF:     "EOF",
		TokenError:   "Error",
		TokenComment: "Comment",
		TokenText:    "Error",
		// Primitive literals.
		TokenBool:    "Bool",
		TokenFloat:   "Float",
		TokenInteger: "Integer",
		TokenString:  "String",
		TokenList:    "List",
		TokenMap:     "Map",
		// Logic, Expressions, Commas etc
		TokenStar:             "Star",
		TokenEqual:            "Equal",
		TokenNE:               "NE",
		TokenGE:               "GE",
		TokenLE:               "LE",
		TokenGT:               "GT",
		TokenLT:               "LT",
		TokenLeftParenthesis:  "LeftParenthesis",
		TokenRightParenthesis: "RightParenthesis",
		TokenComma:            "Comma",
		TokenLogicOr:          "Or",
		TokenLogicAnd:         "And",
		TokenIN:               "IN",
		TokenLike:             "LIKE",
		TokenNegate:           "NOT",
		// Expression
		TokenUdfExpr: "EXPR",
		// QL Keywords
		TokenEOS:                  "EndOfStatement",
		TokenSqlTable:             "Table",
		TokenSqlColumn:            "Column",
		TokenSqlInsert:            "Insert",
		TokenSqlInto:              "Into",
		TokenSqlUpdate:            "Update",
		TokenSqlSet:               "Set",
		TokenSqlAs:                "As",
		TokenSqlDelete:            "Delete",
		TokenSqlFrom:              "From",
		TokenSqlSelect:            "Select",
		TokenSqlWhere:             "Where",
		TokenSqlGroupBy:           "Group By",
		TokenSqlValues:            "Values",
		TokenSqlValue:             "Value",
		TokenValueWithSingleQuote: "ValueWithSingleQuote",
	}
)

// convert to human readable string
func (typ TokenType) String() string {
	s, ok := TokenNameMap[typ]
	if ok {
		return s
	}
	return "not implemented"
}

// Token Emit/Writer

// tokenEmitter accepts tokens found by lexer and allows storage or channel emission
type tokenEmitter interface {
	Emit(t *Token)
}

type tokensStoreEmitter struct {
	idx    int
	tokens []*Token
}

// String converts tokensProducerConsumer to a string.
func (t tokensStoreEmitter) String() string {
	return fmt.Sprintf(
		"tokensProducerConsumer: idx=%d; tokens(%d)=%s",
		t.idx,
		len(t.tokens),
		t.tokens)
}

// Lexer ----------------------------------------------------------------------

const (
	eof        = -1
	leftDelim  = "{"
	rightDelim = "}"
	decDigits  = "0123456789"
	hexDigits  = "0123456789ABCDEF"
)

// StateFn represents the state of the lexer as a function that returns the
// next state.
type StateFn func(*Lexer) StateFn

// newLexer creates a new lexer for the input string.
//
// It is borrowed from the text/template package with minor changes.
func NewLexer(input string) *Lexer {
	// Two tokens of buffering is sufficient for all state functions.
	l := &Lexer{
		input: input,
		state: lexStatement,
		// 200 seems excesive, but since multiple Comments Can be found? before we reach
		// our token, this is needed?
		tokens: make(chan Token, 1000),
	}
	return l
}

// lexer holds the state of the lexical scanning.
//
// Based on the lexer from the "text/template" package.
// See http://www.youtube.com/watch?v=HxaD_trXwRE
type Lexer struct {
	input       string       // the string being scanned.
	state       StateFn      // the next lexing function to enter.
	pos         int          // current position in the input.
	start       int          // start position of this token.
	width       int          // width of last rune read from input.
	emitter     tokenEmitter // hm
	tokens      chan Token   // channel of scanned tokens.
	doubleDelim bool         // flag for tags starting with double braces.
}

// nextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	for {
		select {
		case token := <-l.tokens:
			return token
		default:
			if l.state == nil {
				u.Error("WTF, no state? ")
				panic("no state?")
			}
			l.state = l.state(l)
		}
	}
	panic("not reached")
}

// next returns the next rune in the input.
func (l *Lexer) next() (r rune) {
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
		l.next()
	}
}

// peek returns but does not consume the next rune in the input.
func (l *Lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// lets grab the next word (till whitespace, without consuming)
func (l *Lexer) peekX(x int) string {
	if l.pos+x > len(l.input) {
		return l.input[l.pos:]
	}
	return l.input[l.pos : l.pos+x]
}

// lets grab the next word (till whitespace, without consuming)
func (l *Lexer) peekWord() string {
	word := ""
	for i := 0; i < len(l.input)-l.pos; i++ {
		r, _ := utf8.DecodeRuneInString(l.input[l.pos+i:])
		if unicode.IsSpace(r) || !isAlNumOrPeriod(r) {
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
	return false
}

// emit passes an token back to the client.
func (l *Lexer) emit(t TokenType) {
	u.Infof("emit: %s  '%s'", t, l.input[l.start:l.pos])
	l.tokens <- Token{t, l.input[l.start:l.pos]}
	l.start = l.pos
}

// ignore skips over the pending input before this point.
func (l *Lexer) ignore() {
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
func (l *Lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *Lexer) acceptRun(valid string) bool {
	pos := l.pos
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
	return l.pos > pos
}

// Returns current lexeme string.
func (l *Lexer) current() string {
	str := l.input[l.start:l.pos]
	l.start = l.pos
	return str
}

// lets move position to word
func (l *Lexer) consumeWord(word string) {
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
	l.tokens <- Token{TokenError, fmt.Sprintf(format, args...)}
	return nil
}

// Skips white space characters in the input.
func (l *Lexer) skipWhiteSpaces() {
	for rune := l.next(); unicode.IsSpace(rune); rune = l.next() {
	}
	l.backup()
	l.ignore()
}

// Scans input and matches against the string.
// Returns true if the expected string was matched.
func (l *Lexer) match(str string, skip int) bool {
	done := true
	for _, rune := range str {
		if skip > 0 {
			skip--
			continue
		}

		n := l.next()
		//u.Debugf("rune=%s n=%s   %v  %v", string(rune), string(n), rune != n, unicode.ToUpper(rune) != n)
		if rune != n && unicode.ToLower(rune) != n {
			//u.debug("setting done = false?")
			done = false
		}
	}
	if !isWhiteSpace(l.peek()) {
		done = false
		//l.scanTillWhiteSpace()
	}
	return done
}

// Scans input and tries to match the expected string.
// Returns true if the expected string was matched.
// Does not advance the input if the string was not matched.
//
// NOTE:  this assumes the @val you are trying to match against is UPPER CASE
//      in which case it will also match lower case
//      if yo do not want caseless match then ??
func (l *Lexer) tryMatch(val string) bool {
	i := 0
	for _, rune := range val {
		i++
		n := l.next()
		if rune != n && unicode.ToLower(rune) != n {
			for ; i > 0; i-- {
				l.backup()
			}
			return false
		}
	}
	return true
}

// Emits an error token and terminates the scan
// by passing back a nil ponter that will be the next state
// terminating lexer.next function
func (l *Lexer) errorToken(format string, args ...interface{}) StateFn {
	//fmt.Sprintf(format, args...)
	l.emit(TokenError)
	return nil
}

// lexValue looks in input for a sql value, then emits token on
// success and then returns passed in next state func
func (l *Lexer) lexValue(fn StateFn) StateFn {
	l.skipWhiteSpaces()
	if l.isEnd() {
		return l.errorToken("expected value but got EOF")
	}
	rune := l.next()
	typ := TokenSqlValue

	// quoted string
	if rune == '\'' {
		l.ignore()
		for rune = l.next(); ; rune = l.next() {
			if rune == '\'' {
				if !l.isEnd() {
					rune = l.next()
					// check for '''
					if rune == '\'' {
						typ = TokenValueWithSingleQuote
					} else {
						// since we read lookahead after single quote that ends the string
						// for lookahead
						l.backup()
						// for single quote which is not part of the value
						l.backup()
						l.emit(typ)
						// now ignore that single quote
						l.next()
						l.ignore()
						//
						return fn
					}
				} else {
					// at the very end
					l.backup()
					l.emit(typ)
					l.next()
					return fn
				}
			}
			if rune == 0 {
				return l.errorToken("string value was not delimited")
			}
		}
		// value
	} else {
		for rune = l.next(); !isWhiteSpace(rune) && rune != ',' && rune != ')'; rune = l.next() {
		}
		l.backup()
		l.emit(typ)
		return fn
	}
	return nil
}

// non-consuming isExpression
func (l *Lexer) isExpr() bool {
	// Expressions are strings not values
	if r := l.peek(); r == '\'' {
		return false
	} else if isDigit(r) {
		return false
	}
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

// non-consuming isIdentity
//  Identities are non-numeric string values that are not quoted
func (l *Lexer) isIdentity() bool {
	// Identity are strings not values
	r := l.peek()
	if r == '\'' {
		return false
	} else if isDigit(r) {
		return false
	} else if isAlpha(r) {
		return true
	}
	return false
}

// // lexLogicalExpr
// func (l *Lexer) lexLogicalExpr(hadLogical, notFoundFn StateFn) StateFn {
// 	u.Debugf("lexLogicalExpr   ")
// 	// if l.match(value, 0) {
// 	// 	l.emit(typ)
// 	// 	return fn
// 	// }
// 	switch {
// 	case l.match("OR", 0):
// 		//l.emit(TokenLeftParenthesis)
// 		u.Error("Found OR, not yet supported")
// 		l.emit(TokenLogicOr)
// 		return hadLogical
// 	case l.match("AND", 0):
// 		//l.emit(TokenLeftParenthesis)
// 		u.Error("Found OR, not yet supported")
// 		l.emit(TokenLogicOr)
// 		return hadLogical
// 	default:
// 		u.Infof("Did not find right expression ?  ")
// 	}
// 	return l.errorToken("Unexpected token:" + l.current())
// }

// lexMatch matches expected string value emitting the token on success
// and returning passed state function.
func (l *Lexer) lexMatch(typ TokenType, value string, skip int, fn StateFn) StateFn {
	u.Debugf("lexMatch   t=%s   v=%s peek=%s", typ.String(), value, l.peekWord())
	if l.match(value, skip) {
		u.Debugf("found match: %s   %v", value, fn)
		l.emit(typ)
		return fn
	}
	u.Error("unexpected token", value)
	return l.errorToken("Unexpected token:" + l.current())
}

// lexer to match expected value returns with args of
//   @matchState state function if match
//   @noMatchState state function if no match
func (l *Lexer) lexIfElseMatch(typ TokenType, val string, matchState StateFn, noMatchState StateFn) StateFn {
	l.skipWhiteSpaces()
	if l.tryMatch(val) {
		l.emit(typ)
		return matchState
	}
	return noMatchState
}

// State functions ------------------------------------------------------------

// look for either an Expression or Item, or next
func lexRepeatExprItem(l *Lexer, current, nextFn StateFn) StateFn {

	for {
		l.skipWhiteSpaces()
		if r := l.peek(); r == ')' {
			l.next()
			l.emit(TokenRightParenthesis)
			u.Debug("in expr 1 )")
			l.skipWhiteSpaces()
			return nextFn(l)
		}
		if l.isExpr() {
			word := l.peekWord()
			u.Debugf("repeat expr: %s", word)
			l.consumeWord(word)
			l.emit(TokenUdfExpr)
			l.next()
			l.emit(TokenLeftParenthesis)
			lexRepeatExprItem(l, current, func(l *Lexer) StateFn {
				return nil
			})
		} else if l.isIdentity() {
			u.Warnf("in expr is identity? %s", l.peekWord())
			//current(l)
			//return nextFn(l)
			l.lexSqlIdentifier(TokenSqlColumn, nil)
		} else {
			l.lexValue(nil)
			//u.Debugf("repeat non-word VALUE?: %s", string(l.peekWord()))
			// lexRepeatExprItem(l, current, func(l *Lexer) StateFn {
			// 	u.Debugf("in inner expr")
			// 	return nil
			// })
		}
		l.skipWhiteSpaces()
		r := l.next()
		u.Debugf("next = %s", string(r))
		switch r {
		case ',':
			l.emit(TokenComma)
			u.Debug("in expr ,")
			//lexRepeatExprItem(l, current, nextFn)
		case ')':
			//panic("unreachable?")
			l.emit(TokenRightParenthesis)
			u.Error("in expr ) ")
			l.skipWhiteSpaces()
			return nextFn(l)
		default:
			// This is?  value?
			l.backup()
			u.Warnf("in lexRepeat? %s : %s", string(r), l.peekX(8))

			return nextFn(l)
		}
	}

	panic("unreachable")
	return nextFn
}

// look for list of comma value expressions
//   (val1,val2,val3)
//   (val1,'val2',a)
func lexCommaValues(l *Lexer, nextFn StateFn) StateFn {

	for {
		l.skipWhiteSpaces()
		r := l.next()
		u.Debugf("commavalues: %s", string(r))
		switch r {
		case '\'':
			l.backup()
			l.lexValue(nil)
			continue
		case ',':
			l.emit(TokenComma)
			l.lexValue(nil)
			continue
		case ')':
			//l.backup()
			l.emit(TokenRightParenthesis)
			//l.skipWhiteSpaces()
			return nextFn(l)
		case '(':
			l.emit(TokenLeftParenthesis)
			//return nil
		default:
			// ??
			l.backup()
			u.Debugf("what is this?  %s  %s", string(r), l.peekWord())
			return nextFn(l)
		}

	}

	panic("unreachable?")
	return nil
}

// lexStatement scans until finding an opening keyword
//   or comment, skipping whitespace
func lexStatement(l *Lexer) StateFn {

findComments:
	for {
		l.skipWhiteSpaces()
		// ensure we have consumed all comments
		r := l.peek()
		u.Debugf("peek: %s", string(r))
		switch r {
		case '/', '-': // comments
			u.Debugf("found comment?   %s", string(r))
			lexComment(l)
			u.Infof("After lex comment?")
		default:
			break findComments
		}
	}

	l.skipWhiteSpaces()

	u.Debugf("lexStatement? %s", string(l.peek()))
	switch l.peek() {
	case 's', 'S': // select
		u.Debugf("found select?? %s", string(l.peek()))
		return l.lexMatch(TokenSqlSelect, "SELECT", 0, lexSqlSelectColumn)
	// case 'i': // insert
	// 	return l.lexMatch(tokenTypeSqlInsert, "INSERT", 1, lexSqlInsertInto)
	// case 'd': // delete
	// 	return l.lexMatch(tokenTypeSqlDelete, "DELETE", 1, lexSqlFrom)
	// 	return lexCommandP(l)
	default:
		u.Errorf("not found?    r=%s", string(l.peek()))
	}

	// Correctly reached EOF.
	if l.pos > l.start {
		l.emit(TokenText)
	}
	l.emit(TokenEOF)
	return nil
}

//  SQL Keywords

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

// lexSqlIndentifier scans and finds named things (tables, columns)
// finding valid sql identifier, emits the token and returns the nextState()
//   it may optionally (config?) use a variety of escaping/quoting techniques
//
//  [name]
//  'name'
//
func (l *Lexer) lexSqlIdentifier(typ TokenType, nextState StateFn) StateFn {
	l.skipWhiteSpaces()

	wasQouted := false
	// first rune has to be valid unicode letter
	firstChar := l.next()
	u.Debugf("lexSqlIdentifier:   %v  %s %v", firstChar, string(firstChar), firstChar == '\'')
	switch firstChar {
	case '[', '\'':
		l.ignore()
		nextChar := l.next()
		if !unicode.IsLetter(nextChar) {
			return l.errorToken("identifier must begin with a letter " + l.input[l.start:l.pos])
		}
		for nextChar = l.next(); isLaxIdentifierRune(nextChar); nextChar = l.next() {

		}
		// iterate until we find non-identifier, then make sure
		if firstChar == '[' && nextChar == ']' {
			// valid
		} else if firstChar == '\'' && nextChar == '\'' {
			// also valid
		} else {
			return l.errorToken("unexpected character in identifier:  " + string(nextChar))
		}
		wasQouted = true
		l.backup()
		u.Debugf("quoted?:   %v  ", l.input[l.start:l.pos])
	default:
		if !unicode.IsLetter(firstChar) {
			return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
		}
		for rune := l.next(); isIdentifierRune(rune); rune = l.next() {
			// iterate until we find non-identifer character
		}
		l.backup()
	}

	l.emit(typ)
	if wasQouted {
		// need to skip last character bc it was quoted
		l.next()
		l.ignore()
	}

	l.skipWhiteSpaces()
	word := l.peekWord()
	if strings.ToUpper(word) == "AS" {
		l.skipX(2)
		l.emit(TokenSqlAs)
		return l.lexSqlIdentifier(TokenSqlColumn, nextState)
	}
	return nextState
}

func lexSqlFrom(l *Lexer) StateFn {
	l.skipWhiteSpaces()
	u.Debug("lexSqlFrom")
	return l.lexMatch(TokenSqlFrom, "FROM", 0, lexSqlFromTable)
}

func lexSqlFromTable(l *Lexer) StateFn {
	u.Debug("in lex sql from table")
	u.Debugf("lexSqlFromTable:  %s", l.peekWord())
	return l.lexSqlIdentifier(TokenSqlTable, lexSqlWhere)
	//return l.lexSqlIdentifier(tokenSqlTable, lexSqlWhere)
}

func lexSqlEndOfStatement(l *Lexer) StateFn {
	l.skipWhiteSpaces()
	r := l.next()
	u.Debugf("sqlend of statement  %s", string(r))
	if r == ';' {
		l.emit(TokenEOS)
	}
	if l.isEnd() {
		return nil
	}
	return l.errorToken("Unexpected token:" + l.current())
}

func lexSqlWhere(l *Lexer) StateFn {
	u.Debugf("in lexSqlWhere")
	return l.lexIfElseMatch(TokenSqlWhere, "WHERE", lexSqlWhereColumn, lexGroupBy)
}

func lexSqlWhereColumn(l *Lexer) StateFn {
	return l.lexSqlIdentifier(TokenSqlColumn, lexSqlWhereColumnExpr)
}

// This covers the
func lexSqlWhereCommaOrLogicOrNext(l *Lexer) StateFn {
	l.skipWhiteSpaces()

	r := l.next()
	switch r {
	case '(':
		l.emit(TokenLeftParenthesis)
		l.skipWhiteSpaces()
		u.Error("Found ( parenthesis")
	case ')':
		l.emit(TokenRightParenthesis)
		l.skipWhiteSpaces()
		u.Error("Found ) parenthesis")
	case ',':
		l.emit(TokenComma)
		return lexSqlWhereColumn
	default:
		l.backup()
	}

	word := l.peekWord()
	word = strings.ToUpper(word)
	u.Debugf("sqlcommaorlogic:  word=%s", word)
	switch word {
	case "":
		return lexGroupBy
	case "OR": // OR
		l.skipX(2)
		l.emit(TokenLogicOr)
		return lexSqlWhereColumn
	case "AND": // AND
		l.skipX(3)
		l.backup()
		l.emit(TokenLogicAnd)
		return lexSqlWhereColumn
	default:
		u.Infof("Did not find right expression ?  %s len=%d", word, len(word))
		l.backup()
	}

	// Since we do not have another Where expr, then go to next
	return lexGroupBy
}

// Handles within a Where Clause the Start of an expression, when in a WHERE
//  (X = y OR a = b)
func lexSqlWhereColumnExpr(l *Lexer) StateFn {
	u.Debug("lexSqlWhereColumnExpr")
	l.skipWhiteSpaces()
	r := l.next()
	switch r {
	case '!':
		if r2 := l.peek(); r2 == '=' {
			l.next()
			l.emit(TokenNE)
		} else {
			u.Error("Found ! without equal")
			return nil
		}
	case '=':
		l.emit(TokenEqual)
	case '>':
		if r2 := l.peek(); r2 == '=' {
			l.next()
			l.emit(TokenGE)
		}
		l.emit(TokenGT)
	case '<':
		if r2 := l.peek(); r2 == '=' {
			l.next()
			l.emit(TokenLE)
		}
	default:
		l.backup()
		word := l.peekWord()
		word = strings.ToUpper(word)
		u.Debugf("looking for operator:  word=%s", word)
		switch word {
		case "IN": // IN
			l.skipX(2)
			l.emit(TokenIN)
			return lexCommaValues(l, func(l *Lexer) StateFn {
				u.Debug("in IN lex return?")
				return lexSqlWhereCommaOrLogicOrNext
			})
		case "LIKE": // LIKE
			l.skipX(4)
			l.emit(TokenLike)
			return l.lexValue(lexSqlWhereCommaOrLogicOrNext)
		default:
			u.Infof("Did not find right expression ?  %s", word)
		}
	}
	return l.lexValue(lexSqlWhereCommaOrLogicOrNext)
}

func lexGroupBy(l *Lexer) StateFn {
	return l.lexIfElseMatch(TokenSqlGroupBy, "GROUP BY", lexSqlGroupByColumns, lexSqlEndOfStatement)
}

func lexSqlGroupByColumns(l *Lexer) StateFn {
	lexRepeatExprItem(l, func(l *Lexer) StateFn { return nil }, lexSqlEndOfStatement)
	return nil
}

//   SELECT [    ] FROM
func lexSqlSelectColumn(l *Lexer) StateFn {
	l.skipWhiteSpaces()
	fn := func(l *Lexer) StateFn {
		u.Debugf("inside lex sql select from")
		return lexRepeatExprItem(l,
			func(l *Lexer) StateFn {
				return func(l *Lexer) StateFn {
					u.Warn("should not be here")
					return nil
				}
			},
			lexSqlSelectColumnCommaOrFrom)
	}
	u.Debug("lexSqlSelectColumn")
	return fn
	//return l.lexSqlIdentifier(TokenSqlColumn, lexSqlSelectColumnCommaOrFrom)
}

func lexSqlSelectColumnCommaOrFrom(l *Lexer) StateFn {
	l.skipWhiteSpaces()
	u.Debugf("in LexSqlCol")
	if l.next() == ',' {
		l.emit(TokenComma)
		return lexSqlSelectColumn
	}
	l.backup()
	return lexSqlFrom(l)
}

func lexSqlSelectStar(l *Lexer) StateFn {
	l.skipWhiteSpaces()
	if l.next() == '*' {
		l.emit(TokenStar)
		return lexSqlFrom
	}
	l.backup()
	return lexSqlSelectColumn(l)
}

// lexComment looks for valid comments
func lexComment(l *Lexer) StateFn {
	u.Debugf("checking comment: '%s' ", l.input[l.pos:l.pos+2])
	if strings.HasPrefix(l.input[l.pos:], "/*") {
		return lexMultilineCmt(l)
	} else if strings.HasPrefix(l.input[l.pos:], "//") {
		return lexSingleLineCmt(l)
	} else if strings.HasPrefix(l.input[l.pos:], "--") {
		u.Debugf("found single line comment:   ")
		return lexSingleLineCmt(l)
	}
	u.Warn("What, no comment after all?")
	return nil
}

func lexMultilineCmt(l *Lexer) StateFn {
	// Consume opening "/*"
	l.next()
	l.next()
	for {
		if strings.HasPrefix(l.input[l.pos:], "*/") {
			break
		}
		r := l.next()
		if eof == r {
			panic("Unexpected end of file inside multiline comment")
		}
	}
	// Consume trailing "*/"
	l.next()
	l.next()
	l.emit(TokenComment)

	return nil
}

func lexSingleLineCmt(l *Lexer) StateFn {
	// Consume opening "//" or --
	l.next()
	l.next()
	for {
		r := l.next()
		if r == '\n' || r == eof {
			l.backup()
			break
		}
	}
	l.emit(TokenComment)
	return nil
}

// lexNumber scans a number: a float or integer (which can be decimal or hex).
func lexNumber(l *Lexer) StateFn {
	typ, ok := scanNumber(l)
	if !ok {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	// Emits tokenFloat or tokenInteger.
	l.emit(typ)
	return nil
}

// scan for a number
//
// It returns the scanned tokenType (tokenFloat or tokenInteger) and a flag
// indicating if an error was found.
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
//     - hexadecimal (must begin with 0x and must use capital A-F,
//       e.g. 0x1A2B).
func scanNumber(l *Lexer) (typ TokenType, ok bool) {
	typ = TokenInteger
	// Optional leading sign.
	hasSign := l.accept("+-")
	if l.input[l.pos:l.pos+2] == "0x" {
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
		// Decimal.
		if !l.acceptRun(decDigits) {
			// Requires at least one digit.
			return
		}
		if l.accept(".") {
			// Float.
			if !l.acceptRun(decDigits) {
				// Requires a digit after the dot.
				return
			}
			typ = TokenFloat
		} else {
			if (!hasSign && l.input[l.start] == '0') ||
				(hasSign && l.input[l.start+1] == '0') {
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
	// Next thing must not be alphanumeric.
	if isAlNum(l.peek()) {
		l.next()
		return
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
