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
type TokenInfo struct {
	T           TokenType
	Kw          string
	firstWord   string // in event multi-word (Group By) the first word for match
	HasSpaces   bool
	Description string
}

// token represents a text string returned from the lexer.
type Token struct {
	T TokenType // type
	V string    // value
}

// convert to human readable string
func (t Token) String() string {
	return fmt.Sprintf(`Token{Type:"%v" Value:"%v"}`, t.T.String(), t.V)
}

const (
	// List of all TokenTypes
	TokenNil               TokenType = iota // not used
	TokenEOF                                // EOF
	TokenError                              // error occurred; value is text of error
	TokenText                               // plain text
	TokenComment                            // Comment value string
	TokenCommentML                          // Comment MultiValue
	TokenCommentStart                       // /*
	TokenCommentEnd                         // */
	TokenCommentSingleLine                  // Single Line comment:   -- hello
	TokenCommentHash                        // Single Line comment:  # hello
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
	TokenIdentity             // identity, either column, table name etc
	TokenInsert               // insert
	TokenInto                 // into
	TokenUpdate               // update
	TokenSet                  // set
	TokenAs                   // as
	TokenDelete               // delete
	TokenFrom                 // from
	TokenSelect               // select
	TokenSkip                 // skip
	TokenWhere                // where
	TokenGroupBy              // group by
	TokenBy                   // by
	TokenAlias                // alias
	TokenWith                 // with
	TokenValues               // values
	TokenValue                // 'some string' string or continous sequence of chars delimited by WHITE SPACE | ' | , | ( | )
	TokenValueWithSingleQuote // '' becomes ' inside the string, parser will need to replace the string
	TokenKey                  // key
	TokenTag                  // tag
)

var (
	// Which Identity Characters are allowed?
	//    if we allow forward slashes (weird?) then we allow xpath esque notation
	IDENTITY_CHARS = "_./"
	// A much more lax identity char set rule
	IDENTITY_LAX_CHARS = "_./ "

	// list of token-name
	TokenNameMap = map[TokenType]*TokenInfo{
		TokenEOF:               {Description: "EOF"},
		TokenError:             {Description: "Error"},
		TokenComment:           {Description: "Comment"},
		TokenCommentML:         {Description: "Comment"},
		TokenCommentStart:      {Description: "/*"},
		TokenCommentEnd:        {Description: "*/"},
		TokenCommentHash:       {Description: "#"},
		TokenCommentSingleLine: {Description: "--"},
		TokenText:              {Description: "Text"},
		// Primitive literals.
		TokenBool:    {Description: "Bool"},
		TokenFloat:   {Description: "Float"},
		TokenInteger: {Description: "Integer"},
		TokenString:  {Description: "String"},
		TokenList:    {Description: "List"},
		TokenMap:     {Description: "Map"},
		// Logic, Expressions, Commas etc
		TokenStar:             {Kw: "*", Description: "Star"},
		TokenEqual:            {Kw: "=", Description: "Equal"},
		TokenNE:               {Kw: "!=", Description: "NE"},
		TokenGE:               {Kw: ">=", Description: "GE"},
		TokenLE:               {Kw: "<=", Description: "LE"},
		TokenGT:               {Kw: ">", Description: "GT"},
		TokenLT:               {Kw: "<", Description: "LT"},
		TokenLeftParenthesis:  {Description: "("},
		TokenRightParenthesis: {Description: ")"},
		TokenComma:            {Description: ","},
		TokenLogicOr:          {Kw: "or", Description: "Or"},
		TokenLogicAnd:         {Kw: "and", Description: "And"},
		TokenIN:               {Kw: "in", Description: "IN"},
		TokenLike:             {Kw: "like", Description: "LIKE"},
		TokenNegate:           {Kw: "not", Description: "NOT"},
		// Expression Identifier
		TokenUdfExpr: {Description: "EXPR"},
		// values
		TokenValues:               {Description: "values"},
		TokenValue:                {Description: "value"},
		TokenValueWithSingleQuote: {Description: "valueWithSingleQuote"},
		// QL Keywords, all lower-case
		TokenEOS:      {Description: ";"},
		TokenIdentity: {Description: "identity"},
		TokenInsert:   {Description: "insert"},
		TokenInto:     {Description: "into"},
		TokenUpdate:   {Description: "update"},
		TokenSet:      {Description: "set"},
		TokenAs:       {Description: "as"},
		TokenBy:       {Description: "by"},
		TokenDelete:   {Description: "delete"},
		TokenFrom:     {Description: "from"},
		TokenSelect:   {Description: "select"},
		TokenWhere:    {Description: "where"},
		TokenGroupBy:  {Description: "group by"},
		TokenAlias:    {Description: "alias"},
		TokenWith:     {Description: "with"},
	}
)

func init() {
	LoadTokenInfo()
}

func LoadTokenInfo() {
	for tok, ti := range TokenNameMap {
		ti.T = tok
		if ti.Kw == "" {
			ti.Kw = ti.Description
		}
		if strings.Contains(ti.Kw, " ") {
			parts := strings.Split(ti.Kw, " ")
			ti.firstWord = parts[0]
			ti.HasSpaces = true
		}
	}
}

// convert to human readable string
func (typ TokenType) String() string {
	s, ok := TokenNameMap[typ]
	if ok {
		return s.Kw
	}
	return "not implemented"
}

// which keyword should we look for, either full keyword
// OR in case of spaces such as "group by" look for group
func (typ TokenType) MatchString() string {
	tokInfo, ok := TokenNameMap[typ]
	//u.Debugf("matchstring: '%v' '%v'  '%v'", tokInfo.T, tokInfo.Kw, tokInfo.Description)
	if ok {
		if tokInfo.HasSpaces {
			return tokInfo.firstWord
		}
		return tokInfo.Kw
	}
	return "not implemented"
}

// is this a word such as "Group by" with multiple words?
func (typ TokenType) MultiWord() bool {
	tokInfo, ok := TokenNameMap[typ]
	if ok {
		return tokInfo.HasSpaces
	}
	return false
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

type NamedStateFn struct {
	Name    string
	StateFn StateFn
}

// newLexer creates a new lexer for the input string.
//
func NewLexer(input string, dialect *Dialect) *Lexer {
	// Two tokens of buffering is sufficient for all state functions.
	l := &Lexer{
		input:   input,
		state:   LexDialect,
		tokens:  make(chan Token, 1),
		stack:   make([]NamedStateFn, 0, 10),
		dialect: dialect,
	}
	return l
}

// newLexer creates a new lexer for the input string
//  this is sql(ish) compatible parser
//
func NewSqlLexer(input string) *Lexer {
	// Two tokens of buffering is sufficient for all state functions.
	l := &Lexer{
		input:   input,
		state:   LexDialect,
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
	input        string       // the string being scanned.
	state        StateFn      // the next lexing function to enter
	keywordEntry StateFn      // The current clause StateFn
	pos          int          // current position in the input.
	start        int          // start position of this token.
	width        int          // width of last rune read from input.
	emitter      tokenEmitter // hm
	tokens       chan Token   // channel of scanned tokens.
	doubleDelim  bool         // flag for tags starting with double braces.
	dialect      *Dialect
	dialectPos   int

	// Due to nested Expressions and evaluation this allows us to descend/ascend
	// during lex, using push/pop to add and remove states needing evaluation
	stack []NamedStateFn
}

// nextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	if l.start == 0 {
		l.dialect.init()
		u.Debugf("%v", l.input)
	}

	for {
		//u.Debugf("token: start=%v  pos=%v  peek5=%s", l.start, l.pos, l.peekX(5))
		select {
		case token := <-l.tokens:
			return token
		default:
			if l.state == nil && len(l.stack) > 0 {
				l.state = l.pop()
			} else if l.state == nil {
				u.Error("no state? ")
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
	u.Infof("pushed item onto stack: %v  %v", name, len(l.stack))
	l.stack = append(l.stack, NamedStateFn{name, state})
}

func (l *Lexer) pop() StateFn {
	if len(l.stack) == 0 {
		return l.errorf("BUG in lexer: no states to pop.")
	}
	li := len(l.stack) - 1
	last := l.stack[li]
	l.stack = l.stack[0:li]
	u.Infof("popped item off stack:  %v", last.Name)
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
func (l *Lexer) peek() rune {
	r := l.Next()
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
func (l *Lexer) PeekWord() string {
	word := ""
	for i := 0; i < len(l.input)-l.pos; i++ {
		r, _ := utf8.DecodeRuneInString(l.input[l.pos+i:])
		if unicode.IsSpace(r) || !isIdentifierRune(r) {
			return word
		} else {
			word = word + string(r)
		}
	}
	return word
}

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
	return false
}

// emit passes an token back to the client.
func (l *Lexer) Emit(t TokenType) {
	u.Debugf("emit: %s  '%s'", t, l.input[l.start:l.pos])
	l.tokens <- Token{t, l.input[l.start:l.pos]}
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

// Returns current lexeme string.
func (l *Lexer) current() string {
	str := l.input[l.start:l.pos]
	l.start = l.pos
	return str
}

// lets move position to word
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
	l.tokens <- Token{TokenError, fmt.Sprintf(format, args...)}
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
	if !isWhiteSpace(l.peek()) {
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

// non-consuming isExpression
func (l *Lexer) isExpr() bool {
	// Expressions are strings not values
	if r := l.peek(); r == '\'' {
		return false
	} else if isDigit(r) {
		return false
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

	//u.Infof("isNextKeyword?  %s   pos:%v len:%v", peekWord, l.dialectPos, len(l.dialect.Clauses))
	var clause *Clause
	for i := l.dialectPos; i < len(l.dialect.Clauses); i++ {
		clause = l.dialect.Clauses[i]
		//u.Debugf("clause next keyword?    peek=%s  keyword=%v multi?%v", peekWord, clause.keyword, clause.multiWord)
		if clause.keyword == peekWord || (clause.multiWord && strings.ToLower(l.peekX(len(clause.keyword))) == clause.keyword) {
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

// matches expected tokentype emitting the token on success
// and returning passed state function.
func (l *Lexer) lexMatch(tok TokenType, skip int, fn StateFn) StateFn {
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

// State functions ------------------------------------------------------------

// lexValue is the main entrypoint to lex Keywords, and sub-clauses
//  it expects a Dialect which gives info on the keywords
func LexDialect(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	r := l.peek()

	switch r {
	case '/', '-', '#':
		// ensure we have consumed all comments
		return LexComment(l, LexDialect)
	default:
		var clause *Clause
		peekWord := strings.ToLower(l.PeekWord())
		for i := l.dialectPos; i < len(l.dialect.Clauses); i++ {
			if l.isEnd() {
				break
			}
			clause = l.dialect.Clauses[i]
			// we only ever consume each clause once
			l.dialectPos++
			//u.Debugf("clause parser?  i?%v pos?%v  peek=%s  keyword=%v multi?%v", i, l.dialectPos, peekWord, clause.keyword, clause.multiWord)
			if clause.keyword == peekWord || (clause.multiWord && strings.ToLower(l.peekX(len(clause.keyword))) == clause.keyword) {

				// Set the default entry point for this keyword
				l.keywordEntry = clause.Lexer

				//u.Infof("dialect clause:  '%v' last?%v", clause.keyword, len(l.dialect.Clauses) == l.dialectPos)
				l.Push("lexDialect", LexDialect)
				if clause.Optional {
					return l.lexIfMatch(clause.Token, clause.Lexer)
				}

				return l.lexMatch(clause.Token, 0, clause.Lexer)
			}

		}
		// If we have consumed all clauses, we are ready to be done?
		u.Debugf("not found? word? %s  done?%v", peekWord, len(l.dialect.Clauses) == l.dialectPos)
		if l.dialectPos == len(l.dialect.Clauses) {
			return LexEndOfStatement
		}

	}

	// Correctly reached EOF.
	if l.pos > l.start {
		l.Emit(TokenText)
	}
	l.Emit(TokenEOF)
	return nil
}

// look for value
//
//  "stuff"
//  'stuff'
//  1.23
//
func LexValue(l *Lexer) StateFn {
	//u.Debugf("in LexValue: ")
	l.SkipWhiteSpaces()
	if l.isEnd() {
		return l.errorToken("expected value but got EOF")
	}
	rune := l.Next()
	typ := TokenValue
	if rune == ')' {
		// Whoops
		u.Warnf("why did we get paren? ")
		return nil
	}

	// quoted string
	if rune == '\'' || rune == '"' {
		l.ignore() // consume the quote mark
		for rune = l.Next(); ; rune = l.Next() {
			//u.Debugf("LexValue rune=%v  end?%v", string(rune), rune == eof)
			if rune == '\'' || rune == '"' {
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
		}
	} else {
		// Non-Quoted String?   Isn't this an identity?
		for rune = l.Next(); !isWhiteSpace(rune) && rune != ',' && rune != ')'; rune = l.Next() {
		}
		l.backup()
		l.Emit(typ)
	}
	return nil
}

// look for either an Expression or Identity
//
//  expressions:    Legal identity characters, terminated by (
//  identity:    legal identity characters
//
//  REPLACE(name,"stuff")       //  Expression
//  name
//
func LexExpressionOrIdentity(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	//peek := l.PeekWord()
	//peekChar := l.peek()
	//u.Debugf("in LexExpressionOrIdentity %v:%v", string(peekChar), string(peek))
	// Expressions end in Parens:     LOWER(item)
	if l.isExpr() {
		return lexExpressionIdentifier(l)
	} else if l.isIdentity() {
		// Non Expressions are Identities, or Columns
		// u.Warnf("in expr is identity? %s", l.PeekWord())
		// by passing nil here, we are going to go back to Pull items off stack)
		return lexIdentifier(l)
	} else {
		//u.Warnf("LexExpressionOrIdentity ??? '%v'", peek)
		return LexValue(l)
	}

	return nil
}

// lex Expression looks for an expression, identified by parenthesis
//
//    dostuff(name,"arg")    // the left parenthesis identifies it as Expression
func LexExpression(l *Lexer) StateFn {

	// first rune has to be valid unicode letter
	firstChar := l.Next()
	//u.Debugf("LexExpression:  %v", string(firstChar))
	if firstChar != '(' {
		u.Errorf("bad expression? %v", string(firstChar))
		return l.errorToken("expression must begin with a paren: ( " + string(l.input[l.start:l.pos]))
	}
	l.Emit(TokenLeftParenthesis)
	u.Infof("LexExpression:   %v", string(firstChar))
	return LexListOfArgs
}

// lex expression identity keyword
func lexExpressionIdentifier(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	// first rune has to be valid unicode letter
	firstChar := l.Next()
	if !unicode.IsLetter(firstChar) {
		u.Warnf("lexExpressionIdentifier couldnt find expression idenity?  %v stack=%v", string(firstChar), len(l.stack))
		return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
	}
	// Now look for run of runes, where run is ended by first non-identifier character
	for rune := l.Next(); isIdentifierRune(rune); rune = l.Next() {
		// iterate until we find non-identifer character
	}
	// TODO:  validate identity vs next keyword?, ie ensure it is not a keyword/reserved word

	l.backup() // back up one character
	l.Emit(TokenUdfExpr)
	return LexExpression
}

//  list of arguments, comma seperated list of args which may be a mixture
//   of expressions, identities, values
//
//       REPLACE(LOWER(x),"xyz")
//       REPLACE(x,"xyz")
//       COUNT(*) AS ct_stuff
//       IN (a,b,c)
//
func LexListOfArgs(l *Lexer) StateFn {

	// as we descend into Expressions, we are going to use push/pop to
	//  add future evaluation after we have descended
	l.SkipWhiteSpaces()

	r := l.Next()
	u.Debugf("in LexListOfArgs:  '%s'", string(r))

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
		l.Emit(TokenStar)
		return nil
	case ';':
		l.backup()
		return nil
	default:
		// So, not comma, * so either is Expression, Identity, Value
		l.backup()
		peekWord := strings.ToLower(l.PeekWord())
		//u.Debugf("in LexListOfArgs:  '%s'", peekWord)
		if l.isNextKeyword(peekWord) {
			u.Warnf("found keyword while looking for arg? %v", string(r))
			return nil
		}

		u.Debugf("LexListOfArgs sending LexExpressionOrIdentity: %v", string(peekWord))
		l.Push("LexListOfArgs", LexListOfArgs)
		return LexExpressionOrIdentity
	}

	u.Warnf("exit LexListOfArgs")
	return nil
}

// lexIdentifier scans and finds named things (tables, columns)
//  supports quoted, bracket, or raw identifiers
//
//   TODO: dialect controls escaping/quoting techniques
//
//  [name]         select [first name] from usertable;
//  'name'         select 'user' from usertable;
//  first_name     select first_name from usertable;
//  usertable      select first_name AS fname from usertable;
//
func lexIdentifier(l *Lexer) StateFn {

	l.SkipWhiteSpaces()

	wasQouted := false
	// first rune has to be valid unicode letter
	firstChar := l.Next()
	//u.Debugf("lexIdentifier:   %s is='? %v", string(firstChar), firstChar == '\'')
	//u.LogTracef(u.INFO, "lexIdentifier: %v", string(firstChar))
	switch firstChar {
	case '[', '\'':
		// Fields can be bracket or single quote escaped
		//  [user]
		//  [email]
		//  'email'
		l.ignore()
		nextChar := l.Next()
		if !unicode.IsLetter(nextChar) {
			u.Warnf("aborting lexIdentifier: %v", string(nextChar))
			return l.errorToken("identifier must begin with a letter " + l.input[l.start:l.pos])
		}
		for nextChar = l.Next(); isLaxIdentifierRune(nextChar); nextChar = l.Next() {

		}
		// iterate until we find non-identifier, then make sure it is valid/end
		if firstChar == '[' && nextChar == ']' {
			// valid
		} else if firstChar == '\'' && nextChar == '\'' {
			// also valid
		} else {
			u.Errorf("unexpected character in identifier?  %v", string(nextChar))
			return l.errorToken("unexpected character in identifier:  " + string(nextChar))
		}
		wasQouted = true
		l.backup()
		//u.Debugf("quoted?:   %v  ", l.input[l.start:l.pos])
	default:
		if !unicode.IsLetter(firstChar) {
			u.Warnf("aborting lexIdentifier: %v", string(firstChar))
			return l.errorToken("identifier must begin with a letter " + string(l.input[l.start:l.pos]))
		}
		for rune := l.Next(); isIdentifierRune(rune); rune = l.Next() {
			// iterate until we find non-identifer character
		}
		l.backup()
	}
	//u.Debugf("about to emit: %#v", typ)
	l.Emit(TokenIdentity)
	if wasQouted {
		// need to skip last character bc it was quoted
		l.Next()
		l.ignore()
	}

	//u.Debugf("about to return:  %v", nextFn)
	return nil // pop up to parent
}

// Look for end of statement defined by either a semicolon or end of file
func LexEndOfStatement(l *Lexer) StateFn {
	l.SkipWhiteSpaces()
	r := l.Next()
	u.Debugf("sqlend of statement  %s", string(r))
	if r == ';' {
		l.Emit(TokenEOS)
	}
	if l.isEnd() {
		return nil
	}
	u.Warnf("error looking for end of statement: %v", l.current())
	return l.errorToken("Unexpected token:" + l.current())
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
//  (colx = y OR colb = b)
//  cola = 'a5'
//  cola != "a5", colb = "a6"
//  REPLACE(cola,"stuff") != "hello"
//  FirstName = REPLACE(LOWER(name," "))
//  cola IN (1,2,3)
//  cola LIKE "abc"
//  eq(name,"bob") AND age > 5
//
func LexColumns(l *Lexer) StateFn {

	l.SkipWhiteSpaces()
	r := l.Next()

	u.Debugf("LexColumn  r= '%v'", string(r))

	// Cover the logic and grouping
	switch r {
	case '!', '=', '>', '<', '(', ')', ',', ';':
		foundLogical := false
		switch r {
		case ';':
			l.backup()
			return nil
		case '(': // this is a logical Grouping/Ordering
			//l.Push("LexParenEnd", LexParenEnd)
			l.Emit(TokenLeftParenthesis)
			return l.keywordEntry
		case ')': // this is a logical Grouping/Ordering
			l.Emit(TokenRightParenthesis)
			return l.keywordEntry
		case ',':
			l.Emit(TokenComma)
			return l.keywordEntry
		case '!': //  !=
			if r2 := l.peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenNE)
				foundLogical = true
			} else {
				u.Error("Found ! without equal")
				return nil
			}
		case '=': // what about == ?
			l.Emit(TokenEqual)
			foundLogical = true
		case '>':
			if r2 := l.peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenGE)
			}
			l.Emit(TokenGT)
			foundLogical = true
		case '<':
			if r2 := l.peek(); r2 == '=' {
				l.Next()
				l.Emit(TokenLE)
				foundLogical = true
			}
		}
		if foundLogical == true {
			u.Infof("found LexColumns = '%v'", string(r))
			// There may be more than one item here
			l.Push("LexColumns", l.keywordEntry)
			return LexExpressionOrIdentity
		}
	}

	l.backup()
	op := strings.ToLower(l.PeekWord())
	u.Debugf("looking for operator:  word=%s", op)
	switch op {
	case "as":
		l.skipX(2)
		l.Emit(TokenAs)
		l.Push("LexColumns", l.keywordEntry)
		l.Push("lexIdentifier", lexIdentifier)
		return nil
	case "in", "like": // what is complete list here?
		switch op {
		case "in": // IN
			l.skipX(2)
			l.Emit(TokenIN)
			l.Push("LexColumns", l.keywordEntry)
			l.Push("LexListOfArgs", LexListOfArgs)
			return nil
		case "like": // like
			l.skipX(4)
			l.Emit(TokenLike)
			u.Infof("like?  %v", l.peekX(10))
			l.Push("LexColumns", l.keywordEntry)
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
		l.Push("LexColumns", l.keywordEntry)
		return LexExpressionOrIdentity

	default:
		r = l.peek()
		if r == ',' {
			l.Emit(TokenComma)
			l.Push("LexColumns", l.keywordEntry)
			return LexExpressionOrIdentity
		}
		if l.isNextKeyword(op) {
			u.Infof("found keyword? %v ", op)
			return nil
		}
	}
	//u.LogTracef(u.WARN, "hmmmmmmm")
	u.Infof("LexColumns = '%v'", string(r))
	//l.Push("LexCommaOrLogicOrNext", LexCommaOrLogicOrNext)
	//l.Push("LexIdentity", LexIdentity)
	// ensure we don't get into a recursive death spiral here?
	if len(l.stack) < 100 {
		l.Push("LexColumns", l.keywordEntry)
	} else {
		u.Errorf("Gracefully refusing to add more LexColumns: ")
	}
	//u.Debugf("in col or comma sending to expression or identity")
	return LexExpressionOrIdentity
}

// LexComment looks for valid comments which are any of the following
//
//  /* hello */
//  //  hello
//  -- hello
//  # hello
func LexComment(l *Lexer, nextFn StateFn) StateFn {
	//u.Debugf("checking comment: '%s' ", l.input[l.pos:l.pos+2])
	// TODO:  switch statement instead of strings has prefix
	if strings.HasPrefix(l.input[l.pos:], "/*") {
		return lexMultilineCmt(l, nextFn)
	} else if strings.HasPrefix(l.input[l.pos:], "//") {
		//u.Debugf("found single line comment:  // ")
		return lexSingleLineCmt(l, nextFn)
	} else if strings.HasPrefix(l.input[l.pos:], "--") {
		//u.Debugf("found single line comment:  -- ")
		return lexSingleLineCmt(l, nextFn)
	} else if strings.HasPrefix(l.input[l.pos:], "#") {
		//u.Debugf("found single line comment:  # ")
		return lexSingleLineCmt(l, nextFn)
	}
	return nil
}

func lexMultilineCmt(l *Lexer, nextFn StateFn) StateFn {
	// Consume opening "/*"
	l.ignoreWord("/*")
	// peek := l.peek()
	// if peek == '\n' {
	// 	l.ignoreWord("\n")
	// }
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

	return nextFn
}

func lexSingleLineCmt(l *Lexer, nextFn StateFn) StateFn {
	// Consume opening "//" or -- or #
	r := l.Next()
	if r == '-' || r == '/' {
		l.Next()
		l.start = l.pos
	} else {
		// else if r == # we only need one
		l.start = l.pos
	}
	u.Debugf("hm? %q", l.input[l.start:l.pos])

	for {
		r = l.Next()
		if r == '\n' || r == eof {
			l.backup()
			break
		}
	}
	u.Debugf("hm? %q", l.input[l.start:l.pos])
	l.Emit(TokenComment)
	return nextFn
}

// LexNumber scans a number: a float or integer (which can be decimal or hex).
func LexNumber(l *Lexer) StateFn {
	typ, ok := ScanNumber(l)
	if !ok {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	// Emits tokenFloat or tokenInteger.
	l.Emit(typ)
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
func ScanNumber(l *Lexer) (typ TokenType, ok bool) {
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
		l.Next()
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
