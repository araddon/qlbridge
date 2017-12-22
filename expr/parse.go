package expr

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	_     = u.EMPTY
	Trace bool
	eoft  = lex.Token{T: lex.TokenEOF}
)

func init() {
	if t := os.Getenv("exprtrace"); t != "" {
		Trace = true
	}
}

func debugf(depth int, f string, args ...interface{}) {
	if Trace {
		f = strings.Repeat("→ ", depth) + f
		u.DoLog(3, u.DEBUG, fmt.Sprintf(f, args...))
	}
}

// We have a default Dialect, which is the "Language" or rule-set of ql
var DefaultDialect *lex.Dialect = lex.LogicalExpressionDialect

// TokenPager wraps a Lexer, and implements the Logic to determine what is
// the end of this particular clause.  Lexer's are stateless, while
// tokenpager implements state ontop of pager and allows forward/back etc
type TokenPager interface {
	Peek() lex.Token
	Next() lex.Token
	Cur() lex.Token
	Backup()
	IsEnd() bool
	ClauseEnd() bool
	Lexer() *lex.Lexer
	ErrMsg(msg string) error
}

// SchemaInfo is interface for a Column type
type SchemaInfo interface {
	Key() string
}

// SchemaInfoString implements schemaInfo Key()
type SchemaInfoString string

func (m SchemaInfoString) Key() string { return string(m) }

// TokenPager is responsible for determining end of
// current tree (column, etc)
type LexTokenPager struct {
	done   bool
	tokens []lex.Token // list of all the tokens
	cursor int
	lex    *lex.Lexer
}

func NewLexTokenPager(lex *lex.Lexer) *LexTokenPager {
	p := LexTokenPager{
		lex: lex,
	}
	p.cursor = 0
	p.lexNext()
	return &p
}

func (m *LexTokenPager) ErrMsg(msg string) error {
	return m.lex.ErrMsg(m.Cur(), msg)
}

func (m *LexTokenPager) lexNext() {
	if !m.done {
		tok := m.lex.NextToken()
		if tok.T == lex.TokenEOF {
			m.done = true
		}
		m.tokens = append(m.tokens, tok)
	}
}

// Next returns the current token and advances cursor to next one
func (m *LexTokenPager) Next() lex.Token {
	m.lexNext()
	m.cursor++
	if m.cursor+1 > len(m.tokens) {
		//u.Warnf("Next() CRAP? increment cursor: %v of %v %v", m.cursor, len(m.tokens))
		return eoft
	}
	return m.tokens[m.cursor-1]
}

// Returns the current token, does not advance
func (m *LexTokenPager) Cur() lex.Token {
	if m.cursor+1 > len(m.tokens) {
		//u.Warnf("Next() CRAP? increment cursor: %v of %v %v", m.cursor, len(m.tokens), m.cursor < len(m.tokens))
		return eoft
	}
	return m.tokens[m.cursor]
}

// IsEnd determines if pager is at end of statement
func (m *LexTokenPager) IsEnd() bool {
	return false
}

// ClauseEnd are we at end of clause
func (m *LexTokenPager) ClauseEnd() bool {
	return false
}

// Lexer get the underlying lexer
func (m *LexTokenPager) Lexer() *lex.Lexer {
	return m.lex
}

// backup backs the input stream up one token.
func (m *LexTokenPager) Backup() {
	if m.cursor > 0 {
		m.cursor--
		return
	}
}

// Peek returns but does not consume the next token.
func (m *LexTokenPager) Peek() lex.Token {
	if len(m.tokens) <= m.cursor+1 && !m.done {
		m.lexNext()
	}
	if len(m.tokens) < 2 {
		m.lexNext()
	}
	if len(m.tokens) == m.cursor+1 {
		return m.tokens[m.cursor]
	}
	if m.cursor == -1 {
		return m.tokens[1]
	}
	return m.tokens[m.cursor+1]
}

// Tree is the representation of a single parsed expression
type tree struct {
	funcCheck  bool // should we resolve function existence at parse time?
	boolean    bool // Stateful flag for in mid of boolean expressions
	TokenPager      // pager for grabbing next tokens, backup(), recognizing end
	fr         FuncResolver
}

func newTree(pager TokenPager) *tree {
	t := tree{TokenPager: pager, funcCheck: false}
	return &t
}
func newTreeFuncs(pager TokenPager, fr FuncResolver) *tree {
	t := tree{TokenPager: pager, fr: fr, funcCheck: fr != nil}
	return &t
}

// ParseExpression parse a single Expression, returning an Expression Node
//
//    ParseExpression("5 * toint(item_name)")
//
func ParseExpression(expressionText string) (Node, error) {
	l := lex.NewLexer(expressionText, lex.LogicalExpressionDialect)
	pager := NewLexTokenPager(l)
	t := newTree(pager)

	// Parser panics on unexpected syntax, convert this into an err
	return t.parse()
}

// MustParse parse a single Expression, returning an Expression Node
// and panics if it cannot be parsed
//
//    MustParse("5 * toint(item_name)")
//
func MustParse(expressionText string) Node {
	n, err := ParseExpression(expressionText)
	if err != nil {
		panic(err.Error())
	}
	return n
}

// Parse a single Expression, returning an Expression Node
//
// @fr = function registry with any additional functions
//
//    ParseExprWithFuncs("5 * toint(item_name)", funcRegistry)
//
func ParseExprWithFuncs(p TokenPager, fr FuncResolver) (Node, error) {
	t := newTreeFuncs(p, fr)
	// Parser panics on unexpected syntax, convert this into an err
	return t.parse()
}

// Parse a single Expression, returning an Expression Node
//
// @pager = Token Pager
func ParsePager(pager TokenPager) (Node, error) {
	t := newTree(pager)
	// Parser panics on unexpected syntax, convert this into an err
	return t.parse()
}

// errorf formats the error and terminates processing.
func (t *tree) errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// error terminates processing.
func (t *tree) error(err error) {
	t.errorf("%s", err)
}

// expect verifies the current token and guarantees it has the required type
func (t *tree) expect(expected lex.TokenType, context string) lex.Token {
	token := t.Cur()
	if token.T != expected {
		t.unexpected(token, context)
	}
	return token
}

// expectOneOf consumes the next token and guarantees it has one of the required types.
func (t *tree) expectOneOf(expected1, expected2 lex.TokenType, context string) lex.Token {
	token := t.Cur()
	if token.T != expected1 && token.T != expected2 {
		t.unexpected(token, context)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (t *tree) unexpected(token lex.Token, msg string) {
	err := token.ErrMsg(t.Lexer(), msg)
	panic(err.Error())
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (t *tree) recover(errp *error) {
	e := recover()
	if e != nil {
		u.Errorf("Recover():  %v", e)
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		*errp = e.(error)
	}
	return
}

// parse take the tokens and recursively build into Node
func (t *tree) parse() (_ Node, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("parse error: %v", p)
		}
	}()
	return t.O(0), err
}

/*

General overview of Recursive Descent Parsing

https://www.engr.mun.ca/~theo/Misc/exp_parsing.htm

Operator Predence planner during parse phase:
  when we parse and build our node-sub-node structures we need to plan
  the precedence rules, we use a recursion tree to build this

http://dev.mysql.com/doc/refman/5.0/en/operator-precedence.html
https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html#SQL-PRECEDENCE

TODO:
 - if/else, case, for
 - call stack & vars
--------------------------------------
O -> A {( "||" | OR  ) A}
A -> C {( "&&" | AND ) C}
C -> P {( "==" | "!=" | ">" | ">=" | "<" | "<=" | "LIKE" | "IN" | "CONTAINS" | "INTERSECTS") P}
P -> M {( "+" | "-" ) M}
M -> F {( "*" | "/" ) F}
F -> v | "(" O ")" | "!" v | "-" O | "NOT" C | "EXISTS" v | "IS" O | "AND (" O ")" | "OR (" O ")"
v -> value | Func | "INCLUDE" <identity>
Func -> <identity> "(" value {"," value} ")"
value -> number | "string" | O | <identity>



Recursion:  We recurse so the LAST to evaluate is the highest (parent, then or)
   ie the deepest we get in recursion tree is the first to be evaluated

0	Value's
1	Unary + - arithmetic operators
2	* / arithmetic operators
3	Binary + - arithmetic operators, || character operators
4	All comparison operators
5	NOT logical operator
6	AND logical operator
7	OR logical operator
8	Paren's


*/

// expr:
func (t *tree) O(depth int) Node {
	debugf(depth, "O  pre: %v", t.Cur())
	n := t.A(depth)
	debugf(depth, "O post: n:%v cur:%v ", n, t.Cur())
	for {
		tok := t.Cur()
		switch tok.T {
		case lex.TokenLogicOr, lex.TokenOr:
			t.Next()
			n = NewBinaryNode(tok, n, t.A(depth+1))
		case lex.TokenCommentSingleLine:
			t.Next() // consume --
			t.Next() // consume comment after --
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenComma, lex.TokenIf,
			lex.TokenAs, lex.TokenSelect, lex.TokenLimit:
			// these are indicators of End of Current Clause, so we can return
			return n
		default:
			return n
		}
	}
}

func (t *tree) A(depth int) Node {
	debugf(depth, "A  pre: %v", t.Cur())
	n := t.C(depth)
	for {
		debugf(depth, "A post:  cur=%v peek=%v", t.Cur(), t.Peek())
		switch tok := t.Cur(); tok.T {
		case lex.TokenLogicAnd, lex.TokenAnd:
			p := t.Peek()
			if p.T == lex.TokenLeftParenthesis && t.boolean {
				// This is a Boolean Expression Not Binary
				return n
			}
			t.Next()
			debugf(depth, "AND pre-binary n=%s", n)
			n = NewBinaryNode(tok, n, t.C(depth+1))
			debugf(depth, "and post %s", n)
		default:
			return n
		}
	}
}

func (t *tree) C(depth int) Node {
	debugf(depth, "C  pre: %v", t.Cur())
	n := t.P(depth)
	for {
		debugf(depth, "C post: %v peek=%v n=%v", t.Cur(), t.Peek(), n)
		switch cur := t.Cur(); cur.T {
		case lex.TokenNegate:
			debugf(depth+1, "C NEGATE Urnary?: %v", t.Cur())
			t.Next()
			return NewUnary(cur, t.cInner(n, depth+1))
		case lex.TokenIs:
			t.Next()
			if t.Cur().T == lex.TokenNegate {
				cur = t.Next()
				ne := lex.Token{T: lex.TokenNE, V: "!="}
				return NewBinaryNode(ne, n, t.P(depth+1))
			}
			u.Warnf("TokenIS?  is this supported?")
			return NewUnary(cur, t.cInner(n, depth+1))
		default:
			return t.cInner(n, depth)
		}
	}
}

func (t *tree) cInner(n Node, depth int) Node {
	for {
		debugf(depth, "cInner:  tok:  cur=%v peek=%v n=%v", t.Cur(), t.Peek(), n)
		switch cur := t.Cur(); cur.T {
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE, lex.TokenGT, lex.TokenGE,
			lex.TokenLE, lex.TokenLT, lex.TokenLike, lex.TokenContains:
			t.Next()
			n = NewBinaryNode(cur, n, t.P(depth+1))
		case lex.TokenBetween:
			// weird syntax:    BETWEEN x AND y     AND is ignored essentially
			t.Next()
			n2 := t.P(depth)
			t.expect(lex.TokenLogicAnd, "input")
			t.Next()
			n = NewTriNode(cur, n, n2, t.P(depth+1))
		case lex.TokenIN:
			t.Next()
			switch t.Cur().T {
			case lex.TokenIdentity:
				ident := t.Next()
				return NewBinaryNode(cur, n, NewIdentityNode(&ident))
			case lex.TokenLeftParenthesis, lex.TokenLeftBracket:
				// This is a special type of Binary? its 2nd argument is a array node
				return NewBinaryNode(cur, n, t.ArrayNode(depth))
			case lex.TokenUdfExpr:
				fn := t.Next() // consume Function Name
				return NewBinaryNode(cur, n, t.Func(depth, fn))
			case lex.TokenValue, lex.TokenString:
				v := t.Next()
				return NewBinaryNode(cur, n, NewStringNode(v.V))
			case lex.TokenValueEscaped:
				v := t.Next()
				return NewBinaryNode(cur, n, NewStringNeedsEscape(v))
			default:
				t.unexpected(t.Cur(), "Right side of IN expected (identity|array|func|value) but got")
			}
		case lex.TokenIntersects:
			t.Next() // Consume "INTERSECTS"
			switch t.Cur().T {
			case lex.TokenIdentity:
				// x INTERSECTS field   where field MUST be an array
				ident := t.Next()
				in := NewIdentityNode(&ident)
				if in.IsBooleanIdentity() {
					t.unexpected(t.Cur(), "expected array on right side of INTERSECTS")
				}
				return NewBinaryNode(cur, n, in)
			case lex.TokenLeftParenthesis, lex.TokenLeftBracket:
				// The 2nd argument is an array node
				return NewBinaryNode(cur, n, t.ArrayNode(depth))
			case lex.TokenUdfExpr:
				fn := t.Next() // consume Function Name
				return NewBinaryNode(cur, n, t.Func(depth, fn))
			default:
				t.unexpected(t.Cur(), "expected array on right side of INTERSECTS")
			}
		case lex.TokenNull, lex.TokenNil:
			t.Next()
			return NewNull(cur)
		default:
			return n
		}
	}
}

func (t *tree) P(depth int) Node {
	debugf(depth, "P pre : %v", t.Cur())
	n := t.M(depth)
	debugf(depth, "P post: %v", t.Cur())
	for {
		switch cur := t.Cur(); cur.T {
		case lex.TokenPlus, lex.TokenMinus:
			t.Next()
			n = NewBinaryNode(cur, n, t.M(depth+1))
		default:
			return n
		}
	}
}

func (t *tree) M(depth int) Node {
	debugf(depth, "M pre : %v", t.Cur())
	n := t.F(depth)
	debugf(depth, "M post: %v  %v", t.Cur(), n)
	for {
		switch cur := t.Cur(); cur.T {
		case lex.TokenStar, lex.TokenMultiply, lex.TokenDivide, lex.TokenModulus:
			t.Next()
			n = NewBinaryNode(cur, n, t.F(depth+1))
		default:
			return n
		}
	}
}

// F -> v | "(" O ")" | "!" O | "-" O | "NOT" C | "EXISTS" v | "IS" O | "AND (" O ")" | "OR (" O ")"
func (t *tree) F(depth int) Node {
	debugf(depth, "F: %v", t.Cur())

	// Urnary operations
	switch cur := t.Cur(); cur.T {
	case lex.TokenNegate, lex.TokenMinus:

		t.Next() // consume NOT, !, Minus

		debugf(depth, "start:%v cur: %v   peek:%v", cur, t.Cur(), t.Peek())
		var arg Node

		switch t.Peek().T {
		case lex.TokenIN, lex.TokenLike, lex.TokenContains, lex.TokenBetween,
			lex.TokenIntersects:
			// TODO:  this is a bug.  An old version of generator was saving these
			//  NOT news INTERSECTS ("a")    which is invalid it should be
			//  news NOT INTERSECTS ("a")  OR NOT (news INTERSECTS ("a"))
			//
			// NOT <expr> LIKE <expr>
			// NOT <expr> INTERSECTS <expr>
			// NOT <expr> BETWEEN <expr> AND <expr>
			// NOT <expr> CONTAINS <expr>
			// NOT <expr> IN <expr>
			//
			// NOT identity > 7

			arg = t.C(depth + 1)
		default:

			switch t.Cur().T {
			case lex.TokenUdfExpr:
				arg = t.v(depth + 1)
			default:
				arg = t.C(depth + 1)
			}
		}
		n := NewUnary(cur, arg)
		debugf(depth, "f urnary: %s   arg: %#v", n, arg)
		return n
	case lex.TokenExists:
		// Urnary operations:  require right side value node
		t.Next() // Consume "EXISTS"
		debugf(depth, "F PRE  EXISTS:%v   cur:%v", cur, t.Cur())
		n := NewUnary(cur, t.v(depth+1))
		debugf(depth, "F POST EXISTS: %s  cur:%v", n, t.Cur())
		return n
	case lex.TokenIs:
		nxt := t.Next()
		if nxt.T == lex.TokenNegate {
			return NewUnary(cur, t.F(depth+1))
		}
		return NewUnary(cur, t.F(depth+1))
	case lex.TokenLogicAnd, lex.TokenLogicOr:
		debugf(depth, "found boolean and/or (O)? %v", cur)
		t.Next() // consume AND/OR
		t.discardNewLinesAndComments()
		switch t.Cur().T {
		case lex.TokenLeftParenthesis:
			t.Next() // Consume Left Paren
			t.discardNewLinesAndComments()
			n := NewBooleanNode(cur)
			t.boolean = true
			args, err, wasBoolean := nodeArray(t, depth)
			if err != nil {
				panic(fmt.Errorf("Unexpected %v", err))
			}
			n.Args = args
			if !wasBoolean {
				// Whoops, binary not boolean, there are some ambiguous ones:
				// binary:   x = y OR ( stuff > 5)
				// boolean:  AND (x = y, OR ( stuff > 5, x = 9))
				u.Warnf("not handled was boolean")
			}

			t.expect(lex.TokenRightParenthesis, "input")
			t.boolean = false
			t.Next()
			//debugf(depth, "found boolean expression %v", n.Collapse())
			return n.Collapse()
		}
		t.unexpected(t.Cur(), "Expected Left Paren after AND/OR ()")
	default:
		return t.v(depth)
	}
	panic("unreachable")
}

func (t *tree) v(depth int) Node {
	debugf(depth, "v: cur(): %v   peek:%v", t.Cur(), t.Peek())
	switch cur := t.Cur(); cur.T {
	case lex.TokenInclude:
		inc := t.Next() // consume Include
		nxt := t.Next()
		//u.Debugf("inc: %v  nxt %v", inc, nxt)
		if nxt.T == lex.TokenIdentity {
			id := NewIdentityNode(&nxt)
			return NewInclude(inc, id)
		}
		panic(fmt.Errorf("Unexpected Identity got %v", nxt))
	case lex.TokenInteger, lex.TokenFloat:
		n, err := NewNumberStr(cur.V)
		if err != nil {
			t.error(err)
		}
		t.Next()
		return n
	case lex.TokenValue:
		n := NewStringNodeToken(cur)
		t.Next()
		debugf(depth, "after value %v", t.Cur())
		return n
	case lex.TokenValueEscaped:
		n := NewStringNeedsEscape(cur)
		t.Next()
		return n
	case lex.TokenIdentity:
		n := NewIdentityNode(&cur)
		t.Next() // Consume identity

		return n
	case lex.TokenNull:
		t.Next()
		return NewNull(cur)
	case lex.TokenStar:
		n := NewStringNoQuoteNode(cur.V)
		t.Next()
		return n
	case lex.TokenLeftBracket:
		// [   ie     [1,2,3] json array or static array values
		t.Next() // Consume the [
		arrayVal, err := ValueArray(depth+1, t.TokenPager)
		if err != nil {
			t.unexpected(t.Cur(), "jsonarray unexpected token")
			return nil
		}
		n := NewValueNode(arrayVal)
		return n
	case lex.TokenUdfExpr:
		t.Next() // consume Function Name
		return t.Func(depth, cur)
	case lex.TokenLeftParenthesis:
		t.Next() // Consume  (
		n := t.O(depth + 1)
		debugf(depth, "v: paren  T:%T  %v   cur:%v", n, n, t.Cur())
		if bn, ok := n.(*BinaryNode); ok {
			bn.Paren = true
		}
		debugf(depth, "after paren %v", t.Cur())
		t.expect(lex.TokenRightParenthesis, "Expected Right Paren to end ()")
		t.Next()
		return n
	default:
		if t.ClauseEnd() {
			return nil
		}
		t.unexpected(cur, "Un recognized input")
	}
	t.Backup()
	return nil
}

func (t *tree) Func(depth int, funcTok lex.Token) (fn *FuncNode) {
	debugf(depth, "Func: tok: %v cur:%v peek:%v", funcTok.V, t.Cur(), t.Peek())
	if t.Cur().T != lex.TokenLeftParenthesis {
		t.unexpected(t.Cur(), "must have left paren on function")
	}
	var node Node
	var tok lex.Token

	funcImpl, ok := t.getFunction(funcTok.V)
	if !ok {
		if t.funcCheck {
			t.errorf("non existent function %s", funcTok.V)
		} else {
			// if we aren't testing for validity, make a "fake" func
			// we may not be using vm, just ast
			//u.Warnf("non func? %v", funcTok.V)
			funcImpl = Func{Name: funcTok.V, Eval: EmptyEvalFunc}
		}
	}
	fn = NewFuncNode(funcTok.V, funcImpl)
	fn.Missing = !ok

	t.expect(lex.TokenLeftParenthesis, "func")
	t.Next() // Are we sure we consume?

	defer func() {
		if err := fn.Validate(); err != nil {
			t.error(err) // will panic
		}
	}()

	switch {
	// Ugh, we need a way of identifying which functions get this special
	// parser?
	case t.Peek().T == lex.TokenAs && strings.ToLower(fn.Name) == "cast":
		// We are not in a comma style function
		//  CAST(<expression> AS <identity>)

		node = t.O(depth + 1)
		if node != nil {
			fn.append(node)
		}
		if t.Cur().T != lex.TokenAs {
			t.unexpected(t.Cur(), "func AS")
		}
		// This really isn't correct, we probably need an OperatorNode?
		fn.append(NewStringNodeToken(t.Next()))
		if t.Cur().T != lex.TokenIdentity {
			t.unexpected(t.Cur(), "func AS exected Identity")
		}
		fn.append(NewStringNodeToken(t.Next()))
		return fn
	default:
		lastComma := false
		for {
			node = nil

			switch firstToken := t.Cur(); firstToken.T {
			case lex.TokenRightParenthesis:
				t.Next()
				if node != nil {
					fn.append(node)
				}
				return
			case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom:
				if node != nil {
					fn.append(node)
				}
				return
			case lex.TokenComma:
				if len(fn.Args) == 0 || t.Peek().T == lex.TokenComma || lastComma {
					t.unexpected(tok, "Wanted argument but got comma")
				}
				lastComma = true
				t.Next()
				continue
			default:
				node = t.O(depth + 1)
			}
			lastComma = false

			tok = t.Cur()
			switch tok.T {
			case lex.TokenComma:
				if node != nil {
					fn.append(node)
				}
				lastComma = true
				// continue
			case lex.TokenRightParenthesis:
				if node != nil {
					fn.append(node)
				}
				t.Next()
				return
			case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenAs:
				if node != nil {
					fn.append(node)
				}
				t.Next()
				return
			case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE, lex.TokenGT, lex.TokenGE,
				lex.TokenLE, lex.TokenLT, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide:
				// this func arg is an expression
				//     toint(str_item * 5)
				node = t.O(depth + 1)
				if node != nil {
					fn.append(node)
				}
			default:
				t.unexpected(tok, "func")
			}

			t.Next()
		}
	}
}

// get Function from Global function registry.
func (t *tree) getFunction(name string) (fn Func, ok bool) {
	if t.fr != nil {
		if fn, ok = t.fr.FuncGet(name); ok {
			return
		}
	}
	if fn, ok = funcReg.FuncGet(strings.ToLower(name)); ok {
		return
	}
	return
}

// ArrayNode parses multi-argument array nodes aka: IN (a,b,c).
func (t *tree) ArrayNode(depth int) Node {

	an := NewArrayNode()
	t.expect(lex.TokenLeftParenthesis, "Expected left paren: (")
	t.Next() // Consume Left Paren

	for {
		debugf(depth, "ArrayNode(%d): %v", len(an.Args), t.Cur())
		switch cur := t.Cur(); cur.T {
		case lex.TokenRightParenthesis:
			t.Next() // Consume the Paren
			debugf(depth, "ArrayNode EXIT: %v", an)
			return an
		case lex.TokenComma:
			t.Next()
		default:
			n := t.O(depth)
			if n != nil {
				an.Append(n)
			} else {
				u.Warnf("invalid?  %v", t.Cur())
				return an
			}
		}
	}
}

// ValueArray
//     IN ("a","b","c")
//     ["a","b","c"]
func ValueArray(depth int, pg TokenPager) (value.Value, error) {

	vals := make([]value.Value, 0)
arrayLoop:
	for {
		tok := pg.Next() // consume token
		debugf(depth, "ValueArray: len(%d), cur:%v", len(vals), tok)
		switch tok.T {
		case lex.TokenComma:
			// continue
		case lex.TokenRightParenthesis:
			break arrayLoop
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenAs:
			break arrayLoop
		case lex.TokenValue:
			vals = append(vals, value.NewStringValue(tok.V))
		case lex.TokenValueEscaped:
			newVal, _ := StringUnEscape('"', tok.V)
			vals = append(vals, value.NewStringValue(newVal))
		case lex.TokenInteger:
			fv, err := strconv.ParseFloat(tok.V, 64)
			if err == nil {
				vals = append(vals, value.NewNumberValue(fv))
			} else {
				return value.NilValueVal, err
			}
		case lex.TokenFloat:
			fv, err := strconv.ParseFloat(tok.V, 64)
			if err == nil {
				vals = append(vals, value.NewNumberValue(fv))
			} else {
				return value.NilValueVal, err
			}
		default:
			return value.NilValueVal, fmt.Errorf("Could not recognize token: %v", tok)
		}

		tok = pg.Next()
		switch tok.T {
		case lex.TokenComma:
			// fine, consume the comma
		case lex.TokenRightBracket:
			break arrayLoop
		default:
			u.Warnf("unrecognized token: %v", tok)
			return value.NilValueVal, fmt.Errorf("unrecognized token %v", tok)
		}
	}
	return value.NewSliceValues(vals), nil
}

func nodeArray(t *tree, depth int) ([]Node, error, bool) {

	nodes := make([]Node, 0)

	for {

		t.discardNewLinesAndComments()

		switch t.Cur().T {
		case lex.TokenRightParenthesis:
			debugf(depth, "NodeArray(%d) EXIT", len(nodes))
			return nodes, nil, true
		case lex.TokenComma:
			t.Next() // Consume
		}

		debugf(depth, "NodeArray(%d) cur:%v peek:%v", len(nodes), t.Cur().V, t.Peek().V)
		n := t.O(depth + 1)
		if n == nil {
			return nodes, nil, true
		}
		nodes = append(nodes, n)

	nextNodeLoop:
		for {
			// We are going to loop until we find the first Non-Comment Token
			switch t.Cur().T {
			case lex.TokenNewLine:
				t.Next() // Consume new line
				break nextNodeLoop
			case lex.TokenComma:
				// indicates start of new expression
				t.Next() // consume comma
				break nextNodeLoop
			case lex.TokenComment, lex.TokenCommentML,
				lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd,
				lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
				// skip, currently ignore these
				t.Next()
			case lex.TokenRightParenthesis:
				return nodes, nil, true
			default:
				// first non-comment token
				break nextNodeLoop
			}
		}
	}
}

func (t *tree) discardNewLinesAndComments() {
	for {
		// We are going to loop until we find the first Non-Comment Token
		switch t.Cur().T {
		case lex.TokenNewLine:
			t.Next() // Consume new line
		case lex.TokenComment, lex.TokenCommentML,
			lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd,
			lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
			t.Next()
		default:
			// first non-comment token
			return
		}
	}
}
