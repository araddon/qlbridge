package expr

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// We have a default Dialect, which is the "Language" or rule-set of ql
var DefaultDialect *lex.Dialect = lex.LogicalExpressionDialect

// TokenPager wraps a Lexer, and implements the Logic to determine what is
// the end of this particular clause.  Lexer's are stateless, while
// tokenpager implements state ontop of pager and allows forward/back etc
//
type TokenPager interface {
	Peek() lex.Token
	Next() lex.Token
	Cur() lex.Token
	Backup()
	IsEnd() bool
	ClauseEnd() bool
	Lexer() *lex.Lexer
}

// SchemaInfo is interface for a Column type
//
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
	}
	return m.tokens[m.cursor-1]
}

// Returns the current token, does not advance
func (m *LexTokenPager) Cur() lex.Token {
	//u.Debugf("Cur(): %v of %v  %v", m.cursor, len(m.tokens), m.tokens[m.cursor])
	if m.cursor+1 >= len(m.tokens) {
		//u.Warnf("Next() CRAP? increment cursor: %v of %v %v", m.cursor, len(m.tokens), m.cursor < len(m.tokens))
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
	//u.Debugf("prepeek: %v of %v", m.cursor, len(m.tokens))
	if len(m.tokens) <= m.cursor+1 && !m.done {
		m.lexNext()
		//u.Warnf("lexed cursor?: %v %p", m.cursor, &m.cursor)
	}
	if len(m.tokens) < 2 {
		m.lexNext()
	}
	if len(m.tokens) == m.cursor+1 {
		//u.Infof("last one?: %v of %v  %v", m.cursor, len(m.tokens), m.tokens[m.cursor])
		return m.tokens[m.cursor]
	}
	if m.cursor == -1 {
		return m.tokens[1]
	}
	//u.Infof("peek:  %v of %v %v", m.cursor, len(m.tokens), m.tokens[m.cursor+1])
	return m.tokens[m.cursor+1]
}

// Tree is the representation of a single parsed expression
type Tree struct {
	runCheck   bool
	Root       Node // top-level root node of the tree
	TokenPager      // pager for grabbing next tokens, backup(), recognizing end
	fr         FuncResolver
}

func NewTree(pager TokenPager) *Tree {
	t := Tree{TokenPager: pager}
	return &t
}
func NewTreeFuncs(pager TokenPager, fr FuncResolver) *Tree {
	t := Tree{TokenPager: pager, fr: fr}
	return &t
}

// Parse a single Expression, returning a Tree
//
//    ParseExpression("5 * toint(item_name)")
//
func ParseExpression(expressionText string) (*Tree, error) {
	l := lex.NewLexer(expressionText, lex.LogicalExpressionDialect)
	pager := NewLexTokenPager(l)
	t := NewTree(pager)

	// Parser panics on unexpected syntax, convert this into an err
	err := t.BuildTree(true)
	return t, err
}

// Parsing.

// errorf formats the error and terminates processing.
func (t *Tree) errorf(format string, args ...interface{}) {
	t.Root = nil
	format = fmt.Sprintf("expr: %s", format)
	msg := fmt.Errorf(format, args...)
	u.LogTracef(u.WARN, "about to panic: %v for \n%s", msg, t.Lexer().RawInput())
	panic(msg)
}

// error terminates processing.
func (t *Tree) error(err error) {
	t.errorf("%s", err)
}

// expect verifies the current token and guarantees it has the required type
func (t *Tree) expect(expected lex.TokenType, context string) lex.Token {
	token := t.Cur()
	//u.Debugf("checking expected? %v got?: %v", expected, token)
	if token.T != expected {
		u.Warnf("unexpeted token? %v want:%v", token, expected)
		t.unexpected(token, context)
	}
	return token
}

// expectOneOf consumes the next token and guarantees it has one of the required types.
func (t *Tree) expectOneOf(expected1, expected2 lex.TokenType, context string) lex.Token {
	token := t.Cur()
	if token.T != expected1 && token.T != expected2 {
		t.unexpected(token, context)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (t *Tree) unexpected(token lex.Token, context string) {
	u.Errorf("unexpected?  %v", token)
	t.errorf("unexpected %s in %s", token, context)
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (t *Tree) recover(errp *error) {
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

// buildTree take the tokens and recursively build into expression tree node
// @runCheck  Do we want to verify this tree?   If being used as VM then yes.
func (t *Tree) BuildTree(runCheck bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("parse error: %v", p)
		}
	}()
	t.runCheck = runCheck
	t.Root = t.O(0)
	if runCheck {
		if err = t.Root.Check(); err != nil {
			u.Errorf("found error: %v", err)
			t.error(err)
			return err
		}
	}
	return err
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
 - implement new one for parens
 - implement flags for commutative/
--------------------------------------
O -> A {( "||" | OR  ) A}
A -> C {( "&&" | AND ) C}
C -> P {( "==" | "!=" | ">" | ">=" | "<" | "<=" | "LIKE" | "IN" | "CONTAINS") P}
P -> M {( "+" | "-" ) M}
M -> F {( "*" | "/" ) F}
F -> v | "(" O ")" | "!" O | "-" O
v -> number | func(..)
Func -> name "(" param {"," param} ")"
param -> number | "string" | O



Recursion:  We recurse so the LAST to evaluate is the highest (parent, then or)
   ie the deepest we get in recursion tree is the first to be evaluated

1	Unary + - arithmetic operators, PRIOR operator
2	* / arithmetic operators
3	Binary + - arithmetic operators, || character operators
4	All comparison operators
5	NOT logical operator
6	AND logical operator
7	OR logical operator
8   Paren's


*/

// expr:
func (t *Tree) O(depth int) Node {
	//u.Debugf("%s t.O  pre: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.A(depth)
	//u.Debugf("%s t.O post: n:%v cur:%v ", strings.Repeat("→ ", depth), n, t.Cur())
	for {
		tok := t.Cur()
		//u.Debugf("tok:  cur=%v peek=%v", t.Cur(), t.Peek())
		switch tok.T {
		case lex.TokenLogicOr, lex.TokenOr:
			t.Next()
			n = NewBinaryNode(tok, n, t.A(depth+1))
		case lex.TokenCommentSingleLine:
			// we consume the comment signifier "--""   as well as comment
			//u.Debugf("tok:  %v", t.Next())
			//u.Debugf("tok:  %v", t.Next())
			t.Next()
			t.Next()
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenComma, lex.TokenIf,
			lex.TokenAs, lex.TokenSelect, lex.TokenLimit:
			// these are indicators of End of Current Clause, so we can return?
			//u.Debugf("done, return: %v", tok)
			return n
		default:
			//u.Debugf("root couldnt evaluate node? %v", tok)
			return n
		}
	}
}

func (t *Tree) A(depth int) Node {
	//u.Debugf("%s t.A  pre: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.C(depth)
	//u.Debugf("%s t.A post: %v", strings.Repeat("→ ", depth), t.Cur())
	for {
		//u.Debugf("tok:  cur=%v peek=%v", t.Cur(), t.Peek())
		switch tok := t.Cur(); tok.T {
		case lex.TokenLogicAnd, lex.TokenAnd:
			t.Next()
			n = NewBinaryNode(tok, n, t.C(depth+1))
		default:
			return n
		}
	}
}

func (t *Tree) C(depth int) Node {
	//u.Debugf("%s t.C  pre: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.P(depth)
	//u.Debugf("%s t.C post: %v", strings.Repeat("→ ", depth), t.Cur())
	for {
		//u.Debugf("tok:  cur=%v peek=%v n=%v", t.Cur(), t.Peek(), n)
		switch cur := t.Cur(); cur.T {
		case lex.TokenNegate:
			t.Next()
			return NewUnary(cur, t.cInner(n, depth+1))
		case lex.TokenIs:
			t.Next()
			if t.Cur().T == lex.TokenNegate {
				cur = t.Next()
				ne := lex.Token{T: lex.TokenNE, V: "!="}
				return NewBinaryNode(ne, n, t.P(depth+1))
			}
			return NewUnary(cur, t.cInner(n, depth+1))
		default:
			return t.cInner(n, depth)
		}
	}
}

func (t *Tree) cInner(n Node, depth int) Node {
	//u.Debugf("%s t.cInner: %v", strings.Repeat("→ ", depth), t.Cur())
	for {
		//u.Debugf("cInner:  tok:  cur=%v peek=%v n=%v", t.Cur(), t.Peek(), n.String())
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
		case lex.TokenIN, lex.TokenIntersects:
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
			default:
				t.unexpected(t.Cur(), "input")
			}
		case lex.TokenNull, lex.TokenNil:
			t.Next()
			return NewNull(cur)
		default:
			return n
		}
	}
}

func (t *Tree) P(depth int) Node {
	//u.Debugf("%s t.P pre : %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.M(depth)
	//u.Debugf("%s t.P post: %v", strings.Repeat("→ ", depth), t.Cur())
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

func (t *Tree) M(depth int) Node {
	//u.Debugf("%s t.M pre : %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.F(depth)
	//u.Debugf("%s t.M post: %v  %v", strings.Repeat("→ ", depth), t.Cur(), n)
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

// ArrayNode parses multi-argument array nodes aka: IN (a,b,c).
func (t *Tree) ArrayNode(depth int) Node {
	//u.Debugf("%s t.ArrayNode: %v", strings.Repeat("→ ", depth), t.Cur())
	an := NewArrayNode()
	switch cur := t.Cur(); cur.T {
	case lex.TokenLeftParenthesis:
		// continue
	default:
		t.unexpected(cur, "input")
	}
	t.Next() // Consume Left Paren

	for {
		//u.Debugf("%s t.ArrayNode after: %v ", strings.Repeat("→ ", depth), t.Cur())
		switch cur := t.Cur(); cur.T {
		case lex.TokenRightParenthesis:
			t.Next() // Consume the Paren
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

func (t *Tree) F(depth int) Node {
	//u.Debugf("%s t.F: %v", strings.Repeat("→ ", depth), t.Cur())
	switch cur := t.Cur(); cur.T {
	case lex.TokenUdfExpr:
		return t.v(depth)
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
		return t.v(depth)
	case lex.TokenIdentity:
		return t.v(depth)
	case lex.TokenValue:
		return t.v(depth)
	case lex.TokenNull:
		return t.v(depth)
	case lex.TokenLeftBracket:
		// [
		return t.v(depth)
	case lex.TokenStar:
		// in special situations:   count(*) ??
		return t.v(depth)
	case lex.TokenNegate, lex.TokenMinus, lex.TokenExists:
		t.Next()
		n := NewUnary(cur, t.F(depth+1))
		return n
	case lex.TokenIs:
		nxt := t.Next()
		if nxt.T == lex.TokenNegate {
			return NewUnary(cur, t.F(depth+1))
		}
		return NewUnary(cur, t.F(depth+1))
	case lex.TokenLeftParenthesis:
		// I don't think this is right, parens should be higher up
		// in precedence stack, very top?
		t.Next() // Consume the Paren
		n := t.O(depth + 1)
		if bn, ok := n.(*BinaryNode); ok {
			bn.Paren = true
		}
		t.expect(lex.TokenRightParenthesis, "input")
		t.Next()
		return n
	case lex.TokenLogicAnd, lex.TokenLogicOr:
		//u.Debugf("found and/or? %v", cur)
		t.Next() // consume AND/OR
		switch t.Cur().T {
		case lex.TokenLeftParenthesis:
			t.Next()
			n := NewBooleanNode(cur)
			args, err := nodeArray(t)
			if err != nil {
				panic(fmt.Errorf("Unexpected %v", err))
			}
			n.Args = args
			return n
		}
	default:
		u.Warnf("unexpected? %v", cur)
		//t.unexpected(cur, "input")
		panic(fmt.Sprintf("unexpected token %v ", cur))
	}
	return nil
}

func (t *Tree) v(depth int) Node {
	//u.Debugf("%s t.v: cur(): %v   peek:%v", strings.Repeat("→ ", depth), t.Cur(), t.Peek())
	switch cur := t.Cur(); cur.T {
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
		return n
	case lex.TokenIdentity:
		n := NewIdentityNode(&cur)
		t.Next()
		return n
	case lex.TokenNull:
		t.Next()
		return NewNull(cur)
	case lex.TokenStar:
		n := NewStringNoQuoteNode(cur.V)
		t.Next()
		return n
	case lex.TokenLeftBracket:
		// [
		t.Next() // Consume the [
		arrayVal, err := ValueArray(t.TokenPager)
		if err != nil {
			t.unexpected(t.Cur(), "jsonarray")
			return nil
		}
		n := NewValueNode(arrayVal)
		return n
	case lex.TokenUdfExpr:
		t.Next() // consume Function Name
		return t.Func(depth, cur)
	case lex.TokenLeftParenthesis:
		// I don't think this is right, it should be higher up
		// in precedence stack, very top?
		t.Next()
		n := t.O(depth + 1)
		if bn, ok := n.(*BinaryNode); ok {
			bn.Paren = true
		}
		//u.Debugf("cur?%v n %v  ", t.Cur(), n.StringAST())
		t.Next()
		t.expect(lex.TokenRightParenthesis, "input")
		return n
	default:
		if t.ClauseEnd() {
			return nil
		}
		//u.Warnf("Unexpected?: %v", cur)
		t.unexpected(cur, "input")
	}
	t.Backup()
	return nil
}

func (t *Tree) Func(depth int, funcTok lex.Token) (fn *FuncNode) {
	//u.Debugf("%s Func tok: %v cur:%v peek:%v", strings.Repeat("→ ", depth), funcTok.V, t.Cur().V, t.Peek().V)
	if t.Cur().T != lex.TokenLeftParenthesis {
		panic(fmt.Sprintf("must have left paren on function: %v", t.Peek()))
	}
	var node Node
	var tok lex.Token

	funcImpl, ok := t.getFunction(funcTok.V)
	if !ok {
		if t.runCheck {
			//u.Warnf("non func? %v", funcTok.V)
			t.errorf("non existent function %s", funcTok.V)
		} else {
			// if we aren't testing for validity, make a "fake" func
			// we may not be using vm, just ast
			//u.Warnf("non func? %v", funcTok.V)
			funcImpl = Func{Name: funcTok.V}
		}
	}
	fn = NewFuncNode(funcTok.V, funcImpl)
	fn.Missing = !ok
	//u.Debugf("%d t.Func()?: %v %v", depth, t.Cur(), t.Peek())
	//t.Next() // step forward to hopefully left paren
	t.expect(lex.TokenLeftParenthesis, "func")
	t.Next() // Are we sure we consume?

	switch {
	// Ugh, we need a way of identifying which functions get this special
	// parser?
	case t.Peek().T == lex.TokenAs && strings.ToLower(fn.Name) == "cast":
		// We are not in a comma style function
		//  CAST(<expression> AS <identity>)

		node = t.O(depth + 1)
		//u.Debugf("non comma func: node%#v, cur:%v", node, t.Cur())
		if node != nil {
			fn.append(node)
		}
		if t.Cur().T != lex.TokenAs {
			t.unexpected(t.Cur(), "func AS")
		}
		// This really isn't correct, we probably need an OperatorNode?
		fn.append(NewStringNodeToken(t.Next()))
		if t.Cur().T != lex.TokenIdentity {
			t.unexpected(t.Cur(), "func AS")
		}
		fn.append(NewStringNodeToken(t.Next()))
		//u.Debugf("nice %s", fn.String())
		return fn
	default:
		lastComma := false
		for {
			node = nil

			//u.Infof("%s func arg loop: %v peek=%v", strings.Repeat("→ ", depth+1), t.Cur(), t.Peek())
			switch firstToken := t.Cur(); firstToken.T {
			case lex.TokenRightParenthesis:
				t.Next()
				if node != nil {
					fn.append(node)
				}
				//u.Warnf(" right paren? ")
				return
			case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom:
				//u.Warnf("return: %v", t.Cur())
				if node != nil {
					fn.append(node)
				}
				return
			case lex.TokenComma:
				if len(fn.Args) == 0 || t.Peek().T == lex.TokenComma || lastComma {
					//u.Errorf("No node but comma? %v", tok)
					t.unexpected(tok, "Wanted argument but got comma")
				}
				lastComma = true
				t.Next()
				continue
			default:
				//u.Debugf("%v getting node? t.Func()?: %v", depth, firstToken)
				node = t.O(depth + 1)
			}
			lastComma = false

			tok = t.Cur()
			//u.Infof("%d Func() pt2 consumed token?: %v", depth, tok)
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
				//u.Warnf("found right paren %v", t.Cur())
				return
			case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenAs:
				if node != nil {
					fn.append(node)
				}
				t.Next()
				//u.Debugf("return: %v", t.Cur())
				return
			case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE, lex.TokenGT, lex.TokenGE,
				lex.TokenLE, lex.TokenLT, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide:
				// this func arg is an expression
				//     toint(str_item * 5)

				//t.Backup()
				//u.Debugf("hmmmmm:  %v  cu=%v", tok, t.Cur())
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

// get Function from Global
func (t *Tree) getFunction(name string) (v Func, ok bool) {
	if t.fr != nil {
		if v, ok = t.fr.FuncGet(name); ok {
			return
		}
	}
	if v, ok = funcs[strings.ToLower(name)]; ok {
		return
	}
	return
}

// ValueArray
//     IN ("a","b","c")
//     ["a","b","c"]
func ValueArray(pg TokenPager) (value.Value, error) {

	//u.Debugf("valueArray cur:%v peek:%v", pg.Cur().V, pg.Peek().V)
	vals := make([]value.Value, 0)
arrayLoop:
	for {
		tok := pg.Next() // consume token
		//u.Infof("valueArray() consumed token?: %v", tok)
		switch tok.T {
		case lex.TokenComma:
			// continue
		case lex.TokenRightParenthesis:
			//u.Warnf("found right paren  %v cur: %v", tok, pg.Cur())
			break arrayLoop
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenAs:
			//u.Debugf("return: %v", tok)
			break arrayLoop
		case lex.TokenValue:
			vals = append(vals, value.NewStringValue(tok.V))
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
			//u.Warnf("right bracket: %v", tok)
			break arrayLoop
		default:
			u.Warnf("unrecognized token: %v", tok)
			return value.NilValueVal, fmt.Errorf("unrecognized token %v", tok)
		}
	}
	//u.Debugf("returning array: %v", vals)
	return value.NewSliceValues(vals), nil
}

func nodeArray(parent *Tree) ([]Node, error) {

	//u.Debugf("NodeArray cur:%v peek:%v", parent.Cur().V, parent.Peek().V)
	nodes := make([]Node, 0)

nodeLoop:
	for {
		t := NewTreeFuncs(parent.TokenPager, parent.fr)
		err := t.BuildTree(parent.runCheck)
		if err != nil {
			u.Errorf("error: %v", err)
			return nil, err
		} else if t.Root != nil {
			nodes = append(nodes, t.Root)
			//u.Infof("nodeArray() consumed tree?: %s", t.Root)
		} else {
			panic(fmt.Sprintf("wtf? %v", t))
		}
	nextNodeLoop:
		for {
			//u.Debugf("what? %v", parent.Cur())
			// We are going to loop until we find the first Non-Comment Token
			switch parent.Cur().T {
			case lex.TokenNewLine:
				parent.Next() // Consume new line
				break nextNodeLoop
			case lex.TokenComma:
				// indicates start of new expression
				parent.Next() // consume comma
				break nextNodeLoop
			case lex.TokenComment, lex.TokenCommentML,
				lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd,
				lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
				// skip, currently ignore these
				parent.Next()
			case lex.TokenRightParenthesis:
				parent.Next() // consume??
				break nodeLoop
			default:
				// first non-comment token
				break nextNodeLoop
			}
		}
	}
	//u.Debugf("returning nodeArray: %v", nodes)
	return nodes, nil
}

func (t *Tree) String() string {
	return t.Root.String()
}
