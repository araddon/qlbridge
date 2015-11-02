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
	Last() lex.TokenType
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

// TokenPager is responsible for determining end of
// current tree (column, etc)
type LexTokenPager struct {
	done   bool
	tokens []lex.Token // list of all the tokens
	cursor int
	lex    *lex.Lexer
	end    lex.TokenType
}

func NewLexTokenPager(lex *lex.Lexer) *LexTokenPager {
	p := LexTokenPager{
		lex: lex,
	}
	p.cursor = 0
	p.lexNext()
	return &p
}

// next returns the next token.
func (m *LexTokenPager) Next() lex.Token {
	m.lexNext()
	m.cursor++
	if m.cursor+1 > len(m.tokens) {
		//u.Warnf("Next() CRAP? increment cursor: %v of %v %v", m.cursor, len(m.tokens))
		//panic("WTF, not enough tokens?")
	}
	//u.Debugf("Next(): %v of %v %v", m.cursor, len(m.tokens), m.tokens[m.cursor])
	return m.tokens[m.cursor-1]
}
func (m *LexTokenPager) lexNext() {
	if !m.done {
		tok := m.lex.NextToken()
		if tok.T == lex.TokenEOF {
			m.done = true
		}
		m.tokens = append(m.tokens, tok)
		//u.Infof("lexNext: %v of %v cur=%v", m.cursor, len(m.tokens), tok)
	}
}
func (m *LexTokenPager) Cur() lex.Token {
	//u.Debugf("Cur(): %v of %v  %v", m.cursor, len(m.tokens), m.tokens[m.cursor])
	if m.cursor+1 >= len(m.tokens) {
		//panic("WTF, not enough tokens?")
		//u.Warnf("Next() CRAP? increment cursor: %v of %v %v", m.cursor, len(m.tokens), m.cursor < len(m.tokens))
	}
	return m.tokens[m.cursor]
}
func (m *LexTokenPager) Last() lex.TokenType {
	return m.end
}
func (m *LexTokenPager) IsEnd() bool {
	return false
}
func (m *LexTokenPager) ClauseEnd() bool {
	return false
}
func (m *LexTokenPager) Lexer() *lex.Lexer {
	return m.lex
}

// backup backs the input stream up one token.
func (m *LexTokenPager) Backup() {
	if m.cursor > 0 {
		m.cursor--
		//u.Warnf("Backup?: %v", m.cursor)
		return
	}
}

// peek returns but does not consume the next token.
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
		u.Infof("last one?: %v of %v  %v", m.cursor, len(m.tokens), m.tokens[m.cursor])
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
}

func NewTree(pager TokenPager) *Tree {
	t := Tree{TokenPager: pager}
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
	pager.end = lex.TokenEOF

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
	u.LogTracef(u.WARN, "about to panic: %v", msg)
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
	//u.Debugf("parsing: %v", t.Cur())
	t.runCheck = runCheck
	//u.Debugf("parsing: %v", t.Cur())
	t.Root = t.O(0)
	//u.Debugf("after parse()")
	if !t.ClauseEnd() {
		//u.Warnf("Not End? last=%v", t.TokenPager.Last())
		//t.expect(t.TokenPager.Last(), "input")
	}
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
C -> P {( "==" | "!=" | ">" | ">=" | "<" | "<=" | "LIKE" | "IN" ) P}
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
	//u.Debugf("depth:%s t.O Cur(): %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.A(depth)
	//u.Debugf("depth:%s t.O AFTER: n:%v cur:%v ", strings.Repeat("→ ", depth), n, t.Cur())
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
	//u.Debugf("%s t.A: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.C(depth)
	//u.Debugf("%s t.A: AFTER %v", strings.Repeat("→ ", depth), t.Cur())
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
	//u.Debugf("%s t.C: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.P(depth)
	//u.Debugf("%s t.C: %v", strings.Repeat("→ ", depth), t.Cur())
	for {
		//u.Debugf("tok:  cur=%v peek=%v n=%v", t.Cur(), t.Peek(), n)
		switch cur := t.Cur(); cur.T {
		case lex.TokenNegate:
			//u.Infof("doing urnary node on negate: %v", cur)
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
	//u.Debugf("%d t.cInner: %v", depth, t.Cur())
	for {
		//u.Debugf("cInner:  tok:  cur=%v peek=%v n=%v", t.Cur(), t.Peek(), n.StringAST())
		switch cur := t.Cur(); cur.T {
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE, lex.TokenGT, lex.TokenGE,
			lex.TokenLE, lex.TokenLT, lex.TokenLike:
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
			// This isn't really a Binary?   It is an array or
			// other type of native data type?
			//n = NewSet(cur, n, t.Set(depth+1))
			return t.MultiArg(n, cur, depth)
		case lex.TokenNull:
			t.Next()
			return NewNull(cur)
		default:
			return n
		}
	}
}

func (t *Tree) P(depth int) Node {
	//u.Debugf("%s t.P: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.M(depth)
	//u.Debugf("%s t.P: AFTER %v", strings.Repeat("→ ", depth), t.Cur())
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
	//u.Debugf("%s t.M: %v", strings.Repeat("→ ", depth), t.Cur())
	n := t.F(depth)
	//u.Debugf("%s t.M after: %v  %s", strings.Repeat("→ ", depth), t.Cur(), n.NodeType())
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

// MultiArg parses multi-argument clauses like x IN y.
func (t *Tree) MultiArg(first Node, op lex.Token, depth int) Node {
	//u.Debugf("%d t.MultiArg: %v", depth, t.Cur())
	multiNode := NewMultiArgNode(op)
	multiNode.Append(first)
	switch cur := t.Cur(); cur.T {
	case lex.TokenIdentity:
		t.Next() // Consume identity
		multiNode.Append(NewIdentityNode(&cur))
		return multiNode
	case lex.TokenLeftParenthesis:
		// continue
	default:
		t.unexpected(cur, "input")
	}
	t.Next() // Consume Left Paren
	//u.Debugf("%d t.MultiArg after: %v ", depth, t.Cur())
	for {
		//u.Debugf("MultiArg iteration: %v", t.Cur())
		switch cur := t.Cur(); cur.T {
		case lex.TokenRightParenthesis:
			t.Next() // Consume the Paren
			return multiNode
		case lex.TokenComma:
			t.Next()
		default:
			n := t.O(depth)
			if n != nil {
				multiNode.Append(n)
			} else {
				u.Warnf("invalid?  %v", t.Cur())
				return multiNode
			}
		}
	}
}

func (t *Tree) F(depth int) Node {
	//u.Debugf("%s t.F: %v", strings.Repeat("→ ", depth), t.Cur())
	switch cur := t.Cur(); cur.T {
	case lex.TokenUdfExpr:
		return t.v(depth)
	case lex.TokenInteger, lex.TokenFloat:
		return t.v(depth)
	case lex.TokenIdentity:
		return t.v(depth)
	case lex.TokenValue:
		return t.v(depth)
	case lex.TokenNull:
		return t.v(depth)
	// case lex.TokenLeftBrace:
	// 	// {
	// 	return t.v(depth)
	case lex.TokenLeftBracket:
		// [
		return t.v(depth)
	case lex.TokenStar:
		// in special situations:   count(*) ??
		return t.v(depth)
	case lex.TokenNegate, lex.TokenMinus, lex.TokenExists:
		//u.Infof("%s doing unary node on: %v", strings.Repeat("→ ", depth), cur)
		t.Next()
		n := NewUnary(cur, t.F(depth+1))
		//u.Infof("%s returning unary node: %v", strings.Repeat("→ ", depth), cur)
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
		//u.Debugf("expects right paren? cur=%v p=%v", t.Cur(), t.Peek())
		t.expect(lex.TokenRightParenthesis, "input")
		t.Next()
		return n
	default:
		u.Warnf("unexpected? %v", cur)
		//t.unexpected(cur, "input")
		panic(fmt.Sprintf("unexpected token %v ", cur))
	}
	return nil
}

func (t *Tree) v(depth int) Node {
	//u.Debugf("depth:%d t.v: cur(): %v   peek:%v", depth, t.Cur(), t.Peek())
	switch cur := t.Cur(); cur.T {
	case lex.TokenInteger, lex.TokenFloat:
		n, err := NewNumberStr(cur.V)
		if err != nil {
			t.error(err)
		}
		t.Next()
		return n
	case lex.TokenValue:
		n := NewStringNode(cur.V)
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
	// case lex.TokenLeftBrace:
	// 	// {
	// 	return t.v(depth)
	case lex.TokenLeftBracket:
		// [
		t.Next() // Consume the [
		arrayVal, err := valueArray(t.TokenPager)
		if err != nil {
			t.unexpected(t.Cur(), "jsonarray")
			return nil
		}
		n := NewValueNode(arrayVal)
		//u.Infof("what is next token?  %v peek:%v   str=%s", t.Cur(), t.Peek(), n.String())
		//t.Next()
		return n
	case lex.TokenUdfExpr:
		//u.Debugf("depth:%v t.v calling Func()?: %v", depth, cur)
		t.Next() // consume Function Name
		//u.Debugf("func? %v", funcTok)
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
	//u.Debugf("Func tok: %v cur:%v peek:%v", funcTok.V, t.Cur().V, t.Peek().V)
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
	//u.Debugf("%d t.Func()?: %v %v", depth, t.Cur(), t.Peek())
	//t.Next() // step forward to hopefully left paren
	t.expect(lex.TokenLeftParenthesis, "func")

	for {
		node = nil
		t.Next() // Are we sure we consume?
		//u.Infof("%d pre loop token?: cur=%v peek=%v", depth, t.Cur(), t.Peek())
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
			//t.Next() // ??
			continue
		default:
			//u.Debugf("%v getting node? t.Func()?: %v", depth, firstToken)
			node = t.O(depth + 1)
		}

		tok = t.Cur()
		//u.Infof("%d Func() pt2 consumed token?: %v", depth, tok)
		switch tok.T {
		case lex.TokenComma:
			if node != nil {
				fn.append(node)
			}
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
	}
}

// get Function from Global
func (t *Tree) getFunction(name string) (v Func, ok bool) {
	if v, ok = funcs[strings.ToLower(name)]; ok {
		return
	}
	return
}

func valueArray(pg TokenPager) (value.Value, error) {

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
		case lex.TokenFloat, lex.TokenInteger:
			fv, err := strconv.ParseFloat(tok.V, 64)
			if err == nil {
				vals = append(vals, value.NewNumberValue(fv))
			}
			return value.NilValueVal, err
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

func (t *Tree) String() string {
	return t.Root.String()
}
