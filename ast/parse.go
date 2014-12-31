package ast

import (
	"fmt"
	"runtime"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
)

var _ = u.EMPTY

// We have a default Dialect, which is the "Language" or rule-set of ql
var DefaultDialect *lex.Dialect = lex.LogicalExpressionDialect

// TokenPager wraps a Lexer, and implements the Logic to determine what is
// the end of this particular Expression
//
//    SELECT * FROM X   --   keyword FROM identifies end of columns
//    SELECT x, y, cast(item,string) AS item_str FROM product  -- commas, FROM are end of columns
//
type TokenPager interface {
	Peek() lex.Token
	Next() lex.Token
	Last() lex.TokenType
	Backup()
	IsEnd() bool
}

// SchemaInfo
//
type SchemaInfo interface {
	Key() string
}

type ExpressionPager struct {
	token     [1]lex.Token // one-token lookahead for parser
	peekCount int
	lex       *lex.Lexer
	end       lex.TokenType
}

func NewExpressionPager(lex *lex.Lexer) *ExpressionPager {
	return &ExpressionPager{
		lex: lex,
	}
}

func (m *ExpressionPager) SetCurrent(tok lex.Token) {
	m.peekCount = 1
	m.token[0] = tok
}

// next returns the next token.
func (m *ExpressionPager) Next() lex.Token {
	if m.peekCount > 0 {
		m.peekCount--
	} else {
		m.token[0] = m.lex.NextToken()
	}
	return m.token[m.peekCount]
}
func (m *ExpressionPager) Last() lex.TokenType {
	return m.end
}
func (m *ExpressionPager) IsEnd() bool {
	return false
}

// backup backs the input stream up one token.
func (m *ExpressionPager) Backup() {
	if m.peekCount > 0 {
		//u.Warnf("PeekCount?  %v: %v", m.peekCount, m.token)
		return
	}
	m.peekCount++
}

// peek returns but does not consume the next token.
func (m *ExpressionPager) Peek() lex.Token {

	if m.peekCount > 0 {
		//u.Infof("peek:  %v: len=%v", m.peekCount, len(m.token))
		return m.token[m.peekCount-1]
	}
	m.peekCount = 1
	m.token[0] = m.lex.NextToken()
	//u.Infof("peek:  %v: len=%v %v", m.peekCount, len(m.token), m.token[0])
	return m.token[0]
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
	pager := NewExpressionPager(l)
	t := NewTree(pager)
	pager.end = lex.TokenEOF
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

// expect consumes the next token and guarantees it has the required type.
func (t *Tree) expect(expected lex.TokenType, context string) lex.Token {
	token := t.Next()
	//u.Debugf("checking expected? token? %v", token)
	if token.T != expected {
		u.Warnf("unexpeted token? %v want:%v", token, expected)
		t.unexpected(token, context)
	}
	return token
}

// expectOneOf consumes the next token and guarantees it has one of the required types.
func (t *Tree) expectOneOf(expected1, expected2 lex.TokenType, context string) lex.Token {
	token := t.Next()
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
func (t *Tree) BuildTree(runCheck bool) error {
	//u.Debugf("parsing: %v", t.Text)
	t.runCheck = runCheck
	t.Root = t.O()
	//u.Debugf("after parse()")
	if !t.IsEnd() {
		//u.Warnf("Not End?")
		t.expect(t.TokenPager.Last(), "input")
	}
	if runCheck {
		if err := t.Root.Check(); err != nil {
			u.Errorf("found error: %v", err)
			t.error(err)
			return err
		}
	}

	return nil
}

/*

Operator Predence planner during parse phase:
  when we parse and build our node-sub-node structures we need to plan
  the precedence rules, we use a recursion tree to build this

http://dev.mysql.com/doc/refman/5.0/en/operator-precedence.html
https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence

TODO:
 - implement new one for parens
 - implement flags for commutative/
--------------------------------------
O -> A {"||" A}
A -> C {"&&" C}
C -> P {( "==" | "!=" | ">" | ">=" | "<" | "<=" | "LIKE" | "IN" ) P}
P -> M {( "+" | "-" ) M}
M -> F {( "*" | "/" ) F}
F -> v | "(" O ")" | "!" O | "-" O
v -> number | func(..)
Func -> name "(" param {"," param} ")"
param -> number | "string" | O


!
- (unary minus), ~ (unary bit inversion)
*, /, DIV, %, MOD
-, +
<<, >>
&
|
= (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, REGEXP, IN
BETWEEN, CASE, WHEN, THEN, ELSE
NOT
&&, AND
XOR
||, OR
= (assignment), :=

*/

// expr:
func (t *Tree) O() Node {
	//u.Debugf("t.O: %v", t.Peek())
	n := t.A()
	//u.Debugf("t.O AFTER:  %v", n)
	for {
		tok := t.Peek()
		//u.Debugf("tok:  %v", tok)
		switch tok.T {
		case lex.TokenLogicOr, lex.TokenOr:
			n = NewBinary(t.Next(), n, t.A())
		case lex.TokenCommentSingleLine:
			// hm....
			//u.Debugf("tok:  %v", t.Next())
			//u.Debugf("tok:  %v", t.Next())
			t.Next()
			t.Next()
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenComma, lex.TokenIf,
			lex.TokenAs:
			//u.Debugf("return: %v", t.Peek())
			return n
		default:
			//u.Debugf("root couldnt evaluate node? %v", tok)
			return n
		}
	}
}

func (t *Tree) A() Node {
	//u.Debugf("t.A: %v", t.Peek())
	n := t.C()
	//u.Debugf("t.A: AFTER %v", t.Peek())
	for {
		switch tok := t.Peek(); tok.T {
		case lex.TokenLogicAnd, lex.TokenAnd:
			n = NewBinary(t.Next(), n, t.C())
		default:
			return n
		}
	}
}

func (t *Tree) C() Node {
	//u.Debugf("t.C: %v", t.Peek())
	n := t.P()
	//u.Debugf("t.C: %v", t.Peek())
	for {
		switch t.Peek().T {
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE, lex.TokenGT, lex.TokenGE,
			lex.TokenLE, lex.TokenLT, lex.TokenLike, lex.TokenIN:
			n = NewBinary(t.Next(), n, t.P())
		default:
			return n
		}
	}
}

func (t *Tree) P() Node {
	//u.Debugf("t.P: %v", t.Peek())
	n := t.M()
	//u.Debugf("t.P: AFTER %v", t.Peek())
	for {
		switch t.Peek().T {
		case lex.TokenPlus, lex.TokenMinus:
			n = NewBinary(t.Next(), n, t.M())
		default:
			return n
		}
	}
}

func (t *Tree) M() Node {
	//u.Debugf("t.M: %v", t.Peek())
	n := t.F()
	//u.Debugf("t.M after: %v  %v", t.Peek(), n)
	for {
		switch t.Peek().T {
		case lex.TokenStar, lex.TokenMultiply, lex.TokenDivide, lex.TokenModulus:
			n = NewBinary(t.Next(), n, t.F())
		default:
			return n
		}
	}
}

func (t *Tree) F() Node {
	//u.Debugf("t.F: %v", t.Peek())
	switch token := t.Peek(); token.T {
	case lex.TokenUdfExpr:
		return t.v()
	case lex.TokenInteger, lex.TokenFloat:
		return t.v()
	case lex.TokenIdentity:
		return t.v()
	case lex.TokenValue:
		return t.v()
	case lex.TokenNegate, lex.TokenMinus:
		return NewUnary(t.Next(), t.F())
	case lex.TokenLeftParenthesis:
		t.Next()
		n := t.O()
		if bn, ok := n.(*BinaryNode); ok {
			bn.Paren = true
		}
		//u.Debugf("n %v  ", n.StringAST())
		t.expect(lex.TokenRightParenthesis, "input")
		return n
	default:
		u.Warnf("unexpected? %v", t.Peek())
		t.unexpected(token, "input")
	}
	return nil
}

func (t *Tree) v() Node {
	token := t.Next()
	//u.Debugf("t.v: next: %v   peek:%v", token, t.Peek())
	switch token.T {
	case lex.TokenInteger, lex.TokenFloat:
		n, err := NewNumber(Pos(token.Pos), token.V)
		if err != nil {
			t.error(err)
		}
		//u.Debugf("return number node: %v", token)
		return n
	case lex.TokenValue:
		n := NewStringNode(Pos(token.Pos), token.V)
		return n
	case lex.TokenIdentity:
		n := NewIdentityNode(Pos(token.Pos), token.V)
		return n
	case lex.TokenUdfExpr:
		//u.Debugf("t.v calling Func()?: %v", token)
		t.Backup()
		return t.Func(token)
	default:
		if t.IsEnd() {
			return nil
		}
		u.Warnf("Unexpected?: %v", token)
		t.unexpected(token, "input")
	}
	return nil
}

func (t *Tree) Func(tok lex.Token) (fn *FuncNode) {
	//u.Debugf("Func tok: %v peek:%v", tok, t.Peek())
	var token lex.Token
	if t.Peek().T == lex.TokenLeftParenthesis {
		token = tok
	} else {
		token = t.Next()
	}

	var node Node
	//var err error

	funcImpl, ok := t.getFunction(token.V)
	if !ok {
		if t.runCheck {
			u.Warnf("non func? %v", token.V)
			t.errorf("non existent function %s", token.V)
		} else {
			// if we aren't testing for validity, make a "fake" func
			// we may not be using vm, just ast
			funcImpl = Func{Name: token.V}
		}
	}
	fn = NewFuncNode(Pos(token.Pos), token.V, funcImpl)
	//u.Debugf("t.Func()?: %v", token)
	t.expect(lex.TokenLeftParenthesis, "func")

	for {
		node = nil
		firstToken := t.Peek()
		switch firstToken.T {
		case lex.TokenRightParenthesis:
			t.Next()
			if node != nil {
				fn.append(node)
			}
			return
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom:
			u.Debugf("return: %v", t.Peek())
			if node != nil {
				fn.append(node)
			}
			return
		default:
			//u.Debugf("getting node? t.Func()?: %v", firstToken)
			node = t.O()
		}

		switch token = t.Next(); token.T {
		case lex.TokenComma:
			if node != nil {
				fn.append(node)
			}
			// continue
		case lex.TokenRightParenthesis:
			if node != nil {
				fn.append(node)
			}
			return
		case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom:
			u.Debugf("return: %v", t.Peek())
			if node != nil {
				fn.append(node)
			}
			return
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE, lex.TokenGT, lex.TokenGE,
			lex.TokenLE, lex.TokenLT, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide:
			// this func arg is an expression
			//     toint(str_item * 5)

			t.Backup()
			u.Debugf("hmmmmm:  %v  peek=%v", token, t.Peek())
			node = t.O()
			if node != nil {
				fn.append(node)
			}
		default:
			t.unexpected(token, "func")
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

func (t *Tree) String() string {
	return t.Root.String()
}
