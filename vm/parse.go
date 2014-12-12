// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package parse builds parse trees for expressions as defined by expr. Clients
// should use that package to construct expressions rather than this one, which
// provides shared internal data structures not intended for general use.
package vm

import (
	"fmt"
	"runtime"
	//"strconv"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlbridge/lex"
)

var _ = u.EMPTY

// We have a default Dialect, which is the "Language" or rule-set of ql
var DefaultDialect *ql.Dialect = ql.LogicalExpressionDialect

// Tree is the representation of a single parsed expression
type Tree struct {
	Root      Node        // top-level root node of the tree
	token     [1]ql.Token // one-token lookahead for parser
	peekCount int
	lex       *ql.Lexer
	end       ql.TokenType
}

func NewTree(lex *ql.Lexer) *Tree {
	t := Tree{lex: lex}
	return &t
}

// Parse a single Expression, returning a Tree
//
//    ParseExpression("5 * toint(item_name)")
//
func ParseExpression(expressionText string) (*Tree, error) {
	t := NewTree(ql.NewLexer(expressionText, ql.LogicalExpressionDialect))
	t.end = ql.TokenEOF
	err := t.buildTree()
	return t, err
}

func (t *Tree) SetCurrent(tok ql.Token) {
	t.peekCount = 1
	t.token[0] = tok
}

// next returns the next token.
func (t *Tree) next() ql.Token {
	if t.peekCount > 0 {
		t.peekCount--
	} else {
		t.token[0] = t.lex.NextToken()
	}
	return t.token[t.peekCount]
}

// backup backs the input stream up one token.
func (t *Tree) backup() {
	if t.peekCount > 0 {
		//u.Warnf("PeekCount?  %v: %v", t.peekCount, t.token)
		return
	}
	t.peekCount++
}

// peek returns but does not consume the next token.
func (t *Tree) peek() ql.Token {

	if t.peekCount > 0 {
		//u.Infof("peek:  %v: len=%v", t.peekCount, len(t.token))
		return t.token[t.peekCount-1]
	}
	t.peekCount = 1
	t.token[0] = t.lex.NextToken()
	//u.Infof("peek:  %v: len=%v %v", t.peekCount, len(t.token), t.token[0])
	return t.token[0]
}

// Parsing.

// errorf formats the error and terminates processing.
func (t *Tree) errorf(format string, args ...interface{}) {
	t.Root = nil
	format = fmt.Sprintf("expr: %s", format)
	msg := fmt.Errorf(format, args...)
	//u.LogTracef(u.WARN, "about to panic: %v", msg)
	panic(msg)
}

// error terminates processing.
func (t *Tree) error(err error) {
	t.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (t *Tree) expect(expected ql.TokenType, context string) ql.Token {
	token := t.next()
	u.Debugf("checking expected? token? %v", token)
	if token.T != expected {
		u.Warnf("unexpeted token? %v want:%v", token, expected)
		t.unexpected(token, context)
	}
	return token
}

// expectOneOf consumes the next token and guarantees it has one of the required types.
func (t *Tree) expectOneOf(expected1, expected2 ql.TokenType, context string) ql.Token {
	token := t.next()
	if token.T != expected1 && token.T != expected2 {
		t.unexpected(token, context)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (t *Tree) unexpected(token ql.Token, context string) {
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
func (t *Tree) buildTree() error {
	//u.Debugf("parsing: %v", t.Text)
	t.Root = t.O()
	//u.Debugf("after parse()")
	switch tok := t.peek(); tok.T {
	case ql.TokenEOS, ql.TokenEOF, ql.TokenFrom, ql.TokenComma, ql.TokenAs:
		// ok
		u.Debugf("Found good End: %v", t.peek())
	default:
		u.Warnf("tok? %v", tok)
		t.expect(t.end, "input")
	}

	if err := t.Root.Check(); err != nil {
		u.Errorf("found error: %v", err)
		t.error(err)
		return err
	}
	return nil
}

// buildTree take the tokens and recursively build into expression tree node
func (t *Tree) buildSqlTree() error {
	//u.Debugf("parsing: %v", t.Text)
	t.Root = t.O()
	//u.Debugf("after parse()")
	switch tok := t.peek(); tok.T {
	case ql.TokenEOS, ql.TokenEOF, ql.TokenFrom, ql.TokenComma, ql.TokenAs, ql.TokenIf:
		// ok
		u.Debugf("Found good End: %v", t.peek())
	default:
		u.Warnf("tok? %v", tok)
		t.expect(t.end, "input")
	}

	if err := t.Root.Check(); err != nil {
		u.Errorf("found error: %v", err)
		t.error(err)
		return err
	}
	return nil
}

/*

Operator Predence planner on Parse:
  when we parse and build our node-sub-node structures we need to plan
  the precedence rules, we use a recursion tree to build this

http://dev.mysql.com/doc/refman/5.0/en/operator-precedence.html
https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence

TODO:  implement new one for parens
--------------------------------------
O -> A {"||" A}
A -> C {"&&" C}
C -> P {( "==" | "!=" | ">" | ">=" | "<" | "<=") P}
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
	u.Debugf("t.O: %v", t.peek())
	n := t.A()
	u.Debugf("t.O AFTER:  %v", n)
	for {
		tok := t.peek()
		u.Debugf("tok:  %v", tok)
		switch tok.T {
		case ql.TokenLogicOr, ql.TokenOr:
			n = NewBinary(t.next(), n, t.A())
		case ql.TokenEOF, ql.TokenEOS, ql.TokenFrom, ql.TokenComma, ql.TokenIf:
			u.Debugf("return: %v", t.peek())
			return n
		default:
			u.Debugf("root couldnt evaluate node? %v", tok)
			return n
		}
	}
}

func (t *Tree) A() Node {
	u.Debugf("t.A: %v", t.peek())
	n := t.C()
	u.Debugf("t.A: AFTER %v", t.peek())
	for {
		switch tok := t.peek(); tok.T {
		case ql.TokenLogicAnd, ql.TokenAnd:
			n = NewBinary(t.next(), n, t.C())
		default:
			return n
		}
	}
}

func (t *Tree) C() Node {
	u.Debugf("t.C: %v", t.peek())
	n := t.P()
	u.Debugf("t.C: %v", t.peek())
	for {
		switch t.peek().T {
		case ql.TokenEqual, ql.TokenEqualEqual, ql.TokenNE, ql.TokenGT, ql.TokenGE,
			ql.TokenLE, ql.TokenLT:
			n = NewBinary(t.next(), n, t.P())
		default:
			return n
		}
	}
}

func (t *Tree) P() Node {
	u.Debugf("t.P: %v", t.peek())
	n := t.M()
	u.Debugf("t.P: AFTER %v", t.peek())
	for {
		switch t.peek().T {
		case ql.TokenPlus, ql.TokenMinus:
			n = NewBinary(t.next(), n, t.M())
		default:
			return n
		}
	}
}

func (t *Tree) M() Node {
	u.Debugf("t.M: %v", t.peek())
	n := t.F()
	u.Debugf("t.M after: %v  %v", t.peek(), n)
	for {
		switch t.peek().T {
		case ql.TokenStar, ql.TokenMultiply, ql.TokenDivide, ql.TokenModulus:
			n = NewBinary(t.next(), n, t.F())
		default:
			return n
		}
	}
}

func (t *Tree) F() Node {
	u.Debugf("t.F: %v", t.peek())
	switch token := t.peek(); token.T {
	case ql.TokenUdfExpr:
		return t.v()
	case ql.TokenInteger, ql.TokenFloat:
		return t.v()
	case ql.TokenIdentity:
		return t.v()
	case ql.TokenValue:
		return t.v()
	case ql.TokenNegate, ql.TokenMinus:
		return NewUnary(t.next(), t.F())
	case ql.TokenLeftParenthesis:
		t.next()
		n := t.O()
		u.Debugf("n %v", n)
		t.expect(ql.TokenRightParenthesis, "input")
		return n
	default:
		u.Warnf("unexpected? %v", t.peek())
		t.unexpected(token, "input")
	}
	return nil
}

func (t *Tree) v() Node {
	token := t.next()
	u.Debugf("t.v: next: %v   peek:%v", token, t.peek())
	switch token.T {
	case ql.TokenInteger, ql.TokenFloat:
		n, err := NewNumber(Pos(token.Pos), token.V)
		if err != nil {
			t.error(err)
		}
		//u.Debugf("return number node: %v", token)
		return n
	case ql.TokenValue:
		n := NewStringNode(Pos(token.Pos), token.V)
		return n
	case ql.TokenIdentity:
		n := NewIdentityNode(Pos(token.Pos), token.V)
		return n
	case ql.TokenUdfExpr:
		u.Debugf("t.v calling Func()?: %v", token)
		t.backup()
		return t.Func(token)
	default:
		u.Warnf("Unexpected?: %v", token)
		t.unexpected(token, "input")
	}
	return nil
}

func (t *Tree) Func(tok ql.Token) (fn *FuncNode) {
	u.Debugf("Func tok: %v peek:%v", tok, t.peek())
	var token ql.Token
	if t.peek().T == ql.TokenLeftParenthesis {
		token = tok
	} else {
		token = t.next()
	}

	var node Node
	//var err error

	funcImpl, ok := t.getFunction(token.V)
	if !ok {
		u.Warnf("non func? %v", token.V)
		t.errorf("non existent function %s", token.V)
	}
	fn = NewFuncNode(Pos(token.Pos), token.V, funcImpl)
	u.Debugf("t.Func()?: %v", token)
	t.expect(ql.TokenLeftParenthesis, "func")

	for {
		node = nil
		firstToken := t.peek()
		switch firstToken.T {
		case ql.TokenRightParenthesis:
			t.next()
			if node != nil {
				fn.append(node)
			}
			return
		case ql.TokenEOF, ql.TokenEOS, ql.TokenFrom:
			u.Debugf("return: %v", t.peek())
			if node != nil {
				fn.append(node)
			}
			return
		default:
			u.Debugf("getting node? t.Func()?: %v", firstToken)
			node = t.O()
		}

		switch token = t.next(); token.T {
		case ql.TokenComma:
			if node != nil {
				fn.append(node)
			}
			// continue
		case ql.TokenRightParenthesis:
			if node != nil {
				fn.append(node)
			}
			return
		case ql.TokenEOF, ql.TokenEOS, ql.TokenFrom:
			u.Debugf("return: %v", t.peek())
			if node != nil {
				fn.append(node)
			}
			return
		case ql.TokenEqual, ql.TokenEqualEqual, ql.TokenNE, ql.TokenGT, ql.TokenGE,
			ql.TokenLE, ql.TokenLT, ql.TokenStar, ql.TokenMultiply, ql.TokenDivide:
			// this func arg is an expression
			//     toint(str_item * 5)

			t.backup()
			u.Debugf("hmmmmm:  %v  peek=%v", token, t.peek())
			node = t.O()
			if node != nil {
				fn.append(node)
			}
		default:
			t.unexpected(token, "func")
		}
	}
}

func (t *Tree) FuncOld(tok ql.Token) (fn *FuncNode) {
	u.Debugf("Func tok: %v peek:%v", tok, t.peek())
	var token ql.Token
	if t.peek().T == ql.TokenLeftParenthesis {
		token = tok
	} else {
		token = t.next()
	}

	var node Node
	var err error

	funcImpl, ok := t.getFunction(token.V)
	if !ok {
		u.Warnf("non func? %v", token.V)
		t.errorf("non existent function %s", token.V)
	}
	fn = NewFuncNode(Pos(token.Pos), token.V, funcImpl)
	u.Debugf("t.Func()?: %v", token)
	t.expect(ql.TokenLeftParenthesis, "func")

	for {
		node = nil
		switch token = t.next(); token.T {
		case ql.TokenValue:
			node = NewStringNode(Pos(token.Pos), token.V)
		case ql.TokenInteger:
			node, err = NewNumber(Pos(token.Pos), token.V)
			if err != nil {
				// what do we do?
				u.Errorf("error:%v", err)
			}
		case ql.TokenFloat:
			node, err = NewNumber(Pos(token.Pos), token.V)
			if err != nil {
				// what do we do?
				u.Errorf("error:%v", err)
			}
		case ql.TokenIdentity:
			node = NewIdentityNode(Pos(token.Pos), token.V)
			u.Debugf("identity arg in t.Func()?: %v", token)
		default:
			u.Warnf("missing token? t.Func()?: %v", token)
			t.backup()
			node = t.O()
		}

		switch token = t.next(); token.T {
		case ql.TokenComma:
			if node != nil {
				fn.append(node)
			}
			// continue
		case ql.TokenRightParenthesis:
			if node != nil {
				fn.append(node)
			}
			return
		case ql.TokenEOF, ql.TokenEOS, ql.TokenFrom:
			u.Debugf("return: %v", t.peek())
			if node != nil {
				fn.append(node)
			}
			return
		case ql.TokenEqual, ql.TokenEqualEqual, ql.TokenNE, ql.TokenGT, ql.TokenGE,
			ql.TokenLE, ql.TokenLT, ql.TokenStar, ql.TokenMultiply, ql.TokenDivide:
			// this func arg is an expression
			//     toint(str_item * 5)

			t.backup()
			u.Debugf("hmmmmm:  %v  peek=%v", token, t.peek())
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
	if v, ok = funcs[name]; ok {
		return
	}
	return
}

func (t *Tree) String() string {
	return t.Root.String()
}
