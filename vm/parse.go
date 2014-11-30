// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package parse builds parse trees for expressions as defined by expr. Clients
// should use that package to construct expressions rather than this one, which
// provides shared internal data structures not intended for general use.
package exprvm

import (
	"fmt"
	"runtime"
	//"strconv"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser/lex"
)

var _ = u.EMPTY

// We have a default Dialect, which is the "Language" or rule-set of ql
var DefaultDialect *ql.Dialect = ql.LogicalExpressionDialect

// Tree is the representation of a single parsed expression
type Tree struct {
	Text      string      // text parsed to create the expression
	Root      Node        // top-level root of the tree, returns a number
	token     [1]ql.Token // one-token lookahead for parser
	peekCount int
	lex       *ql.Lexer
}

// returns a Tree, created by parsing the expression described in the
// argument string. If an error is encountered, parsing stops and an empty Tree
// is returned with the error.
func ParseTree(text string) (*Tree, error) {
	t := &Tree{}
	t.Text = text
	err := t.Parse(text)
	return t, err
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
	u.LogTracef(u.WARN, "about to panic: %v", msg)
	panic(msg)
}

// error terminates processing.
func (t *Tree) error(err error) {
	t.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (t *Tree) expect(expected ql.TokenType, context string) ql.Token {
	token := t.next()
	//u.Warnf("checking expected? token? %v", token)
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
func (t *Tree) recoverxx(errp *error) {
	e := recover()
	if e != nil {
		u.Errorf("Recover():  %v", e)
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		if t != nil {
			t.stopParse()
		}
		*errp = e.(error)
	}
	return
}

// startParse initializes the parser, using the lexer.
func (t *Tree) startParse(lex *ql.Lexer) {
	t.Root = nil
	t.lex = lex
}

// stopParse terminates parsing.
func (t *Tree) stopParse() {
	t.lex = nil
}

// Parse parses the expression string to construct a tree representation of
// the expression for execution.
func (t *Tree) Parse(text string) (err error) {
	//defer t.recover(&err)
	t.startParse(ql.NewLexer(text, DefaultDialect))
	t.Text = text
	t.parse()
	t.stopParse()
	return nil
}

// parse is the top-level parser for a template,runs to EOF
func (t *Tree) parse() {
	//u.Debugf("parsing: %v", t.Text)
	t.Root = t.O()
	//u.Infof("after parse()")
	t.expect(ql.TokenEOF, "input")
	if err := t.Root.Check(); err != nil {
		u.Errorf("found error: %v", err)
		t.error(err)
	}
}

/* Grammar:
O -> A {"||" A}
A -> C {"&&" C}
C -> P {( "==" | "!=" | ">" | ">=" | "<" | "<=") P}
P -> M {( "+" | "-" ) M}
M -> F {( "*" | "/" ) F}
F -> v | "(" O ")" | "!" O | "-" O
v -> number | func(..)
Func -> name "(" param {"," param} ")"
param -> number | "string" | [query]
*/

// expr:
func (t *Tree) O() Node {
	//u.Debugf("t.O: %v", t.peek())
	n := t.A()
	//u.Infof("t.O AFTER:  %v", n)
	for {
		tok := t.peek()
		//u.Infof("tok:  %v", tok)
		switch tok.T {
		case ql.TokenLogicOr:
			n = NewBinary(t.next(), n, t.A())
		case ql.TokenEOF, ql.TokenEOS:
			return n
		default:
			u.Warnf("root couldnt evaluate node? %v", tok)
			return n
		}
	}
}

func (t *Tree) A() Node {
	//u.Debugf("t.A: %v", t.peek())
	n := t.C()
	for {
		switch t.peek().T {
		case ql.TokenLogicAnd:
			n = NewBinary(t.next(), n, t.C())
		default:
			return n
		}
	}
}

func (t *Tree) C() Node {
	//u.Debugf("t.C: %v", t.peek())
	n := t.P()
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
	//u.Debugf("t.P: %v", t.peek())
	n := t.M()
	//u.Debugf("t.P: AFTER %v", t.peek())
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
	//u.Debugf("t.M: %v", t.peek())
	n := t.F()
	//u.Debugf("t.M after: %v  %v", t.peek(), n)
	for {
		switch t.peek().T {
		case ql.TokenStar, ql.TokenMultiply, ql.TokenDivide:
			n = NewBinary(t.next(), n, t.F())
		default:
			return n
		}
	}
}

func (t *Tree) F() Node {
	//u.Debugf("t.F: %v", t.peek())
	switch token := t.peek(); token.T {
	case ql.TokenUdfExpr:
		return t.v()
	case ql.TokenInteger, ql.TokenFloat:
		return t.v()
	case ql.TokenIdentity:
		return t.v()
	case ql.TokenNegate, ql.TokenMinus:
		return NewUnary(t.next(), t.F())
	case ql.TokenLeftParenthesis:
		t.next()
		n := t.O()
		t.expect(ql.TokenRightParenthesis, "input")
		return n
	default:
		t.unexpected(token, "input")
	}
	return nil
}

func (t *Tree) v() Node {
	u.Debugf("t.v: %v", t.peek())
	switch token := t.next(); token.T {
	case ql.TokenInteger, ql.TokenFloat:
		n, err := NewNumber(Pos(token.Pos), token.V)
		if err != nil {
			t.error(err)
		}
		//u.Debugf("return number node: %v", token)
		return n
	case ql.TokenIdentity:
		n := NewIdentityNode(Pos(token.Pos), token.V)
		return n
	case ql.TokenUdfExpr:
		//u.Debugf("t.v calling Func()?: %v", token)
		t.backup()
		return t.Func()
	default:
		t.unexpected(token, "input")
	}
	return nil
}

func (t *Tree) Func() (fn *FuncNode) {
	token := t.next()
	funcImpl, ok := t.getFunction(token.V)
	if !ok {
		u.Warnf("non func? %v", token.V)
		t.errorf("non existent function %s", token.V)
	}
	fn = NewFuncNode(Pos(token.Pos), token.V, funcImpl)
	u.Debugf("t.Func()?: %v", token)
	t.expect(ql.TokenLeftParenthesis, "func")
	for {
		switch token = t.next(); token.T {
		case ql.TokenValue:
			fn.append(NewStringNode(Pos(token.Pos), token.V))
		case ql.TokenInteger:
			n, err := NewNumber(Pos(token.Pos), token.V)
			if err != nil {
				// what do we do?
			} else {
				fn.append(n)
			}
		case ql.TokenFloat:
			n, err := NewNumber(Pos(token.Pos), token.V)
			if err != nil {
				// what do we do?
			} else {
				fn.append(n)
			}
		case ql.TokenIdentity:
			identityArg := NewIdentityNode(Pos(token.Pos), token.V)
			u.Debugf("identity arg in t.Func()?: %v", token)
			fn.append(identityArg)
		default:
			u.Debugf("missing token? t.Func()?: %v", token)
			t.backup()
			fn.append(t.O())
		}

		switch token = t.next(); token.T {
		case ql.TokenComma:
			// continue
		case ql.TokenRightParenthesis:
			return
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
