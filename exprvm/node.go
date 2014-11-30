package exprvm

import (
	"fmt"
	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser"
	"reflect"
	"strconv"
)

var _ = u.EMPTY

// A Node is an element in the parse tree, to be implemented by specific NodeTypes
//
type Node interface {
	// string representation of internals
	String() string

	// The Marshalled AST value, should match original input
	StringAST() string

	// byte position of start of node in full original input string
	Position() Pos

	// performs type checking for itself and sub-nodes
	Check() error

	// describes the return type
	Type() reflect.Value
}

// An argument to an expression can be either a Value or a Node
type ExprArg interface {
	Type() reflect.Value
}

// Pos represents a byte position in the original input text which was parsed
type Pos int

func (p Pos) Position() Pos { return p }

// FuncNode holds a function invocation
type FuncNode struct {
	Pos
	Name string // Name of func
	F    Func   // The actual function that this AST maps to
	Args []Node // Arguments are them selves nodes
}

func NewFuncNode(pos Pos, name string, f Func) *FuncNode {
	return &FuncNode{Pos: pos, Name: name, F: f}
}

func (c *FuncNode) append(arg Node) {
	c.Args = append(c.Args, arg)
}

func (c *FuncNode) String() string {
	s := c.Name + "("
	for i, arg := range c.Args {
		if i > 0 {
			s += ", "
		}
		s += arg.String()
	}
	s += ")"
	return s
}

func (c *FuncNode) StringAST() string {
	s := c.Name + "("
	for i, arg := range c.Args {
		if i > 0 {
			s += ", "
		}
		s += arg.StringAST()
	}
	s += ")"
	return s
}

func (c *FuncNode) Check() error {

	if len(c.Args) < len(c.F.Args) {
		return fmt.Errorf("parse: not enough arguments for %s", c.Name)
	} else if len(c.Args) > len(c.F.Args) {
		return fmt.Errorf("parse: too many arguments for %s want:%v got:%v   %#v", c.Name, len(c.F.Args), len(c.Args), c.Args)
	}
	for i, a := range c.Args {
		switch a.(type) {
		case Node:
			if err := a.Check(); err != nil {
				return err
			}
		case Value:
			// TODO: we need to check co-ercion here, ie which Args can be converted to what types

			// For Env Variables, we need to Check those (On Definition?)
			if c.F.Args[i].Kind() != a.Type().Kind() {
				u.Errorf("error in parse Check(): %v", a)
				return fmt.Errorf("parse: expected %v, got %v    ", a.Type().Kind(), c.F.Args[i].Kind())
			}
			if err := a.Check(); err != nil {
				return err
			}
		}

	}
	return nil
}

func (f *FuncNode) Type() reflect.Value { return f.F.Return }

// NumberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type NumberNode struct {
	Pos
	IsInt   bool    // Number has an integer value.
	IsFloat bool    // Number has a floating-point value.
	Int64   int64   // The integer value.
	Float64 float64 // The floating-point value.
	Text    string  // The original textual representation from the input.
}

func NewNumber(pos Pos, text string) (*NumberNode, error) {
	n := &NumberNode{Pos: pos, Text: text}
	// Do integer test first so we get 0x123 etc.
	u, err := strconv.ParseInt(text, 0, 64) // will fail for -0.
	if err == nil {
		n.IsInt = true
		n.Int64 = u
	}
	// If an integer extraction succeeded, promote the float.
	if n.IsInt {
		n.IsFloat = true
		n.Float64 = float64(n.Int64)
	} else {
		f, err := strconv.ParseFloat(text, 64)
		if err == nil {
			n.IsFloat = true
			n.Float64 = f
			// If a floating-point extraction succeeded, extract the int if needed.
			if !n.IsInt && float64(int64(f)) == f {
				n.IsInt = true
				n.Int64 = int64(f)
			}
		}
	}
	if !n.IsInt && !n.IsFloat {
		return nil, fmt.Errorf("illegal number syntax: %q", text)
	}
	return n, nil
}

func (n *NumberNode) String() string {
	return n.Text
}

func (n *NumberNode) StringAST() string {
	return n.String()
}

func (n *NumberNode) Check() error {
	return nil
}

func (n *NumberNode) Type() reflect.Value { return floatRv }

// StringNode holds a string constant, quotes not included
type StringNode struct {
	Pos
	Text string
}

func NewStringNode(pos Pos, text string) *StringNode {
	return &StringNode{Pos: pos, Text: text}
}
func (m *StringNode) String() string      { return m.Text }
func (m *StringNode) StringAST() string   { return m.String() }
func (m *StringNode) Check() error        { return nil }
func (m *StringNode) Type() reflect.Value { return stringRv }

// IdentityNode will look up a value out of a env bag
type IdentityNode struct {
	Pos
	Text string
}

func NewIdentityNode(pos Pos, text string) *IdentityNode {
	return &IdentityNode{Pos: pos, Text: text}
}

func (m *IdentityNode) String() string      { return m.Text }
func (m *IdentityNode) StringAST() string   { return m.String() }
func (m *IdentityNode) Check() error        { return nil }
func (s *IdentityNode) Type() reflect.Value { return stringRv }

// BinaryNode holds two arguments and an operator
/*
binary_op  = "||" | "&&" | rel_op | add_op | mul_op .
rel_op     = "==" | "!=" | "<" | "<=" | ">" | ">=" .
add_op     = "+" | "-" | "|" | "^" .
mul_op     = "*" | "/" | "%" | "<<" | ">>" | "&" | "&^" .

unary_op   = "+" | "-" | "!" | "^" | "*" | "&" | "<-" .
*/
type BinaryNode struct {
	Pos
	Args     [2]ExprArg
	Operator ql.Token
}

func NewBinary(operator ql.Token, arg1, arg2 ExprArg) *BinaryNode {
	return &BinaryNode{Pos: Pos(operator.Pos), Args: [2]ExprArg{arg1, arg2}, Operator: operator}
}

func (b *BinaryNode) String() string {
	return fmt.Sprintf("%s %s %s", b.Args[0], b.Operator.V, b.Args[1])
}

func (b *BinaryNode) StringAST() string {
	return fmt.Sprintf("%s(%s, %s)", b.Operator.V, b.Args[0], b.Args[1])
}

func (b *BinaryNode) Check() error {
	// do all args support Binary Operations?   Does that make sense or not?
	// if not we need to implement type checking
	// type0 := b.Args[0].Type()
	// type1 := b.Args[1].Type()
	// if !canCoerce(type0, type1) {
	// 	return fmt.Errorf("Cannot coerce %v into %v", type1, type0)
	// }
	return nil
	// if err := b.Args[0].Check(); err != nil {
	// 	return err
	// }
	// return b.Args[1].Check()
}

func (b *BinaryNode) Type() reflect.Value {
	// switch t := b.Args[0].(type) {
	// case Node:
	// 	return t.Type()
	// case Value:
	// 	return t.Type()
	// default:
	// 	panic(fmt.Sprintf("Unknown node type: %v", t))
	// }
	return b.Args[0].Type()

}

// UnaryNode holds one argument and an operator.
type UnaryNode struct {
	Pos
	Arg      ExprArg
	Operator ql.Token
}

func NewUnary(operator ql.Token, arg ExprArg) *UnaryNode {
	return &UnaryNode{Pos: Pos(operator.Pos), Arg: arg, Operator: operator}
}

func (n *UnaryNode) String() string {
	return fmt.Sprintf("%s%s", n.Operator.V, n.Arg)
}

func (n *UnaryNode) StringAST() string {
	return fmt.Sprintf("%s(%s)", n.Operator.V, n.Arg)
}

func (n *UnaryNode) Check() error {
	switch t := n.Arg.(type) {
	case Node:
		return t.Check()
	case Value:
		//return t.Type()
		return nil
	default:
		return fmt.Errorf("parse: type error in expected? got %v", t)
	}
}

func (n *UnaryNode) Type() reflect.Value {
	return n.Arg.Type()
}

// Walk invokes f on n and sub-nodes of n.
func Walk(arg ExprArg, f func(Node)) {

	switch argType := arg.(type) {
	case Node:
		f(argType)

		switch n := arg.(type) {
		case *BinaryNode:
			Walk(n.Args[0], f)
			Walk(n.Args[1], f)
		case *FuncNode:
			for _, a := range n.Args {
				Walk(a, f)
			}
		case *NumberNode, *StringNode:
			// Ignore
		case *IdentityNode:
			//Walk(n.Arg, f)
		case *UnaryNode:
			Walk(n.Arg, f)
		default:
			panic(fmt.Errorf("other type: %T", n))
		}
	case Value:
		// continue
	default:
		panic(fmt.Errorf("other type: %T", arg))
	}

}
