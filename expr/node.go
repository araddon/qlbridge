package expr

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// our DataTypes we support, a limited sub-set of go
	floatRv   = reflect.ValueOf(float64(1.2))
	int64Rv   = reflect.ValueOf(int64(1))
	int32Rv   = reflect.ValueOf(int32(1))
	stringRv  = reflect.ValueOf("hello")
	stringsRv = reflect.ValueOf([]string{"hello"})
	boolRv    = reflect.ValueOf(true)
	mapIntRv  = reflect.ValueOf(map[string]int64{"hello": int64(1)})
	timeRv    = reflect.ValueOf(time.Time{})
	nilRv     = reflect.ValueOf(nil)
)

type NodeType uint8

const (
	NodeNodeType        NodeType = 1
	FuncNodeType        NodeType = 2
	IdentityNodeType    NodeType = 3
	StringNodeType      NodeType = 4
	NumberNodeType      NodeType = 5
	BinaryNodeType      NodeType = 10
	UnaryNodeType       NodeType = 11
	SqlPreparedType     NodeType = 29
	SqlSelectNodeType   NodeType = 30
	SqlInsertNodeType   NodeType = 31
	SqlUpdateNodeType   NodeType = 32
	SqlUpsertNodeType   NodeType = 33
	SqlDeleteNodeType   NodeType = 35
	SqlDescribeNodeType NodeType = 40
	SqlShowNodeType     NodeType = 41
	SqlCreateNodeType   NodeType = 50
)

// A Node is an element in the expression tree, implemented
// by different types
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

	// describes the Node type
	NodeType() NodeType
}

type NodeValueType interface {
	// describes the return type
	Type() reflect.Value
}

// Describes a function
type Func struct {
	Name string
	// The arguments we expect
	Args            []reflect.Value
	VariadicArgs    bool
	Return          reflect.Value
	ReturnValueType value.ValueType
	// The actual Go Function
	F reflect.Value
}

// FuncNode holds a Func, which desribes a go Function as
// well as fulfilling the Pos, String() etc for a Node
//
// interfaces:   Node
type FuncNode struct {
	Pos
	Name string // Name of func
	F    Func   // The actual function that this AST maps to
	Args []Node // Arguments are them-selves nodes
}

// IdentityNode will look up a value out of a env bag
type IdentityNode struct {
	Pos
	Text string
}

// StringNode holds a value literal, quotes not included
type StringNode struct {
	Pos
	Text string
}

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

type BinaryNode struct {
	Pos
	Paren    bool
	Args     [2]Node
	Operator lex.Token
}

// UnaryNode holds one argument and an operator
//    !eq(5,6)
//    !true
//    !(true OR false)
//    !toint(now())
type UnaryNode struct {
	Pos
	Arg      Node
	Operator lex.Token
}

// Pos represents a byte position in the original input text which was parsed
type Pos int

func (p Pos) Position() Pos { return p }

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
		//u.Debugf("arg: %v   %T", arg, arg)
		if i > 0 {
			s += ", "
		}
		s += arg.StringAST()
	}
	s += ")"
	return s
}

func (c *FuncNode) Check() error {

	if len(c.Args) < len(c.F.Args) && !c.F.VariadicArgs {
		return fmt.Errorf("parse: not enough arguments for %s  supplied:%d  f.Args:%v", c.Name, len(c.Args), len(c.F.Args))
	} else if (len(c.Args) >= len(c.F.Args)) && c.F.VariadicArgs {
		// ok
	} else if len(c.Args) > len(c.F.Args) {
		u.Warnf("lenc.Args >= len(c.F.Args?  %v", (len(c.Args) >= len(c.F.Args)))
		err := fmt.Errorf("parse: too many arguments for %s want:%v got:%v   %#v", c.Name, len(c.F.Args), len(c.Args), c.Args)
		u.Errorf("funcNode.Check(): %v", err)
		return err
	}
	for i, a := range c.Args {
		switch a.(type) {
		case Node:
			if err := a.Check(); err != nil {
				return err
			}
		case value.Value:
			// TODO: we need to check co-ercion here, ie which Args can be converted to what types
			if nodeVal, ok := a.(NodeValueType); ok {
				// For Env Variables, we need to Check those (On Definition?)
				if c.F.Args[i].Kind() != nodeVal.Type().Kind() {
					u.Errorf("error in parse Check(): %v", a)
					return fmt.Errorf("parse: expected %v, got %v    ", nodeVal.Type().Kind(), c.F.Args[i].Kind())
				}
				if err := a.Check(); err != nil {
					return err
				}
			}

		}

	}
	return nil
}

func (f *FuncNode) NodeType() NodeType  { return FuncNodeType }
func (f *FuncNode) Type() reflect.Value { return f.F.Return }

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

func (m *NumberNode) NodeType() NodeType  { return NumberNodeType }
func (n *NumberNode) Type() reflect.Value { return floatRv }

func NewStringNode(pos Pos, text string) *StringNode {
	return &StringNode{Pos: pos, Text: text}
}
func (m *StringNode) String() string      { return m.Text }
func (m *StringNode) StringAST() string   { return fmt.Sprintf("%q", m.Text) }
func (m *StringNode) Check() error        { return nil }
func (m *StringNode) NodeType() NodeType  { return StringNodeType }
func (m *StringNode) Type() reflect.Value { return stringRv }

func NewIdentityNode(pos Pos, text string) *IdentityNode {
	return &IdentityNode{Pos: pos, Text: text}
}

func (m *IdentityNode) String() string      { return m.Text }
func (m *IdentityNode) StringAST() string   { return m.Text }
func (m *IdentityNode) Check() error        { return nil }
func (m *IdentityNode) NodeType() NodeType  { return IdentityNodeType }
func (m *IdentityNode) Type() reflect.Value { return stringRv }
func (m *IdentityNode) IsBooleanIdentity() bool {
	val := strings.ToLower(m.Text)
	if val == "true" || val == "false" {
		return true
	}
	return false
}
func (m *IdentityNode) Bool() bool {
	val := strings.ToLower(m.Text)
	if val == "true" {
		return true
	}
	return false
}

// BinaryNode holds two arguments and an operator
/*
binary_op  = "||" | "&&" | rel_op | add_op | mul_op .
rel_op     = "==" | "!=" | "<" | "<=" | ">" | ">=" .
add_op     = "+" | "-" | "|" | "^" .
mul_op     = "*" | "/" | "%" | "<<" | ">>" | "&" | "&^" .

unary_op   = "+" | "-" | "!" | "^" | "*" | "&" | "<-" .
*/

func NewBinary(operator lex.Token, arg1, arg2 Node) *BinaryNode {
	return &BinaryNode{Pos: Pos(operator.Pos), Args: [2]Node{arg1, arg2}, Operator: operator}
}

func (b *BinaryNode) String() string { return b.StringAST() }
func (b *BinaryNode) StringAST() string {
	if b.Paren {
		return fmt.Sprintf("(%s %s %s)", b.Args[0].StringAST(), b.Operator.V, b.Args[1].StringAST())
	}
	return fmt.Sprintf("%s %s %s", b.Args[0].StringAST(), b.Operator.V, b.Args[1].StringAST())
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
func (m *BinaryNode) NodeType() NodeType { return BinaryNodeType }
func (b *BinaryNode) Type() reflect.Value {
	if argVal, ok := b.Args[0].(NodeValueType); ok {
		return argVal.Type()
	}
	return boolRv
}

func NewUnary(operator lex.Token, arg Node) *UnaryNode {
	return &UnaryNode{Pos: Pos(operator.Pos), Arg: arg, Operator: operator}
}

func (m *UnaryNode) String() string    { return fmt.Sprintf("%s%s", m.Operator.V, m.Arg) }
func (m *UnaryNode) StringAST() string { return fmt.Sprintf("%s(%s)", m.Operator.V, m.Arg) }
func (n *UnaryNode) Check() error {
	switch t := n.Arg.(type) {
	case Node:
		return t.Check()
	case value.Value:
		//return t.Type()
		return nil
	default:
		return fmt.Errorf("parse: type error in expected? got %v", t)
	}
}
func (m *UnaryNode) NodeType() NodeType  { return UnaryNodeType }
func (m *UnaryNode) Type() reflect.Value { return boolRv }
