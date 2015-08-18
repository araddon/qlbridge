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

	// Standard errors
	ErrNotSupported   = fmt.Errorf("qlbridge Not supported")
	ErrNotImplemented = fmt.Errorf("qlbridge Not implemented")
	ErrUnknownCommand = fmt.Errorf("qlbridge Unknown Command")
	ErrInternalError  = fmt.Errorf("qlbridge Internal Error")
)

type NodeType uint8

const (
	NodeNodeType        NodeType = 1
	FuncNodeType        NodeType = 2
	IdentityNodeType    NodeType = 3
	StringNodeType      NodeType = 4
	NumberNodeType      NodeType = 5
	ValueNodeType       NodeType = 8
	BinaryNodeType      NodeType = 10
	UnaryNodeType       NodeType = 11
	TriNodeType         NodeType = 13
	MultiArgNodeType    NodeType = 14
	NullNodeType        NodeType = 15
	SqlPreparedType     NodeType = 29
	SqlSelectNodeType   NodeType = 30
	SqlInsertNodeType   NodeType = 31
	SqlUpdateNodeType   NodeType = 32
	SqlUpsertNodeType   NodeType = 33
	SqlDeleteNodeType   NodeType = 35
	SqlDescribeNodeType NodeType = 40
	SqlShowNodeType     NodeType = 41
	SqlCommandNodeType  NodeType = 42
	SqlCreateNodeType   NodeType = 50
	SqlSourceNodeType   NodeType = 55
	SqlWhereNodeType    NodeType = 56
	SqlIntoNodeType     NodeType = 57
	SqlJoinNodeType     NodeType = 58
	//SetNodeType         NodeType = 12
)

type (

	// A Node is an element in the expression tree, implemented
	// by different types (string, binary, urnary, func, case, etc)
	//
	Node interface {
		// string representation of Node, AST parseable back to itself
		String() string

		// performs type checking for itself and sub-nodes, evaluates
		// validity of the expression/node in advance of evaluation
		Check() error

		// describes the Node type, faster than interface casting
		NodeType() NodeType
	}

	ParsedNode interface {
		Finalize() error
	}

	// Node that has a Type Value
	NodeValueType interface {
		// describes the return type
		Type() reflect.Value
	}

	// Eval context, used to contain info for usage/lookup at runtime evaluation
	EvalContext interface {
		ContextReader
	}

	// Context Reader is interface to read the context of message/row/command
	//  being evaluated
	ContextReader interface {
		Get(key string) (value.Value, bool)
		Row() map[string]value.Value
		Ts() time.Time
	}

	// For evaluation storage
	ContextWriter interface {
		Put(col SchemaInfo, readCtx ContextReader, v value.Value) error
		Delete(row map[string]value.Value) error
	}

	// for commiting row ops (insert, update)
	RowWriter interface {
		Commit(rowInfo []SchemaInfo, row RowWriter) error
		Put(col SchemaInfo, readCtx ContextReader, v value.Value) error
	}
)

type (
	// Describes a function which wraps and allows native go functions
	//  to be called (via reflection) via scripting
	//
	Func struct {
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
	FuncNode struct {
		Name string // Name of func
		F    Func   // The actual function that this AST maps to
		Args []Node // Arguments are them-selves nodes
	}

	// IdentityNode will look up a value out of a env bag
	//  also identities of sql objects (tables, columns, etc)
	//  we often need to rewrite these as in sql it is `table.column`
	IdentityNode struct {
		Quote byte
		Text  string
		left  string
		right string
	}

	// StringNode holds a value literal, quotes not included
	StringNode struct {
		Text string
	}

	NullNode struct{}

	// NumberNode holds a number: signed or unsigned integer or float.
	// The value is parsed and stored under all the types that can represent the value.
	// This simulates in a small amount of code the behavior of Go's ideal constants.
	NumberNode struct {
		IsInt   bool    // Number has an integer value.
		IsFloat bool    // Number has a floating-point value.
		Int64   int64   // The integer value.
		Float64 float64 // The floating-point value.
		Text    string  // The original textual representation from the input.
	}

	// Value holds a value.Value type
	//   value.Values can be strings, numbers, arrays, objects, etc
	ValueNode struct {
		Value value.Value
		rv    reflect.Value
	}

	// Binary node is   x op y, two nodes (left, right) and an operator
	// operators can be a variety of:
	//    +, -, *, %, /,
	// Also, parenthesis may wrap these
	BinaryNode struct {
		Paren    bool
		Args     [2]Node
		Operator lex.Token
	}

	// Tri Node
	//    ARG1 Between ARG2 AND ARG3
	TriNode struct {
		Args     [3]Node
		Operator lex.Token
	}

	// UnaryNode holds one argument and an operator
	//    !eq(5,6)
	//    !true
	//    !(true OR false)
	//    !toint(now())
	UnaryNode struct {
		Arg      Node
		Operator lex.Token
	}

	// Multi Arg Node
	//    arg0 IN (arg1,arg2.....)
	//    5 in (1,2,3,4)   => false
	MultiArgNode struct {
		Args     []Node
		Operator lex.Token
	}
)

// Recursively descend down a node looking for first Identity Field
//
//     min(year)                 == year
//     eq(min(item), max(month)) == item
func FindIdentityField(node Node) string {

	switch n := node.(type) {
	case *IdentityNode:
		return n.Text
	case *BinaryNode:
		for _, arg := range n.Args {
			return FindIdentityField(arg)
		}
	case *FuncNode:
		for _, arg := range n.Args {
			return FindIdentityField(arg)
		}
	}
	return ""
}

// Recursively descend down a node looking for all Identity Fields
//
//     min(year)                 == {year}
//     eq(min(item), max(month)) == {item, month}
func FindAllIdentityField(node Node) []string {
	return findallidents(node, nil)
}

func findallidents(node Node, current []string) []string {
	switch n := node.(type) {
	case *IdentityNode:
		current = append(current, n.Text)
	case *BinaryNode:
		for _, arg := range n.Args {
			current = findallidents(arg, current)
		}
	case *FuncNode:
		for _, arg := range n.Args {
			current = findallidents(arg, current)
		}
	}
	return current
}

// Recursively descend down a node looking for first Identity Field
//   and combine with outermost expression to create an alias
//
//     min(year)                 == min_year
//     eq(min(year), max(month)) == eq_year
func FindIdentityName(depth int, node Node, prefix string) string {

	switch n := node.(type) {
	case *IdentityNode:
		if prefix == "" {
			return n.Text
		}
		return fmt.Sprintf("%s_%s", prefix, n.Text)
	case *BinaryNode:
		for _, arg := range n.Args {
			return FindIdentityName(depth+1, arg, strings.ToLower(arg.String()))
		}
	case *FuncNode:
		if depth > 10 {
			return ""
		}
		for _, arg := range n.Args {
			return FindIdentityName(depth+1, arg, strings.ToLower(n.F.Name))
		}
	}
	return ""

}

// Infer Value type from Node
func ValueTypeFromNode(n Node) value.ValueType {
	switch nt := n.(type) {
	case *FuncNode:
		return value.UnknownType
	case *StringNode:
		return value.StringType
	case *IdentityNode:
		// ??
		return value.StringType
	case *NumberNode:
		return value.NumberType
	case *BinaryNode:
		switch nt.Operator.T {
		case lex.TokenLogicAnd, lex.TokenLogicOr:
			return value.BoolType
		case lex.TokenMultiply, lex.TokenMinus, lex.TokenAdd, lex.TokenDivide:
			return value.NumberType
		case lex.TokenModulus:
			return value.IntType
		default:
			u.Warnf("NoValueType? %T", n)
		}
	case nil:
		return value.UnknownType
	default:
		u.Warnf("NoValueType? %T", n)
	}
	return value.UnknownType
}

func NewFuncNode(name string, f Func) *FuncNode {
	return &FuncNode{Name: name, F: f}
}

func (c *FuncNode) append(arg Node) {
	c.Args = append(c.Args, arg)
}

func (c *FuncNode) String() string {
	s := c.Name + "("
	for i, arg := range c.Args {
		//u.Debugf("arg: %v   %T %v", arg, arg, arg.String())
		if i > 0 {
			s += ", "
		}
		s += arg.String()
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

// NewNumberStr is a little weird in that this Node accepts string @text
// and uses go to parse into Int, AND Float.
func NewNumberStr(text string) (*NumberNode, error) {
	n := &NumberNode{Text: text}
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
func NewNumber(fv float64) (*NumberNode, error) {
	n := &NumberNode{Float64: fv, IsFloat: true}
	iv := int64(fv)
	if float64(iv) == fv {
		n.IsInt = true
		n.Int64 = iv
	}
	n.Text = strconv.FormatFloat(fv, 'f', 4, 64)
	return n, nil
}

func (n *NumberNode) String() string { return n.Text }
func (n *NumberNode) Check() error {
	return nil
}
func (m *NumberNode) NodeType() NodeType  { return NumberNodeType }
func (n *NumberNode) Type() reflect.Value { return floatRv }

func NewStringNode(text string) *StringNode {
	return &StringNode{Text: text}
}
func (m *StringNode) String() string      { return fmt.Sprintf("%q", m.Text) }
func (m *StringNode) Check() error        { return nil }
func (m *StringNode) NodeType() NodeType  { return StringNodeType }
func (m *StringNode) Type() reflect.Value { return stringRv }

func NewValueNode(val value.Value) *ValueNode {
	return &ValueNode{Value: val, rv: reflect.ValueOf(val)}
}
func (m *ValueNode) String() string      { return m.Value.ToString() }
func (m *ValueNode) Check() error        { return nil }
func (m *ValueNode) NodeType() NodeType  { return ValueNodeType }
func (m *ValueNode) Type() reflect.Value { return m.rv }

func NewIdentityNode(tok *lex.Token) *IdentityNode {
	return &IdentityNode{Text: tok.V, Quote: tok.Quote}
}

func (m *IdentityNode) String() string {
	if m.Quote == 0 {
		return m.Text
	}
	// What about escaping?
	return string(m.Quote) + m.Text + string(m.Quote)
}
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

// Return left, right values if is of form   `table.column` and
// also return true/false for if it even has left/right
func (m *IdentityNode) LeftRight() (string, string, bool) {
	if m.left == "" {
		vals := strings.Split(m.Text, ".")
		if len(vals) == 1 {
			m.left = m.Text
		} else if len(vals) == 2 {
			m.left = vals[0]
			m.right = vals[1]
		} else {
			// ????
			return m.Text, "", false
		}
	}
	return m.left, m.right, m.right != ""
}

func NewNull(operator lex.Token) *NullNode {
	return &NullNode{}
}

func (m *NullNode) String() string      { return "NULL" }
func (n *NullNode) Check() error        { return nil }
func (m *NullNode) NodeType() NodeType  { return NullNodeType }
func (m *NullNode) Type() reflect.Value { return nilRv }

// BinaryNode holds two arguments and an operator
/*
binary_op  = "||" | "&&" | rel_op | add_op | mul_op .
rel_op     = "==" | "!=" | "<" | "<=" | ">" | ">=" .
add_op     = "+" | "-" | "|" | "^" .
mul_op     = "*" | "/" | "%" | "<<" | ">>" | "&" | "&^" .

unary_op   = "+" | "-" | "!" | "^" | "*" | "&" | "<-" .
*/

// Create a Binary node
//   @operator = * + - %/ / && || = ==
//   @operator =  and, or, "is not"
//  @lhArg, rhArg the left, right side of binary
func NewBinaryNode(operator lex.Token, lhArg, rhArg Node) *BinaryNode {
	//u.Debugf("NewBinaryNode: %v %v %v", lhArg, operator, rhArg)
	return &BinaryNode{Args: [2]Node{lhArg, rhArg}, Operator: operator}
}

func (m *BinaryNode) String() string {
	if m.Paren {
		return fmt.Sprintf("(%s %s %s)", m.Args[0].String(), m.Operator.V, m.Args[1].String())
	}
	return fmt.Sprintf("%s %s %s", m.Args[0].String(), m.Operator.V, m.Args[1].String())
}
func (m *BinaryNode) Check() error {
	// do all args support Binary Operations?   Does that make sense or not?
	return nil
}
func (m *BinaryNode) NodeType() NodeType { return BinaryNodeType }
func (m *BinaryNode) Type() reflect.Value {
	if argVal, ok := m.Args[0].(NodeValueType); ok {
		return argVal.Type()
	}
	return boolRv
}

// A simple binary function is one who does not have nested expressions
//  underneath it, ie just value = y
func (m *BinaryNode) IsSimple() bool {
	for _, arg := range m.Args {
		switch arg.NodeType() {
		case IdentityNodeType, StringNodeType:
			// ok
		default:
			u.Warnf("is not simple: %T", arg)
			return false
		}
	}
	return true
}

// Create a Tri node
//   @operator = Between
//  @arg1, @arg2, @arg3
func NewTriNode(operator lex.Token, arg1, arg2, arg3 Node) *TriNode {
	return &TriNode{Args: [3]Node{arg1, arg2, arg3}, Operator: operator}
}
func (m *TriNode) String() string {
	return fmt.Sprintf("%s BETWEEN %s AND %s", m.Args[0].String(), m.Args[1].String(), m.Args[2].String())
}
func (m *TriNode) Check() error        { return nil }
func (m *TriNode) NodeType() NodeType  { return TriNodeType }
func (m *TriNode) Type() reflect.Value { /* ?? */ return boolRv }

// Urnary nodes
//    NOT
//    EXISTS
func NewUnary(operator lex.Token, arg Node) *UnaryNode {
	return &UnaryNode{Arg: arg, Operator: operator}
}

func (m *UnaryNode) String() string {
	switch m.Operator.T {
	case lex.TokenNegate:
		return fmt.Sprintf("NOT %s", m.Arg.String())
	case lex.TokenExists:
		return fmt.Sprintf("EXISTS %s", m.Arg.String())
	}
	return fmt.Sprintf("%s(%s)", m.Operator.V, m.Arg.String())
}
func (n *UnaryNode) Check() error {
	switch t := n.Arg.(type) {
	case Node:
		return t.Check()
	case value.Value:
		return nil
	default:
		return fmt.Errorf("parse: type error in expected? got %v", t)
	}
}
func (m *UnaryNode) NodeType() NodeType  { return UnaryNodeType }
func (m *UnaryNode) Type() reflect.Value { return boolRv }

// Create a Multi Arg node
//   @operator = In
//   @args ....
func NewMultiArgNode(operator lex.Token) *MultiArgNode {
	return &MultiArgNode{Args: make([]Node, 0), Operator: operator}
}
func NewMultiArgNodeArgs(operator lex.Token, args []Node) *MultiArgNode {
	return &MultiArgNode{Args: args, Operator: operator}
}
func (m *MultiArgNode) String() string {
	args := make([]string, len(m.Args)-1)
	for i := 1; i < len(m.Args); i++ {
		args[i-1] = m.Args[i].String()
	}
	return fmt.Sprintf("%s %s (%s)", m.Args[0].String(), m.Operator.V, strings.Join(args, ","))
}
func (m *MultiArgNode) Check() error        { return nil }
func (m *MultiArgNode) NodeType() NodeType  { return MultiArgNodeType }
func (m *MultiArgNode) Type() reflect.Value { /* ?? */ return boolRv }
func (m *MultiArgNode) Append(n Node)       { m.Args = append(m.Args, n) }
