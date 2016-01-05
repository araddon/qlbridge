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

type (

	// A Node is an element in the expression tree, implemented
	// by different types (string, binary, urnary, func, etc)
	//
	//  - qlbridge does not currently implement statements (if, for, switch, etc)
	//    just expressions, and operators
	Node interface {
		// string representation of Node, AST parseable back to itself
		String() string

		// string representation of Node, AST but with values replaced by @rune (? generally)
		//  used to allow statements to be deterministically cached/prepared even without
		//  usage of keyword prepared
		FingerPrint(r rune) string

		// performs type and syntax checking for itself and sub-nodes, evaluates
		// validity of the expression/node in advance of evaluation
		Check() error
	}

	// Node that has a Type Value, similar to a literal, but can
	//  contain value's such as []string, etc
	NodeValueType interface {
		// describes the enclosed value type
		Type() reflect.Value
	}

	// A negateable node requires a special type of String() function due to
	// an enclosing urnary NOT being inserted into middle of string syntax
	//
	//   <expression> [NOT] IN ("a","b")
	//   <expression> [NOT] BETWEEN <expression> AND <expression>
	//   <expression> [NOT] LIKE <expression>
	//   <expression> [NOT] CONTAINS <expression>
	//
	NegateableNode interface {
		StringNegate() string
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
	//    vm writes results to this after evaluation
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
		Name      string
		Aggregate bool // is this aggregate func?
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
		Quote   byte
		Text    string
		escaped string
		left    string
		right   string
	}

	// StringNode holds a value literal, quotes not included
	StringNode struct {
		Text    string
		noQuote bool
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
	//    +, -, *, %, /, LIKE, CONTAINS
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

	// UnaryNode negates a single node argument
	//
	//   (  not <expression>  |   !<expression> )
	//
	//    !eq(5,6)
	//    !true
	//    !(true OR false)
	//    !toint(now())
	UnaryNode struct {
		Arg      Node
		Operator lex.Token
	}

	// Array Node for holding multiple similar elements
	//    arg0 IN (arg1,arg2.....)
	//    5 in (1,2,3,4)
	ArrayNode struct {
		wraptype string //  (   or [
		Args     []Node
	}
)

// Determine if uses datemath
// - only works on right-hand of equation
// - doesn't work on args of a function
func HasDateMath(node Node) bool {

	switch n := node.(type) {
	case *BinaryNode:
		switch rh := n.Args[1].(type) {
		case *StringNode, *ValueNode:
			if strings.HasPrefix(rh.String(), `"now`) {
				return true
			}
		case *BinaryNode:
			return HasDateMath(rh)
		}
	}
	return false
}

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
		case lex.TokenLogicAnd, lex.TokenLogicOr, lex.TokenEqual, lex.TokenEqualEqual:
			return value.BoolType
		case lex.TokenMultiply, lex.TokenMinus, lex.TokenAdd, lex.TokenDivide:
			return value.NumberType
		case lex.TokenModulus:
			return value.IntType
		case lex.TokenLT, lex.TokenLE, lex.TokenGT, lex.TokenGE:
			return value.BoolType
		default:
			//u.LogTracef(u.WARN, "hello")
			u.Warnf("NoValueType? %T  %#v", n, n)
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
func (c *FuncNode) FingerPrint(r rune) string {
	s := c.Name + "("
	for i, arg := range c.Args {
		//u.Debugf("arg: %v   %T %v", arg, arg, arg.String())
		if i > 0 {
			s += ", "
		}
		s += arg.FingerPrint(r)
	}
	s += ")"
	return s
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

func (n *NumberNode) FingerPrint(r rune) string { return string(r) }
func (n *NumberNode) String() string            { return n.Text }
func (n *NumberNode) Check() error {
	return nil
}
func (n *NumberNode) Type() reflect.Value { return floatRv }

func NewStringNode(text string) *StringNode {
	return &StringNode{Text: text}
}
func NewStringNoQuoteNode(text string) *StringNode {
	return &StringNode{Text: text, noQuote: true}
}
func (n *StringNode) FingerPrint(r rune) string { return string(r) }
func (m *StringNode) String() string {
	if m.noQuote {
		return m.Text
	}
	return fmt.Sprintf("%q", m.Text)
}
func (m *StringNode) Check() error        { return nil }
func (m *StringNode) Type() reflect.Value { return stringRv }

func NewValueNode(val value.Value) *ValueNode {
	return &ValueNode{Value: val, rv: reflect.ValueOf(val)}
}
func (n *ValueNode) FingerPrint(r rune) string { return string(r) }
func (m *ValueNode) String() string {
	switch vt := m.Value.(type) {
	case value.StringsValue:
		vals := make([]string, vt.Len())
		for i, v := range vt.Val() {
			vals[i] = fmt.Sprintf("%q", v)
		}
		return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
	case value.SliceValue:
		vals := make([]string, vt.Len())
		for i, v := range vt.Val() {
			vals[i] = fmt.Sprintf("%q", v.ToString())
		}
		return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
	}
	return m.Value.ToString()
}
func (m *ValueNode) Check() error        { return nil }
func (m *ValueNode) Type() reflect.Value { return m.rv }

func NewIdentityNode(tok *lex.Token) *IdentityNode {
	return &IdentityNode{Text: tok.V, Quote: tok.Quote}
}
func NewIdentityNodeVal(val string) *IdentityNode {
	return &IdentityNode{Text: val}
}

func (m *IdentityNode) FingerPrint(r rune) string { return strings.ToLower(m.String()) }
func (m *IdentityNode) String() string {
	// QuoteRune
	identityOnly := lex.IdentityRunesOnly(m.Text)
	if m.Quote == 0 && !identityOnly {
		return "`" + strings.Replace(m.Text, "`", "", -1) + "`"
	}
	if m.Quote == 0 {
		return m.Text
	}

	// What about escaping instead of replacing?
	return string(m.Quote) + strings.Replace(m.Text, string(m.Quote), "", -1) + string(m.Quote)
}
func (m *IdentityNode) Check() error        { return nil }
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

// Return left, right values if is of form   `table.column` or `schema`.`table` and
// also return true/false for if it even has left & right syntax
func (m *IdentityNode) LeftRight() (string, string, bool) {
	if m.left == "" {
		m.left, m.right, _ = LeftRight(m.Text)
	}
	return m.left, m.right, m.left != ""
}

func NewNull(operator lex.Token) *NullNode {
	return &NullNode{}
}

func (m *NullNode) FingerPrint(r rune) string { return m.String() }
func (m *NullNode) String() string            { return "NULL" }
func (n *NullNode) Check() error              { return nil }
func (m *NullNode) Type() reflect.Value       { return nilRv }

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

func (m *BinaryNode) FingerPrint(r rune) string {
	if m.Paren {
		return fmt.Sprintf("(%s %s %s)", m.Args[0].FingerPrint(r), m.Operator.V, m.Args[1].FingerPrint(r))
	}
	return fmt.Sprintf("%s %s %s", m.Args[0].FingerPrint(r), m.Operator.V, m.Args[1].FingerPrint(r))
}
func (m *BinaryNode) String() string {
	return m.toString("")
}
func (m *BinaryNode) toString(negate string) string {
	if m.Paren {
		return fmt.Sprintf("(%s %s%s %s)", m.Args[0].String(), negate, m.Operator.V, m.Args[1].String())
	}
	return fmt.Sprintf("%s %s%s %s", m.Args[0].String(), negate, m.Operator.V, m.Args[1].String())
}
func (m *BinaryNode) StringNegate() string {
	switch m.Operator.T {
	case lex.TokenIN, lex.TokenLike, lex.TokenContains:
		return m.toString("NOT ")
	}
	return m.toString("")
}
func (m *BinaryNode) Check() error {
	// do all args support Binary Operations?   Does that make sense or not?
	return nil
}
func (m *BinaryNode) Type() reflect.Value {
	if argVal, ok := m.Args[0].(NodeValueType); ok {
		return argVal.Type()
	}
	return boolRv
}

// Create a Tri node
//
//  @arg1 [NOT] BETWEEN @arg2 AND @arg3
//
func NewTriNode(operator lex.Token, arg1, arg2, arg3 Node) *TriNode {
	return &TriNode{Args: [3]Node{arg1, arg2, arg3}, Operator: operator}
}
func (m *TriNode) FingerPrint(r rune) string {
	return fmt.Sprintf("%s BETWEEN %s AND %s", m.Args[0].FingerPrint(r), m.Args[1].FingerPrint(r), m.Args[2].FingerPrint(r))
}
func (m *TriNode) String() string {
	return m.toString(false)
}
func (m *TriNode) StringNegate() string {
	return m.toString(true)
}
func (m *TriNode) toString(negate bool) string {
	neg := ""
	if negate {
		neg = "NOT "
	}
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", m.Args[0].String(), neg, m.Args[1].String(), m.Args[2].String())
}
func (m *TriNode) Check() error        { return nil }
func (m *TriNode) Type() reflect.Value { /* ?? */ return boolRv }

// Unary nodes
//
//    NOT
//    EXISTS
//
func NewUnary(operator lex.Token, arg Node) *UnaryNode {
	return &UnaryNode{Arg: arg, Operator: operator}
}

func (m *UnaryNode) FingerPrint(r rune) string {
	switch m.Operator.T {
	case lex.TokenNegate:
		return fmt.Sprintf("NOT %s", m.Arg.FingerPrint(r))
	case lex.TokenExists:
		return fmt.Sprintf("EXISTS %s", m.Arg.FingerPrint(r))
	}
	return fmt.Sprintf("%s(%s)", m.Operator.V, m.Arg.FingerPrint(r))
}
func (m *UnaryNode) String() string {
	var negatedVal string
	if nn, ok := m.Arg.(NegateableNode); ok {
		negatedVal = nn.StringNegate()
	}
	switch m.Operator.T {
	case lex.TokenNegate:
		if negatedVal != "" {
			return negatedVal
		}
		switch argNode := m.Arg.(type) {
		case *TriNode:
			return fmt.Sprintf("NOT (%s)", argNode.String())
		}
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
func (m *UnaryNode) Type() reflect.Value { return boolRv }

// Create an array of Nodes which is a valid node type for boolean IN operator
//
func NewArrayNode() *ArrayNode {
	return &ArrayNode{Args: make([]Node, 0)}
}
func NewArrayNodeArgs(args []Node) *ArrayNode {
	return &ArrayNode{Args: args}
}
func (m *ArrayNode) FingerPrint(r rune) string {
	args := make([]string, len(m.Args))
	for i := 0; i < len(m.Args); i++ {
		args[i] = m.Args[i].FingerPrint(r)
	}
	return fmt.Sprintf("(%s)", strings.Join(args, ","))
}
func (m *ArrayNode) String() string {
	return m.toString(false)
}
func (m *ArrayNode) toString(negate bool) string {
	p1, p2 := "(", ")"
	if m.wraptype == "[" {
		p1, p2 = "[", "]"
	}
	args := make([]string, len(m.Args))
	for i := 0; i < len(m.Args); i++ {
		args[i] = m.Args[i].String()
	}
	return fmt.Sprintf("%s%s%s", p1, strings.Join(args, ","), p2)
}
func (m *ArrayNode) Check() error {
	for _, arg := range m.Args {
		if err := arg.Check(); err != nil {
			return err
		}
	}
	return nil
}
func (m *ArrayNode) Type() reflect.Value { /* ?? */ return boolRv }
func (m *ArrayNode) Append(n Node)       { m.Args = append(m.Args, n) }
