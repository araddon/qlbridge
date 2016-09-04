// Expression structures, ie the  `a = b` type expression syntax
// including parser, node types, boolean logic check, functions.
package expr

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/gogo/protobuf/proto"

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
	// by different types (binary, urnary, func, identity, etc)
	//
	//  - qlbridge does not currently implement statements (if, for, switch, etc)
	//    just expressions, and operators
	Node interface {
		// string representation of Node parseable back to itself
		String() string

		// Given a dialect writer write out, equivalent of String()
		// but allows different escape characters
		WriteDialect(w DialectWriter)

		// performs type and syntax checking for itself and sub-nodes, evaluates
		// validity of the expression/node in advance of evaluation
		Check() error

		// Protobuf helpers that convert to serializeable format and marshall
		ToPB() *NodePb
		FromPB(*NodePb) Node

		// for testing purposes
		Equal(Node) bool
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
	//   <expression> [NOT] INTERSECTS ("a", "b")
	//
	NegateableNode interface {
		Node
		// If the node is negateable, we may collapse an surrounding
		// negation into here
		Negated() bool
		ReverseNegation()
		StringNegate() string
		WriteNegate(w DialectWriter)
		// Negateable nodes may be collapsed logically
		Node() Node
	}

	// Eval context, used to contain info for usage/lookup at runtime evaluation
	EvalContext interface {
		ContextReader
	}
	// Eval context, used to contain info for usage/lookup at runtime evaluation
	EvalIncludeContext interface {
		ContextReader
		Includer
	}

	// Context Reader is a key-value interface to read the context of message/row
	//  using a  Get("key") interface.  Used by vm to evaluate messages
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

	ContextReadWriter interface {
		ContextReader
		ContextWriter
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
		Name    string // Name of func
		F       Func   // The actual function that this AST maps to
		Missing bool
		Args    []Node // Arguments are them-selves nodes
	}

	// IdentityNode will look up a value out of a env bag
	//  also identities of sql objects (tables, columns, etc)
	//  we often need to rewrite these as in sql it is `table.column`
	IdentityNode struct {
		Quote    byte
		Text     string
		original string
		escaped  string
		left     string
		right    string
	}

	// StringNode holds a value literal, quotes not included
	StringNode struct {
		Quote   byte
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
	//    +, -, *, %, /, LIKE, CONTAINS, INTERSECTS
	// Also, parenthesis may wrap these
	BinaryNode struct {
		negated  bool
		Paren    bool
		Args     []Node
		Operator lex.Token
	}

	// Boolean node is   n nodes and an operator
	// operators can be only AND/OR
	BooleanNode struct {
		negated  bool
		Args     []Node
		Operator lex.Token
	}

	// Tri Node
	//    ARG1 Between ARG2 AND ARG3
	TriNode struct {
		negated  bool
		Args     []Node
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

	// IncludeNode references a named node
	//
	//   (  ! INCLUDE <identity>  |  INCLUDE <identity> | NOT INCLUDE <identity> )
	//
	IncludeNode struct {
		negated  bool
		Expr     Node
		Identity *IdentityNode
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

var (
	// Ensure our dialect writer implements interface
	_ DialectWriter = (*defaultDialect)(nil)

	// Ensure some of our nodes implement Interfaces
	_ NegateableNode = (*BinaryNode)(nil)
	_ NegateableNode = (*BooleanNode)(nil)
	_ NegateableNode = (*TriNode)(nil)
	_ NegateableNode = (*IncludeNode)(nil)
)

var (
	// a static nil includer whose job is to return errors
	// for vm's that don't have an includer
	noIncluder = &IncludeContext{}

	// ErrNoIncluder is message saying a FilterQL included reference
	// to an include when no Includer was available to resolve
	ErrNoIncluder      = fmt.Errorf("No Includer is available")
	ErrIncludeNotFound = fmt.Errorf("Include Not Found")

	// Ensure we implement interface
	_ Includer = (*IncludeContext)(nil)
)

// Includer defines an interface used for resolving INCLUDE clauses into a
// Indclude reference. Implementations should return an error if the name cannot
// be resolved.
type Includer interface {
	Include(name string) (Node, error)
}

type IncludeContext struct {
	ContextReader
}

func NewIncludeContext(cr ContextReader) *IncludeContext {
	return &IncludeContext{ContextReader: cr}
}
func (*IncludeContext) Include(name string) (Node, error) { return nil, ErrNoIncluder }

// Determine if this expression node uses datemath (ie, "now-4h")
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
	case *BooleanNode:
		for _, arg := range n.Args {
			current = findallidents(arg, current)
		}
	case *UnaryNode:
		current = findallidents(n.Arg, current)
	case *TriNode:
		for _, arg := range n.Args {
			current = findallidents(arg, current)
		}
	case *ArrayNode:
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

func findAllIncludes(node Node, current []string) []string {
	switch n := node.(type) {
	case *IncludeNode:
		current = append(current, n.Identity.Text)
	case *BinaryNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *BooleanNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *UnaryNode:
		current = findAllIncludes(n.Arg, current)
	case *TriNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *ArrayNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *FuncNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	}
	return current
}

// FindIncludes Recursively descend down a node looking for all Include identities
func FindIncludes(node Node) []string {
	return findAllIncludes(node, nil)
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
		// should we fall through and say unknown?
		return value.StringType
	case *NumberNode:
		return value.NumberType
	case *BinaryNode:
		switch nt.Operator.T {
		case lex.TokenLogicAnd, lex.TokenAnd, lex.TokenLogicOr, lex.TokenOr,
			lex.TokenEqual, lex.TokenEqualEqual:
			return value.BoolType
		case lex.TokenMultiply, lex.TokenMinus, lex.TokenAdd, lex.TokenDivide:
			return value.NumberType
		case lex.TokenModulus:
			return value.IntType
		case lex.TokenLT, lex.TokenLE, lex.TokenGT, lex.TokenGE:
			return value.BoolType
		default:
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

func (m *FuncNode) append(arg Node) {
	m.Args = append(m.Args, arg)
}
func (m *FuncNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *FuncNode) WriteDialect(w DialectWriter) {
	io.WriteString(w, m.Name)
	io.WriteString(w, "(")
	for i, arg := range m.Args {
		if i > 0 {
			io.WriteString(w, ", ")
		}
		arg.WriteDialect(w)
	}
	io.WriteString(w, ")")
}
func (m *FuncNode) Check() error {

	if len(m.Args) < len(m.F.Args) && !m.F.VariadicArgs {
		return fmt.Errorf("parse: not enough arguments for %s  supplied:%d  f.Args:%v", m.Name, len(m.Args), len(m.F.Args))
	} else if (len(m.Args) >= len(m.F.Args)) && m.F.VariadicArgs {
		// ok
	} else if len(m.Args) > len(m.F.Args) {
		u.Warnf("lenc.Args >= len(m.F.Args?  %v", (len(m.Args) >= len(m.F.Args)))
		err := fmt.Errorf("parse: too many arguments for %s want:%v got:%v   %#v", m.Name, len(m.F.Args), len(m.Args), m.Args)
		u.Errorf("funcNode.Check(): %v", err)
		return err
	}
	for i, a := range m.Args {

		if ne, isNodeExpr := a.(Node); isNodeExpr {
			if err := ne.Check(); err != nil {
				return err
			}
		} else if _, isValue := a.(value.Value); isValue {
			// TODO: we need to check co-ercion here, ie which Args can be converted to what types
			if nodeVal, ok := a.(NodeValueType); ok {
				// For Env Variables, we need to Check those (On Definition?)
				if m.F.Args[i].Kind() != nodeVal.Type().Kind() {
					u.Errorf("error in parse Check(): %v", a)
					return fmt.Errorf("parse: expected %v, got %v    ", nodeVal.Type().Kind(), m.F.Args[i].Kind())
				}
			}
		} else {
			u.Warnf("Unknown type for func arg %T", a)
			return fmt.Errorf("Unknown type for func arg %T", a)
		}
	}
	return nil
}
func (m *FuncNode) Type() reflect.Value { return m.F.Return }
func (m *FuncNode) ToPB() *NodePb {
	n := &FuncNodePb{}
	n.Name = m.Name
	n.Args = make([]NodePb, len(m.Args))
	for i, a := range m.Args {
		//u.Debugf("Func ToPB: arg %T", a)
		n.Args[i] = *a.ToPB()
	}
	return &NodePb{Fn: n}
}
func (m *FuncNode) FromPB(n *NodePb) Node {
	fn, ok := funcs[strings.ToLower(n.Fn.Name)]
	if !ok {
		u.Errorf("Not Found Func %q", n.Fn.Name)
		// Panic?
	}
	return &FuncNode{
		Name: n.Fn.Name,
		Args: NodesFromNodesPb(n.Fn.Args),
		F:    fn,
	}
}
func (m *FuncNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*FuncNode); ok {
		if m.Name != nt.Name {
			return false
		}
		for i, arg := range nt.Args {
			if !arg.Equal(m.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

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

func (n *NumberNode) String() string               { return n.Text }
func (m *NumberNode) WriteDialect(w DialectWriter) { w.WriteNumber(m.Text) }
func (n *NumberNode) Check() error {
	return nil
}
func (n *NumberNode) Type() reflect.Value { return floatRv }
func (m *NumberNode) ToPB() *NodePb {
	n := &NumberNodePb{}
	n.Text = m.Text
	n.Fv = m.Float64
	n.Iv = m.Int64
	return &NodePb{Nn: n}
}
func (m *NumberNode) FromPB(n *NodePb) Node {
	return &NumberNode{
		Text:    n.Nn.Text,
		Float64: n.Nn.Fv,
		Int64:   n.Nn.Iv,
	}
}
func (m *NumberNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*NumberNode); ok {
		if m.Text != nt.Text {
			return false
		}
		if m.Float64 != nt.Float64 {
			return false
		}
		if m.Int64 != nt.Int64 {
			return false
		}
		return true
	}
	return false
}

func NewStringNode(text string) *StringNode {
	return &StringNode{Text: text}
}
func NewStringNodeToken(t lex.Token) *StringNode {
	return &StringNode{Text: t.V, Quote: t.Quote}
}
func NewStringNoQuoteNode(text string) *StringNode {
	return &StringNode{Text: text, noQuote: true}
}
func (m *StringNode) String() string {
	if m.noQuote {
		return m.Text
	}
	if m.Quote > 0 {
		return fmt.Sprintf("%s%s%s", string(m.Quote), m.Text, string(m.Quote))
	}
	return fmt.Sprintf("%q", m.Text)
}
func (m *StringNode) WriteDialect(w DialectWriter) {
	w.WriteLiteral(m.Text)
}
func (m *StringNode) Check() error { return nil }
func (m *StringNode) ToPB() *NodePb {
	n := &StringNodePb{}
	n.Text = m.Text
	if m.noQuote {
		n.Noquote = proto.Bool(true)
	}
	if m.Quote > 0 {
		n.Quote = proto.Int32(int32(m.Quote))
	}
	return &NodePb{Sn: n}
}
func (m *StringNode) FromPB(n *NodePb) Node {
	noQuote := false
	quote := 0
	if n.Sn.Noquote != nil {
		noQuote = *n.Sn.Noquote
	}
	if n.Sn.Quote != nil {
		quote = int(*n.Sn.Quote)
	}
	return &StringNode{
		noQuote: noQuote,
		Text:    n.Sn.Text,
		Quote:   byte(quote),
	}
}
func (m *StringNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*StringNode); ok {
		if m.Text != nt.Text {
			return false
		}
		return true
	}
	return false
}

func NewValueNode(val value.Value) *ValueNode {
	return &ValueNode{Value: val, rv: reflect.ValueOf(val)}
}
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
func (m *ValueNode) WriteDialect(w DialectWriter) {
	switch vt := m.Value.(type) {
	case value.StringsValue:
		io.WriteString(w, "[")
		for i, v := range vt.Val() {
			if i != 0 {
				io.WriteString(w, ", ")
			}
			w.WriteLiteral(v)
		}
		io.WriteString(w, "]")
	case value.SliceValue:
		io.WriteString(w, "[")
		for i, v := range vt.Val() {
			if i != 0 {
				io.WriteString(w, ", ")
			}
			w.WriteLiteral(v.ToString())
		}
		io.WriteString(w, "]")
	case value.StringValue:
		w.WriteLiteral(vt.Val())
	case value.IntValue:
		w.WriteNumber(vt.ToString())
	case value.NumberValue:
		w.WriteNumber(vt.ToString())
	case value.BoolValue:
		w.WriteLiteral(vt.ToString())
	default:
		u.Warnf("unsupported value-node writer: %T", vt)
		io.WriteString(w, vt.ToString())
	}
}
func (m *ValueNode) Check() error        { return nil }
func (m *ValueNode) Type() reflect.Value { return m.rv }
func (m *ValueNode) ToPB() *NodePb {
	u.Errorf("Not implemented %#v", m)
	return nil
}
func (m *ValueNode) FromPB(n *NodePb) Node {
	u.Errorf("Not implemented %#v", n)
	return &ValueNode{}
}
func (m *ValueNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*ValueNode); ok {
		if m.Value.Value() != nt.Value.Value() {
			return false
		}
		return true
	}
	return false
}

func NewIdentityNode(tok *lex.Token) *IdentityNode {
	in := &IdentityNode{Text: tok.V, Quote: tok.Quote}
	in.load()
	return in
}
func NewIdentityNodeVal(val string) *IdentityNode {
	in := &IdentityNode{Text: val}
	in.load()
	return in
}
func (m *IdentityNode) load() {
	if m.Quote != 0 {
		//   this came in with quote which has been stripped by lexer
		m.original = fmt.Sprintf("%s%s%s", string(m.Quote), m.Text, string(m.Quote))
		m.left, m.right, _ = LeftRight(m.original)
	} else {
		m.left, m.right, _ = LeftRight(m.Text)
	}
}

func (m *IdentityNode) String() string {
	if m.original != "" {
		return m.original
	}
	if m.Quote == 0 {
		return m.Text
	}

	// What about escaping instead of replacing?
	return StringEscape(rune(m.Quote), m.Text)
}
func (m *IdentityNode) WriteDialect(w DialectWriter) {
	if m.left != "" {
		// `user`.`email`   type namespacing, may need to be escaped differently
		w.WriteIdentity(m.left)
		w.Write([]byte{'.'})
		w.WriteIdentity(m.right)
		return
	}
	if m.Quote != 0 {
		w.WriteIdentityQuote(m.Text, byte(m.Quote))
		return
	}
	w.WriteIdentity(m.Text)
}
func (m *IdentityNode) OriginalText() string {
	if m.original != "" {
		return m.original
	}
	return m.Text
}
func (m *IdentityNode) Check() error        { return nil }
func (m *IdentityNode) Type() reflect.Value { return stringRv }
func (m *IdentityNode) IdentityPb() *IdentityNodePb {
	n := &IdentityNodePb{}
	n.Text = m.Text
	q := int32(m.Quote)
	n.Quote = &q
	return n
}
func (m *IdentityNode) ToPB() *NodePb {
	return &NodePb{In: m.IdentityPb()}
}
func (m *IdentityNode) FromPB(n *NodePb) Node {
	q := n.In.Quote
	return &IdentityNode{Text: n.In.Text, Quote: byte(*q)}
}
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
func (m *IdentityNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*IdentityNode); ok {
		if nt.Text != m.Text {
			return false
		}

		// Hm, should we compare quotes or not?  Given they are dialect
		// specific and don't affect logic i vote no?

		// if nt.Quote != m.Quote {
		// 	switch m.Quote {
		// 	case '`':
		// 		if nt.Quote == '\'' || nt.Quote == 0 {
		// 			// ok
		// 			return true
		// 		}
		// 	case 0:
		// 		if nt.Quote == '\'' || nt.Quote == '`' {
		// 			// ok
		// 			return true
		// 		}
		// 	}
		// 	return false
		// }
		return true
	}
	return false
}

// HasLeftRight Return bool if is of form   `table.column` or `schema`.`table`
func (m *IdentityNode) HasLeftRight() bool {
	return m.left != ""
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

func (m *NullNode) String() string { return "NULL" }
func (m *NullNode) WriteDialect(w DialectWriter) {
	io.WriteString(w, "NULL")
}
func (n *NullNode) Check() error        { return nil }
func (m *NullNode) Type() reflect.Value { return nilRv }
func (m *NullNode) ToPB() *NodePb       { return nil }
func (m *NullNode) FromPB(n *NodePb) Node {
	return &NullNode{}
}
func (m *NullNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if _, ok := n.(*NullNode); ok {
		return true
	}
	return false
}

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
	return &BinaryNode{Args: []Node{lhArg, rhArg}, Operator: operator}
}

func (m *BinaryNode) ReverseNegation() {
	m.negated = !m.negated
}
func (m *BinaryNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *BinaryNode) StringNegate() string {
	w := NewDefaultWriter()
	m.WriteNegate(w)
	return w.String()
}
func (m *BinaryNode) WriteNegate(w DialectWriter) {
	switch m.Operator.T {
	case lex.TokenIN, lex.TokenIntersects, lex.TokenLike, lex.TokenContains:
		m.writeToString(w, "NOT ")
	default:
		m.writeToString(w, "")
	}
}
func (m *BinaryNode) WriteDialect(w DialectWriter) {
	if m.negated {
		m.writeToString(w, "NOT ")
	} else {
		m.writeToString(w, "")
	}
}
func (m *BinaryNode) writeToString(w DialectWriter, negate string) {
	if m.Paren {
		io.WriteString(w, "(")
	}
	m.Args[0].WriteDialect(w)
	io.WriteString(w, " ")
	if len(negate) > 0 {
		switch m.Operator.T {
		case lex.TokenEqual, lex.TokenEqualEqual:
			io.WriteString(w, lex.TokenNE.String())
		case lex.TokenNE:
			io.WriteString(w, lex.TokenEqual.String())
		case lex.TokenGE:
			io.WriteString(w, lex.TokenLT.String())
		case lex.TokenGT:
			io.WriteString(w, lex.TokenLE.String())
		case lex.TokenLE:
			io.WriteString(w, lex.TokenGT.String())
		case lex.TokenLT:
			io.WriteString(w, lex.TokenGE.String())
		default:
			io.WriteString(w, negate)
			io.WriteString(w, m.Operator.V)
		}
	} else {
		io.WriteString(w, m.Operator.V)
	}

	io.WriteString(w, " ")
	m.Args[1].WriteDialect(w)
	if m.Paren {
		io.WriteString(w, ")")
	}
}
func (m *BinaryNode) Node() Node    { return m }
func (m *BinaryNode) Negated() bool { return m.negated }
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
func (m *BinaryNode) ToPB() *NodePb {
	n := &BinaryNodePb{}
	n.Paren = m.Paren
	n.Op = int32(m.Operator.T)
	n.Args = []NodePb{*m.Args[0].ToPB(), *m.Args[1].ToPB()}
	return &NodePb{Bn: n}
}
func (m *BinaryNode) FromPB(n *NodePb) Node {
	return &BinaryNode{
		Operator: tokenFromInt(n.Bn.Op),
		Paren:    n.Bn.Paren,
		Args:     NodesFromNodesPb(n.Bn.Args),
	}
}
func (m *BinaryNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*BinaryNode); ok {
		if nt.Operator.T != m.Operator.T {
			return false
		}
		if nt.Operator.V != m.Operator.V {
			if strings.ToLower(nt.Operator.V) != strings.ToLower(m.Operator.V) {
				return false
			}
		}
		if nt.Paren != m.Paren {
			return false
		}
		for i, arg := range nt.Args {
			if !arg.Equal(m.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// NewBooleanNode Create a boolean node
//   @operator = AND, OR
//  @args = nodes
func NewBooleanNode(operator lex.Token, args ...Node) *BooleanNode {
	//u.Debugf("NewBinaryNode: %v %v %v", lhArg, operator, rhArg)
	return &BooleanNode{Args: args, Operator: operator}
}

func (m *BooleanNode) ReverseNegation() {
	m.negated = !m.negated
}
func (m *BooleanNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *BooleanNode) StringNegate() string {
	w := NewDefaultWriter()
	m.WriteNegate(w)
	return w.String()
}
func (m *BooleanNode) WriteNegate(w DialectWriter) {
	m.writeToString(w, "NOT ")
}
func (m *BooleanNode) WriteDialect(w DialectWriter) {
	if m.negated {
		m.writeToString(w, "NOT ")
	} else {
		m.writeToString(w, "")
	}

}
func (m *BooleanNode) writeToString(w DialectWriter, negate string) {
	if len(negate) > 0 {
		io.WriteString(w, negate)
	}
	io.WriteString(w, m.Operator.V)
	io.WriteString(w, " ( ")
	for i, n := range m.Args {
		if i != 0 {
			io.WriteString(w, ", ")
		}
		n.WriteDialect(w)
	}
	io.WriteString(w, " )")
}
func (m *BooleanNode) Node() Node {
	if len(m.Args) == 1 {
		if m.Negated() {
			nn, ok := m.Args[0].(NegateableNode)
			if ok {
				nn.ReverseNegation()
				return nn
			}
		}
		return m.Args[0]
	}
	return m
}
func (m *BooleanNode) Negated() bool       { return m.negated }
func (m *BooleanNode) Check() error        { return nil }
func (m *BooleanNode) Type() reflect.Value { return boolRv }
func (m *BooleanNode) ToPB() *NodePb {
	n := &BooleanNodePb{}
	n.Op = int32(m.Operator.T)
	for _, arg := range m.Args {
		n.Args = append(n.Args, *arg.ToPB())
	}
	return &NodePb{Booln: n}
}
func (m *BooleanNode) FromPB(n *NodePb) Node {
	return &BooleanNode{
		Operator: tokenFromInt(n.Booln.Op),
		Args:     NodesFromNodesPb(n.Booln.Args),
	}
}
func (m *BooleanNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*BooleanNode); ok {
		if nt.Operator.T != m.Operator.T {
			return false
		}
		if nt.Operator.V != m.Operator.V {
			if strings.ToLower(nt.Operator.V) != strings.ToLower(m.Operator.V) {
				return false
			}
		}
		if len(m.Args) != len(nt.Args) {
			return false
		}
		for i, arg := range nt.Args {
			if !arg.Equal(m.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// Create a Tri node
//
//  @arg1 [NOT] BETWEEN @arg2 AND @arg3
//
func NewTriNode(operator lex.Token, arg1, arg2, arg3 Node) *TriNode {
	return &TriNode{Args: []Node{arg1, arg2, arg3}, Operator: operator}
}
func (m *TriNode) ReverseNegation() {
	m.negated = !m.negated
}
func (m *TriNode) String() string {
	w := NewDefaultWriter()
	m.writeToString(w, false)
	return w.String()
}
func (m *TriNode) StringNegate() string {
	w := NewDefaultWriter()
	m.writeToString(w, true)
	return w.String()
}
func (m *TriNode) WriteNegate(w DialectWriter) {
	m.writeToString(w, true)
}
func (m *TriNode) WriteDialect(w DialectWriter) {
	m.writeToString(w, false)
}
func (m *TriNode) writeToString(w DialectWriter, negate bool) {
	m.Args[0].WriteDialect(w)
	io.WriteString(w, " ")
	if negate {
		io.WriteString(w, "NOT ")
	}
	switch m.Operator.T {
	case lex.TokenBetween:
		io.WriteString(w, "BETWEEN ")
	}
	m.Args[1].WriteDialect(w)
	io.WriteString(w, " AND ")
	m.Args[2].WriteDialect(w)
}
func (m *TriNode) Node() Node          { return m }
func (m *TriNode) Negated() bool       { return m.negated }
func (m *TriNode) Check() error        { return nil }
func (m *TriNode) Type() reflect.Value { /* ?? */ return boolRv }
func (m *TriNode) ToPB() *NodePb {
	n := &TriNodePb{Args: make([]NodePb, len(m.Args))}
	n.Op = int32(m.Operator.T)
	for i, arg := range m.Args {
		n.Args[i] = *arg.ToPB()
		//u.Debugf("TriNode ToPB: %T", arg)
	}
	return &NodePb{Tn: n}
}
func (m *TriNode) FromPB(n *NodePb) Node {
	return &TriNode{
		Operator: tokenFromInt(n.Tn.Op),
		Args:     NodesFromNodesPb(n.Tn.Args),
	}
}
func (m *TriNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*TriNode); ok {
		if nt.Operator.T != m.Operator.T {
			return false
		}
		for i, arg := range nt.Args {
			if !arg.Equal(m.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// Unary nodes
//
//    NOT <expression>
//    ! <expression>
//    EXISTS <identity>
//    <identity> IS NOT NULL
//
func NewUnary(operator lex.Token, arg Node) Node {
	nn, ok := arg.(NegateableNode)
	switch operator.T {
	case lex.TokenNegate:
		if ok {
			nn.ReverseNegation()
			return nn.Node()
		}
	case lex.TokenOr, lex.TokenLogicOr, lex.TokenLogicAnd, lex.TokenAnd:
		if ok {
			// All negateable nodes may possibly elide
			u.Warnf("is this valid? %#v  %#v", operator, arg)
			panic("not valid?")
			return nn.Node()
		}
	}

	return &UnaryNode{Arg: arg, Operator: operator}
}

func (m *UnaryNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *UnaryNode) WriteDialect(w DialectWriter) {
	switch m.Operator.T {
	case lex.TokenNegate:
		if nn, ok := m.Arg.(NegateableNode); ok {
			nn.WriteNegate(w)
			return
		}
		io.WriteString(w, "NOT ")
		switch m.Arg.(type) {
		case *TriNode:
			io.WriteString(w, "(")
			m.Arg.WriteDialect(w)
			io.WriteString(w, ")")
		default:
			m.Arg.WriteDialect(w)
		}
	case lex.TokenExists:
		io.WriteString(w, "EXISTS ")
		m.Arg.WriteDialect(w)
	default:
		io.WriteString(w, m.Operator.V)
		io.WriteString(w, " (")
		m.Arg.WriteDialect(w)
		io.WriteString(w, ")")
	}
}
func (n *UnaryNode) Check() error {
	if ne, isNodeExpr := n.Arg.(Node); isNodeExpr {
		return ne.Check()
	} else if _, isValue := n.Arg.(value.Value); isValue {
		return nil
	}
	return fmt.Errorf("parse: type error in expected? got %v", n.Arg)
}
func (m *UnaryNode) Node() Node          { return m }
func (m *UnaryNode) Type() reflect.Value { return boolRv }
func (m *UnaryNode) ToPB() *NodePb {
	n := &UnaryNodePb{}
	n.Arg = *m.Arg.ToPB()
	n.Op = int32(m.Operator.T)
	return &NodePb{Un: n}
}
func (m *UnaryNode) FromPB(n *NodePb) Node {
	return &UnaryNode{
		Operator: tokenFromInt(n.Un.Op),
		Arg:      NodeFromNodePb(&n.Un.Arg),
	}
}
func (m *UnaryNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*UnaryNode); ok {
		if nt.Operator.T != m.Operator.T {
			return false
		}
		return m.Arg.Equal(nt.Arg)
	}
	return false
}

// Include nodes
//
//    NOT INCLUDE <identity>
//    ! INCLUDE <identity>
//    INCLUDE <identity>
//
func NewInclude(operator lex.Token, id *IdentityNode) *IncludeNode {
	return &IncludeNode{Identity: id, Operator: operator}
}

func (m *IncludeNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *IncludeNode) WriteDialect(w DialectWriter) {
	if m.negated {
		io.WriteString(w, "NOT ")
	}
	io.WriteString(w, "INCLUDE ")
	m.Identity.WriteDialect(w)
}
func (m *IncludeNode) ReverseNegation() {
	m.negated = !m.negated
}
func (m *IncludeNode) StringNegate() string {
	w := NewDefaultWriter()
	m.WriteNegate(w)
	return w.String()
}
func (m *IncludeNode) WriteNegate(w DialectWriter) {
	if !m.negated { // double negation
		io.WriteString(w, "NOT")
	}
	io.WriteString(w, " INCLUDE ")
	m.Identity.WriteDialect(w)
}
func (m *IncludeNode) Negated() bool { return m.negated }
func (m *IncludeNode) Node() Node    { return m }
func (n *IncludeNode) Check() error {
	return nil
}
func (m *IncludeNode) ToPB() *NodePb {
	n := &IncludeNodePb{}
	n.Identity = *m.Identity.IdentityPb()
	n.Op = int32(m.Operator.T)
	return &NodePb{Incn: n}
}
func (m *IncludeNode) FromPB(n *NodePb) Node {
	inid := n.Incn.Identity
	q := n.Incn.Identity.Quote
	return &IncludeNode{
		negated:  n.Incn.Negated,
		Operator: tokenFromInt(n.Incn.Op),
		Identity: &IdentityNode{Text: inid.Text, Quote: byte(*q)},
	}
}
func (m *IncludeNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	nt, ok := n.(*IncludeNode)
	if !ok {
		return false
	}
	if m.negated != nt.negated {
		return false
	}
	if nt.Operator.T != m.Operator.T {
		return false
	}
	return m.Identity.Equal(nt.Identity)
}

// Create an array of Nodes which is a valid node type for boolean IN operator
//
func NewArrayNode() *ArrayNode {
	return &ArrayNode{Args: make([]Node, 0)}
}
func NewArrayNodeArgs(args []Node) *ArrayNode {
	return &ArrayNode{Args: args}
}
func (m *ArrayNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *ArrayNode) WriteDialect(w DialectWriter) {
	if m.wraptype == "[" {
		io.WriteString(w, "[")
	} else {
		io.WriteString(w, "(")
	}
	for i, arg := range m.Args {
		if i != 0 {
			io.WriteString(w, ", ")
		}
		arg.WriteDialect(w)
	}
	if m.wraptype == "[" {
		io.WriteString(w, "]")
	} else {
		io.WriteString(w, ")")
	}
}
func (m *ArrayNode) Append(n Node) { m.Args = append(m.Args, n) }
func (m *ArrayNode) Check() error {
	for _, arg := range m.Args {
		if err := arg.Check(); err != nil {
			return err
		}
	}
	return nil
}
func (m *ArrayNode) Type() reflect.Value { /* ?? */ return boolRv }
func (m *ArrayNode) ToPB() *NodePb {
	n := &ArrayNodePb{Args: make([]NodePb, len(m.Args))}
	iv := int32(0)
	if m.wraptype != "" && len(m.wraptype) == 1 {
		iv = int32(m.wraptype[0])
	}
	n.Wrap = &iv
	for i, arg := range m.Args {
		n.Args[i] = *arg.ToPB()
	}
	return &NodePb{An: n}
}
func (m *ArrayNode) FromPB(n *NodePb) Node {
	return &ArrayNode{
		Args: NodesFromNodesPb(n.An.Args),
	}
}
func (m *ArrayNode) Equal(n Node) bool {
	if m == nil && n == nil {
		return true
	}
	if m == nil && n != nil {
		return false
	}
	if m != nil && n == nil {
		return false
	}
	if nt, ok := n.(*ArrayNode); ok {
		for i, arg := range nt.Args {
			if !arg.Equal(m.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// Node serialization helpers
func tokenFromInt(iv int32) lex.Token {
	t, ok := lex.TokenNameMap[lex.TokenType(iv)]
	if ok {
		return lex.Token{T: t.T, V: t.Kw}
	}
	return lex.Token{}
}

// Create a node from pb
func NodeFromPb(pb []byte) (Node, error) {
	n := &NodePb{}
	if err := proto.Unmarshal(pb, n); err != nil {
		return nil, err
	}
	return NodeFromNodePb(n), nil
}
func NodeFromNodePb(n *NodePb) Node {
	if n == nil {
		return nil
	}
	switch {
	case n.Bn != nil:
		var bn *BinaryNode
		return bn.FromPB(n)
	case n.Booln != nil:
		var bn *BooleanNode
		return bn.FromPB(n)
	case n.Un != nil:
		var un *UnaryNode
		return un.FromPB(n)
	case n.Fn != nil:
		var fn *FuncNode
		return fn.FromPB(n)
	case n.Tn != nil:
		var tn *TriNode
		return tn.FromPB(n)
	case n.An != nil:
		var an *ArrayNode
		return an.FromPB(n)
	case n.Nn != nil:
		var nn *NumberNode
		return nn.FromPB(n)
	case n.Vn != nil:
		var vn *ValueNode
		return vn.FromPB(n)
	case n.In != nil:
		var in *IdentityNode
		return in.FromPB(n)
	case n.Sn != nil:
		var sn *StringNode
		return sn.FromPB(n)
	case n.Incn != nil:
		var in *IncludeNode
		return in.FromPB(n)
	}
	return nil
}
func NodesFromNodesPbPtr(pb []*NodePb) []Node {
	nodes := make([]Node, len(pb))
	for i, pbn := range pb {
		nodes[i] = NodeFromNodePb(pbn)
	}
	return nodes
}

func NodesFromNodesPb(pb []NodePb) []Node {
	nodes := make([]Node, len(pb))
	for i, pbn := range pb {
		nodes[i] = NodeFromNodePb(&pbn)
	}
	return nodes
}

func NodesPbFromNodes(nodes []Node) []*NodePb {
	pbs := make([]*NodePb, len(nodes))
	for i, n := range nodes {
		pbs[i] = n.ToPB()
	}
	return pbs
}
func NodesEqual(n1, n2 Node) bool {
	switch n1t := n1.(type) {
	case *BinaryNode:
		if n2t, ok := n2.(*BinaryNode); ok {
			return n1t.Equal(n2t)
		}
	case *UnaryNode:

	case *FuncNode:

	case *TriNode:

	case *ArrayNode:

	case *NumberNode:

	case *ValueNode:

	case *IdentityNode:

	case *StringNode:

	}
	return false
}
