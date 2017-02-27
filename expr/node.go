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
	ErrNotSupported   = fmt.Errorf("Not supported")
	ErrNotImplemented = fmt.Errorf("Not implemented")
	ErrUnknownCommand = fmt.Errorf("Unknown Command")
	ErrInternalError  = fmt.Errorf("Internal Error")

	// ErrNoIncluder is message saying a FilterQL included reference
	// to an include when no Includer was available to resolve
	ErrNoIncluder      = fmt.Errorf("No Includer is available")
	ErrIncludeNotFound = fmt.Errorf("Include Not Found")

	// a static nil includer whose job is to return errors
	// for vm's that don't have an includer
	noIncluder = &IncludeContext{}

	// Ensure our dialect writer implements interface
	_ DialectWriter = (*defaultDialect)(nil)

	// Ensure some of our nodes implement Interfaces
	//_ NegateableNode = (*BinaryNode)(nil)
	_ NegateableNode = (*BooleanNode)(nil)
	_ NegateableNode = (*TriNode)(nil)
	_ NegateableNode = (*IncludeNode)(nil)

	// Ensure we implement interface
	_ Includer = (*IncludeContext)(nil)
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

		// Validate Syntax validation of this expression node
		Validate() error

		// Protobuf helpers that convert to serializeable format and marshall
		NodePb() *NodePb
		FromPB(*NodePb) Node

		// Convert to Simple Expression syntax
		// which is useful for json respresentation
		Expr() *Expr
		FromExpr(*Expr) error

		// for testing purposes
		Equal(Node) bool
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
		// Reverse Negation if Possible:  for instance:
		//   "A" NOT IN ("a","b")    =>  "A" IN ("a","b")
		ReverseNegation() bool
		StringNegate() string
		WriteNegate(w DialectWriter)
		// Negateable nodes may be collapsed logically into new nodes
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

	// The generic Expr
	Expr struct {
		// The token, and node expressions are non
		// nil if it is an expression
		Op   string  `json:"op,omitempty"`
		Args []*Expr `json:"args,omitempty"`

		// If op is 0, and args nil then exactly one of these should be set
		Identity string `json:"ident,omitempty"`
		Value    string `json:"val,omitempty"`
		// Really would like to use these instead of un-typed guesses above
		// if we desire serialization into string representation that is fine
		// Int      int64
		// Float    float64
		// Bool     bool
	}

	// Describes a function which wraps and allows native go functions
	//  to be called (via reflection) in expression vm
	Func struct {
		Name      string
		Aggregate bool // is this aggregate func?
		// CustomFunc Is dynamic function that can be registered
		CustomFunc
		Eval EvaluatorFunc
	}

	// FuncNode holds a Func, which desribes a go Function as
	// well as fulfilling the Pos, String() etc for a Node
	//
	// interfaces:   Node
	FuncNode struct {
		Name    string        // Name of func
		F       Func          // The actual function that this AST maps to
		Eval    EvaluatorFunc // the evaluator function
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
	// IdentityNodes is a list of identities
	IdentityNodes []*IdentityNode

	// StringNode holds a value literal, quotes not included
	StringNode struct {
		Quote       byte
		Text        string
		noQuote     bool
		needsEscape bool // Does Text contain Quote value?
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
		ExprNode Node
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
			if strings.HasPrefix(strings.ToLower(rh.String()), `"now`) {
				return true
			}
		case *BinaryNode:
			if HasDateMath(rh) {
				return true
			}
		}
	case *BooleanNode:
		for _, arg := range n.Args {
			if HasDateMath(arg) {
				return true
			}
		}
	case *UnaryNode:
		return HasDateMath(n.Arg)
	}
	return false
}

// Recursively descend down a node looking for first Identity Field
//
//     min(year)                 == year
//     eq(min(item), max(month)) == item
func FindFirstIdentity(node Node) string {
	l := findIdentities(node, nil).Strings()
	if len(l) == 0 {
		return ""
	}
	return l[0]
}

// Recursively descend down a node looking for all Identity Fields
//
//     min(year)                 == {year}
//     eq(min(item), max(month)) == {item, month}
func FindAllIdentityField(node Node) []string {
	return findIdentities(node, nil).Strings()
}

// Recursively descend down a node looking for all Identity Fields
//
//     min(year)                 == {year}
//     eq(min(item), max(month)) == {item, month}
func FindAllLeftIdentityFields(node Node) []string {
	return findIdentities(node, nil).LeftStrings()
}

func findIdentities(node Node, l IdentityNodes) IdentityNodes {
	switch n := node.(type) {
	case *IdentityNode:
		l = append(l, n)
	case *BinaryNode:
		for _, arg := range n.Args {
			l = findIdentities(arg, l)
		}
	case *BooleanNode:
		for _, arg := range n.Args {
			l = findIdentities(arg, l)
		}
	case *UnaryNode:
		l = findIdentities(n.Arg, l)
	case *TriNode:
		for _, arg := range n.Args {
			l = findIdentities(arg, l)
		}
	case *ArrayNode:
		for _, arg := range n.Args {
			l = findIdentities(arg, l)
		}
	case *FuncNode:
		for _, arg := range n.Args {
			l = findIdentities(arg, l)
		}
	}
	return l
}

// FilterSpecialIdentities given a list of identities, filter out
// special identities such as "null", "*", "match_all"
func FilterSpecialIdentities(l []string) []string {
	s := make([]string, 0, len(l))
	for _, val := range l {
		switch strings.ToLower(val) {
		case "*", "match_all", "null", "true", "false":
			// skip
		default:
			s = append(s, val)
		}
	}
	return s
}
func (m IdentityNodes) Strings() []string {
	s := make([]string, len(m))
	for i, in := range m {
		s[i] = in.Text
	}
	return s
}
func (m IdentityNodes) LeftStrings() []string {
	s := make([]string, len(m))
	for i, in := range m {
		l, r, hasLr := in.LeftRight()
		if hasLr {
			s[i] = l
		} else {
			s[i] = r
		}
	}
	return s
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
func (m *FuncNode) Validate() error {

	if m.F.CustomFunc != nil {
		// Nice new style function
		ev, err := m.F.CustomFunc.Validate(m)
		if err != nil {
			return err
		}

		m.Eval = ev
		return nil
	}

	if m.Missing {
		switch strings.ToLower(m.Name) {
		case "distinct":
			return nil
		}
		return nil
	}
	return nil
}
func (m *FuncNode) NodePb() *NodePb {
	n := &FuncNodePb{}
	n.Name = m.Name
	n.Args = make([]NodePb, len(m.Args))
	for i, a := range m.Args {
		//u.Debugf("Func ToPB: arg %T", a)
		n.Args[i] = *a.NodePb()
	}
	return &NodePb{Fn: n}
}
func (m *FuncNode) FromPB(n *NodePb) Node {

	fn, ok := funcs[strings.ToLower(n.Fn.Name)]
	if !ok {
		u.Errorf("Not Found Func %q", n.Fn.Name)
		// Panic?
	}

	f := FuncNode{
		Name: n.Fn.Name,
		Args: NodesFromNodesPb(n.Fn.Args),
		F:    fn,
	}

	if err := f.Validate(); err != nil {
		u.Warnf("could not validate %v", err)
	}

	return &f
}
func (m *FuncNode) Expr() *Expr {
	fe := &Expr{Op: lex.TokenUdfExpr.String()}
	if len(m.Args) > 0 {
		fe.Args = []*Expr{&Expr{Identity: m.Name}}
		fe.Args = append(fe.Args, ExprsFromNodes(m.Args)...)
	}
	return fe
}
func (m *FuncNode) FromExpr(e *Expr) error {
	if e.Op != lex.TokenUdfExpr.String() {
		return fmt.Errorf("Expected 'expr' but got %v", e.Op)
	}
	if len(e.Args) < 1 {
		return fmt.Errorf("Expected function name in args but got none")
	}

	m.Name = e.Args[0].Identity

	if len(e.Args) > 1 {
		args, err := NodesFromExprs(e.Args[1:])
		if err != nil {
			return err
		}
		m.Args = args
	}
	s := m.String()
	n, err := ParseExpression(s)
	if err != nil {
		return fmt.Errorf("Could not round-trip parse func:  %s  err=%v", s, err)
	}
	fn, ok := n.(*FuncNode)
	if !ok {
		return fmt.Errorf("Expected funcnode but got %T", n)
	}
	m.F = fn.F
	if err = fn.Validate(); err != nil {
		return err
	}
	if m.Eval == nil {
		m.Eval = fn.Eval
	}
	return nil
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
	return n, n.load()
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

func (n *NumberNode) load() error {
	// Do integer test first so we get 0x123 etc.
	iv, err := strconv.ParseInt(n.Text, 0, 64) // will fail for -0.
	if err == nil {
		n.IsInt = true
		n.Int64 = iv
	}
	// If an integer extraction succeeded, promote the float.
	if n.IsInt {
		n.IsFloat = true
		n.Float64 = float64(n.Int64)
	} else {
		f, err := strconv.ParseFloat(n.Text, 64)
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
		return fmt.Errorf("illegal number syntax: %q", n.Text)
	}
	return nil
}
func (n *NumberNode) String() string               { return n.Text }
func (m *NumberNode) WriteDialect(w DialectWriter) { w.WriteNumber(m.Text) }
func (m *NumberNode) Validate() error              { return nil }
func (m *NumberNode) NodePb() *NodePb {
	n := &NumberNodePb{}
	n.Text = m.Text
	n.Fv = m.Float64
	n.Iv = m.Int64
	return &NodePb{Nn: n}
}
func (m *NumberNode) FromPB(n *NodePb) Node {
	nn := &NumberNode{
		Text:    n.Nn.Text,
		Float64: n.Nn.Fv,
		Int64:   n.Nn.Iv,
	}
	nn.load()
	return nn
}
func (m *NumberNode) Expr() *Expr {
	return &Expr{Value: m.Text}
}
func (m *NumberNode) FromExpr(e *Expr) error {
	if len(e.Value) > 0 {
		m.Text = e.Value
		return m.load()
	}
	return nil
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
func NewStringNeedsEscape(t lex.Token) *StringNode {
	newVal, needsEscape := StringUnEscape('"', t.V)
	return &StringNode{Text: newVal, Quote: t.Quote, needsEscape: needsEscape}
}
func (m *StringNode) String() string {
	if m.noQuote {
		return m.Text
	}
	if m.Quote > 0 {
		return fmt.Sprintf("%s%s%s", string(m.Quote), StringEscape(rune(m.Quote), m.Text), string(m.Quote))
	}
	return fmt.Sprintf("%q", m.Text)
}
func (m *StringNode) WriteDialect(w DialectWriter) {
	w.WriteLiteral(m.Text)
}
func (m *StringNode) Validate() error { return nil }
func (m *StringNode) NodePb() *NodePb {
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
func (m *StringNode) Expr() *Expr {
	return &Expr{Value: m.Text}
}
func (m *StringNode) FromExpr(e *Expr) error {
	if len(e.Value) > 0 {
		m.Text = e.Value
	}
	return nil
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
func (m *ValueNode) Validate() error { return nil }
func (m *ValueNode) NodePb() *NodePb {
	u.Errorf("Not implemented %#v", m)
	return nil
}
func (m *ValueNode) FromPB(n *NodePb) Node {
	u.Errorf("Not implemented %#v", n)
	return &ValueNode{}
}
func (m *ValueNode) Expr() *Expr {
	return &Expr{Value: m.Value.ToString()}
}
func (m *ValueNode) FromExpr(e *Expr) error {
	if len(e.Value) > 0 {
		m.Value = value.NewStringValue(e.Value)
		return nil
	}
	return fmt.Errorf("unrecognized value")
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

		// This is all deeply flawed, need to go fix it.  Upgrade path will
		// be sweep through and remove all usage of existing ones that used the flawed
		//  `left.right value` escape syntax assuming the period is a split

		if strings.Contains(m.Text, "`.`") {
			//   this came in with quote which has been stripped by lexer
			m.original = fmt.Sprintf("%s%s%s", string(m.Quote), m.Text, string(m.Quote))
			m.left, m.right, _ = LeftRight(m.original)

			//u.Debugf("branch1:  l:%q  r:%q  original:%q text:%q", m.left, m.right, m.original, m.Text)

		} else if strings.Contains(m.Text, "`.") || strings.Contains(m.Text, ".`") {

			m.left, m.right, _ = LeftRight(m.Text)
			l, r := IdentityMaybeQuote(m.Quote, m.left), IdentityMaybeQuote(m.Quote, m.right)
			m.original = fmt.Sprintf("%s.%s", l, r)
			m.left, m.right, _ = LeftRight(m.original)

			//u.Debugf("branch2:  l:%q  r:%q  original:%q text:%q", m.left, m.right, m.original, m.Text)

			//   this came in with quote which has been stripped by lexer
			// m.original = fmt.Sprintf("%s%s%s", string(m.Quote), m.Text, string(m.Quote))
			// m.left, m.right, _ = LeftRight(m.original)

		} else {
			//   this came in with quote which has been stripped by lexer
			m.original = fmt.Sprintf("%s%s%s", string(m.Quote), m.Text, string(m.Quote))
			m.left, m.right, _ = LeftRight(m.original)

			//u.Debugf("branch3:  l:%q  r:%q  original:%q text:%q", m.left, m.right, m.original, m.Text)
		}

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
	if m.Text == "*" {
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
	if m.Text == "*" {
		w.Write([]byte{'*'})
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
func (m *IdentityNode) Validate() error { return nil }
func (m *IdentityNode) IdentityPb() *IdentityNodePb {
	n := &IdentityNodePb{}
	n.Text = m.Text
	q := int32(m.Quote)
	n.Quote = &q
	return n
}
func (m *IdentityNode) NodePb() *NodePb {
	return &NodePb{In: m.IdentityPb()}
}
func (m *IdentityNode) FromPB(n *NodePb) Node {
	q := n.In.Quote
	return &IdentityNode{Text: n.In.Text, Quote: byte(*q)}
}
func (m *IdentityNode) Expr() *Expr {
	if m.IsBooleanIdentity() {
		return &Expr{Value: m.Text}
	}

	if m.HasLeftRight() {
		if IdentityMaybeQuote('`', m.left) != m.left {
			//u.Warnf("This will NOT round-trip  l:%q  r:%q  original:%q text:%q", m.left, m.right, m.original, m.Text)
		}
		return &Expr{Identity: fmt.Sprintf("%s.%s", m.left, m.right)}
	}
	return &Expr{Identity: m.Text}
}
func (m *IdentityNode) FromExpr(e *Expr) error {
	if len(e.Identity) > 0 {
		m.Text = e.Identity
		m.load()
		return nil
	} else if e.Value != "" {
		m.Text = e.Value
		val := strings.ToLower(m.Text)
		if val != "true" && val != "false" {
			return fmt.Errorf("Value identities are either 'true' or 'false' but got %q", m.Text)
		}
		m.load()
		return nil
	}
	return fmt.Errorf("unrecognized identity")
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
			if nt.left == m.left && nt.right == m.right {
				return true
			}
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
func (m *NullNode) Validate() error { return nil }
func (m *NullNode) NodePb() *NodePb { return nil }
func (m *NullNode) FromPB(n *NodePb) Node {
	return &NullNode{}
}
func (m *NullNode) Expr() *Expr {
	return &Expr{Value: "NULL"}
}
func (m *NullNode) FromExpr(e *Expr) error {
	if len(e.Identity) > 0 {
		if strings.ToLower(e.Identity) == "null" {
			return nil
		}
	}
	if len(e.Value) > 0 {
		if strings.ToLower(e.Value) == "null" {
			return nil
		}
	}
	return fmt.Errorf("unrecognized NullNode")
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

func (m *BinaryNode) String() string {
	w := NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
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

/*
Negation
I wanted to do negation on Binaries, but ended up not doing for now

ie, rewrite   NOT (X == "y")   =>  X != "y"

The general problem we ran into is that we lose some fidelity in collapsing
AST that is necessary for other evaluation run-times.

logically `NOT (X > y)` is NOT THE SAME AS  `(X <= y)   due to lack of existince of X

func (m *BinaryNode) ReverseNegation() bool {
	switch m.Operator.T {
	case lex.TokenEqualEqual:
		m.Operator.T = lex.TokenNE
		m.Operator.V = m.Operator.T.String()
	case lex.TokenNE:
		m.Operator.T = lex.TokenEqualEqual
		m.Operator.V = m.Operator.T.String()
	case lex.TokenLT:
		m.Operator.T = lex.TokenGE
		m.Operator.V = m.Operator.T.String()
	case lex.TokenLE:
		m.Operator.T = lex.TokenGT
		m.Operator.V = m.Operator.T.String()
	case lex.TokenGT:
		m.Operator.T = lex.TokenLE
		m.Operator.V = m.Operator.T.String()
	case lex.TokenGE:
		m.Operator.T = lex.TokenLT
		m.Operator.V = m.Operator.T.String()
	default:
		//u.Warnf("What, what is this?   %s", m)
		m.negated = !m.negated
		return true
	}
	return true
}
func (m *BinaryNode) Node() Node { return m }
func (m *BinaryNode) Negated() bool { return m.negated }
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
*/
func (m *BinaryNode) Validate() error {
	if len(m.Args) != 2 {
		return fmt.Errorf("not enough args in binary expected 2 got %d", len(m.Args))
	}
	for _, n := range m.Args {
		if err := n.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (m *BinaryNode) NodePb() *NodePb {
	n := &BinaryNodePb{}
	n.Paren = m.Paren
	n.Op = int32(m.Operator.T)
	n.Args = []NodePb{*m.Args[0].NodePb(), *m.Args[1].NodePb()}
	return &NodePb{Bn: n}
}
func (m *BinaryNode) FromPB(n *NodePb) Node {
	return &BinaryNode{
		Operator: tokenFromInt(n.Bn.Op),
		Paren:    n.Bn.Paren,
		Args:     NodesFromNodesPb(n.Bn.Args),
	}
}
func (m *BinaryNode) Expr() *Expr {
	fe := &Expr{Op: strings.ToLower(m.Operator.V)}
	if len(m.Args) > 0 {
		fe.Args = ExprsFromNodes(m.Args)
	}
	if m.negated {
		return &Expr{Op: "not", Args: []*Expr{fe}}
	}
	return fe
}
func (m *BinaryNode) FromExpr(e *Expr) error {
	if e.Op == "" {
		return fmt.Errorf("unrecognized BinaryNode")
	}
	m.Operator = lex.TokenFromOp(e.Op)
	if len(e.Args) == 0 {
		return fmt.Errorf("Invalid BinaryNode, expected args %+v", e)
	}
	args, err := NodesFromExprs(e.Args)
	if err != nil {
		return err
	}
	m.Args = args
	return nil
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

func (m *BooleanNode) ReverseNegation() bool {
	m.negated = !m.negated
	return true
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
				if nn.ReverseNegation() {
					return nn
				}
			}
			return NewUnary(m.Operator, m.Args[0])
		}
		return m.Args[0]
	}
	return m
}
func (m *BooleanNode) Negated() bool { return m.negated }
func (m *BooleanNode) Validate() error {
	for _, n := range m.Args {
		if err := n.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (m *BooleanNode) NodePb() *NodePb {
	n := &BooleanNodePb{}
	n.Op = int32(m.Operator.T)
	for _, arg := range m.Args {
		n.Args = append(n.Args, *arg.NodePb())
	}
	return &NodePb{Booln: n}
}
func (m *BooleanNode) FromPB(n *NodePb) Node {
	return &BooleanNode{
		Operator: tokenFromInt(n.Booln.Op),
		Args:     NodesFromNodesPb(n.Booln.Args),
	}
}
func (m *BooleanNode) Expr() *Expr {
	fe := &Expr{Op: strings.ToLower(m.Operator.V)}
	if len(m.Args) > 0 {
		fe.Args = ExprsFromNodes(m.Args)
	}
	if m.negated {
		return &Expr{Op: "not", Args: []*Expr{fe}}
	}
	return fe
}
func (m *BooleanNode) FromExpr(e *Expr) error {
	if e.Op == "" {
		return fmt.Errorf("unrecognized BooleanNode op")
	}
	m.Operator = lex.TokenFromOp(e.Op)
	if len(e.Args) == 0 {
		return fmt.Errorf("Invalid BooleanNode, expected args %+v", e)
	}
	args, err := NodesFromExprs(e.Args)
	if err != nil {
		return err
	}
	m.Args = args
	return nil
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
func (m *TriNode) ReverseNegation() bool {
	m.negated = !m.negated
	return true
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
func (m *TriNode) Node() Node    { return m }
func (m *TriNode) Negated() bool { return m.negated }
func (m *TriNode) Validate() error {
	for _, n := range m.Args {
		if err := n.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (m *TriNode) NodePb() *NodePb {
	n := &TriNodePb{Args: make([]NodePb, len(m.Args))}
	n.Op = int32(m.Operator.T)
	for i, arg := range m.Args {
		n.Args[i] = *arg.NodePb()
		//u.Debugf("TriNode NodePb: %T", arg)
	}
	return &NodePb{Tn: n}
}
func (m *TriNode) FromPB(n *NodePb) Node {
	return &TriNode{
		Operator: tokenFromInt(n.Tn.Op),
		Args:     NodesFromNodesPb(n.Tn.Args),
	}
}
func (m *TriNode) Expr() *Expr {
	fe := &Expr{Op: strings.ToLower(m.Operator.V)}
	if len(m.Args) > 0 {
		fe.Args = ExprsFromNodes(m.Args)
	}
	if m.negated {
		return &Expr{Op: "not", Args: []*Expr{fe}}
	}
	return fe
}
func (m *TriNode) FromExpr(e *Expr) error {
	if e.Op == "" {
		return fmt.Errorf("unrecognized TriNode no op")
	}
	m.Operator = lex.TokenFromOp(e.Op)
	if len(e.Args) == 0 {
		return fmt.Errorf("Invalid TriNode, expected args %+v", e)
	}
	if m.Operator.T == lex.TokenNil {
		return fmt.Errorf("Unrecognized op %v", e.Op)
	}
	args, err := NodesFromExprs(e.Args)
	if err != nil {
		return err
	}
	m.Args = args
	return nil
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
	}

	// In the event we are adding a binary here, we might have
	// rewritten a little so lets make sure its interpreted/nested coorectly
	bn, isBinary := arg.(*BinaryNode)
	if isBinary {
		bn.Paren = true
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
func (m *UnaryNode) Validate() error {
	return m.Arg.Validate()
}
func (m *UnaryNode) Node() Node { return m }
func (m *UnaryNode) NodePb() *NodePb {
	n := &UnaryNodePb{}
	n.Arg = *m.Arg.NodePb()
	n.Op = int32(m.Operator.T)
	return &NodePb{Un: n}
}
func (m *UnaryNode) FromPB(n *NodePb) Node {
	return &UnaryNode{
		Operator: tokenFromInt(n.Un.Op),
		Arg:      NodeFromNodePb(&n.Un.Arg),
	}
}
func (m *UnaryNode) Expr() *Expr {
	fe := &Expr{Op: strings.ToLower(m.Operator.V)}
	fe.Args = []*Expr{m.Arg.Expr()}
	return fe
}
func (m *UnaryNode) FromExpr(e *Expr) error {
	if e.Op == "" {
		return fmt.Errorf("unrecognized UnaryNode no op")
	}
	m.Operator = lex.TokenFromOp(e.Op)
	if m.Operator.T == lex.TokenNil {
		return fmt.Errorf("Unrecognized op %v", e.Op)
	}
	if len(e.Args) == 0 {
		return fmt.Errorf("Invalid UnaryNode, expected 1 args %+v", e)
	}
	if len(e.Args) > 1 {
		return fmt.Errorf("Invalid UnaryNode, expected 1 args %+v", e)
	}
	arg, err := NodeFromExpr(e.Args[0])
	if err != nil {
		return err
	}
	m.Arg = arg
	return nil
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
func (m *IncludeNode) ReverseNegation() bool {
	m.negated = !m.negated
	return true
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
func (m *IncludeNode) Validate() error { return nil }
func (m *IncludeNode) Negated() bool   { return m.negated }
func (m *IncludeNode) Node() Node      { return m }
func (m *IncludeNode) NodePb() *NodePb {
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
func (m *IncludeNode) Expr() *Expr {
	fe := &Expr{Op: lex.TokenInclude.String()}
	fe.Args = []*Expr{m.Identity.Expr()}
	if m.negated {
		return &Expr{Op: "not", Args: []*Expr{fe}}
	}
	return fe
}
func (m *IncludeNode) FromExpr(e *Expr) error {
	if e.Op == "" {
		return fmt.Errorf("Invalid IncludeNode %+v", e)
	}
	m.Operator = lex.TokenFromOp(e.Op)
	if m.Operator.T == lex.TokenNil {
		return fmt.Errorf("Unrecognized op %v", e.Op)
	}
	if len(e.Args) == 0 {
		return fmt.Errorf("Invalid IncludeNode, expected 1 args %+v", e)
	}
	if len(e.Args) > 1 {
		return fmt.Errorf("Invalid IncludeNode, expected 1 args %+v", e)
	}
	arg, err := NodeFromExpr(e.Args[0])
	if err != nil {
		return err
	}
	in, ok := arg.(*IdentityNode)
	if !ok {
		return fmt.Errorf("Invalid IncludeNode, expected 1 Identity %+v", e)
	}
	m.Identity = in
	return nil
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
func (m *ArrayNode) Validate() error {
	for _, n := range m.Args {
		if err := n.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (m *ArrayNode) Append(n Node) { m.Args = append(m.Args, n) }
func (m *ArrayNode) NodePb() *NodePb {
	n := &ArrayNodePb{Args: make([]NodePb, len(m.Args))}
	iv := int32(0)
	if m.wraptype != "" && len(m.wraptype) == 1 {
		iv = int32(m.wraptype[0])
	}
	n.Wrap = &iv
	for i, arg := range m.Args {
		n.Args[i] = *arg.NodePb()
	}
	return &NodePb{An: n}
}
func (m *ArrayNode) FromPB(n *NodePb) Node {
	return &ArrayNode{
		Args: NodesFromNodesPb(n.An.Args),
	}
}
func (m *ArrayNode) Expr() *Expr {
	fe := &Expr{}
	if len(m.Args) > 0 {
		fe.Args = ExprsFromNodes(m.Args)
	}
	return fe
}
func (m *ArrayNode) FromExpr(e *Expr) error {
	if len(e.Op) > 0 {
		return fmt.Errorf("Unrecognized expression %+v", e)
	}
	if len(e.Args) == 0 {
		return fmt.Errorf("Invalid ArrayNode No args %+v", e)
	}
	args, err := NodesFromExprs(e.Args)
	if err != nil {
		return err
	}
	m.Args = args
	return nil
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
		pbs[i] = n.NodePb()
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

func ExprsFromNodes(nodes []Node) []*Expr {
	ex := make([]*Expr, len(nodes))
	for i, n := range nodes {
		ex[i] = n.Expr()
	}
	return ex
}
func NodesFromExprs(args []*Expr) ([]Node, error) {
	nl := make([]Node, len(args))
	for i, e := range args {
		n, err := NodeFromExpr(e)
		if err != nil {
			return nil, err
		}
		nl[i] = n
	}
	return nl, nil
}
func NodeFromExpr(e *Expr) (Node, error) {
	if e == nil {
		return nil, fmt.Errorf("Nil expression?")
	}
	var n Node
	if e.Op != "" {
		/*
			// Logic, Expressions, Operators etc
			TokenMultiply:   {Kw: "*", Description: "Multiply"},
			TokenMinus:      {Kw: "-", Description: "-"},
			TokenPlus:       {Kw: "+", Description: "+"},
			TokenPlusPlus:   {Kw: "++", Description: "++"},
			TokenPlusEquals: {Kw: "+=", Description: "+="},
			TokenDivide:     {Kw: "/", Description: "Divide /"},
			TokenModulus:    {Kw: "%", Description: "Modulus %"},
			TokenEqual:      {Kw: "=", Description: "Equal"},
			TokenEqualEqual: {Kw: "==", Description: "=="},
			TokenNE:         {Kw: "!=", Description: "NE"},
			TokenGE:         {Kw: ">=", Description: "GE"},
			TokenLE:         {Kw: "<=", Description: "LE"},
			TokenGT:         {Kw: ">", Description: "GT"},
			TokenLT:         {Kw: "<", Description: "LT"},
			TokenIf:         {Kw: "if", Description: "IF"},
			TokenAnd:        {Kw: "&&", Description: "&&"},
			TokenOr:         {Kw: "||", Description: "||"},
			TokenLogicOr:    {Kw: "or", Description: "Or"},
			TokenLogicAnd:   {Kw: "and", Description: "And"},
			TokenIN:         {Kw: "in", Description: "IN"},
			TokenLike:       {Kw: "like", Description: "LIKE"},
			TokenNegate:     {Kw: "not", Description: "NOT"},
			TokenBetween:    {Kw: "between", Description: "between"},
			TokenIs:         {Kw: "is", Description: "IS"},
			TokenNull:       {Kw: "null", Description: "NULL"},
			TokenContains:   {Kw: "contains", Description: "contains"},
			TokenIntersects: {Kw: "intersects", Description: "intersects"},
		*/
		e.Op = strings.ToLower(e.Op)
		switch e.Op {
		case "expr":
			// udf
			n = &FuncNode{}
		case "and", "or":
			// bool
			n = &BooleanNode{}
		case "include":
			n = &IncludeNode{}
		case "not":
			// This is a special Case, it is possible its urnary
			// but in general we can collapse it
			n = &UnaryNode{}
		case "exists":
			n = &UnaryNode{}
		case "between":
			n = &TriNode{}
		case "=", "-", "+", "++", "+=", "/", "%", "==", "<=", "!=", ">=", ">", "<", "*",
			"like", "contains", "intersects", "in":

			// very weird special case for FILTER * where the * is an ident not op
			if e.Op == "*" && len(e.Args) == 0 {
				n = &IdentityNode{Text: e.Op, left: e.Op}
				return n, nil
			}
			n = &BinaryNode{}
		}
		if n == nil {
			u.Warnf("unrecognized op? %v", e.Op)
			return nil, fmt.Errorf("Unknown op %v", e.Op)
		}
		err := n.FromExpr(e)
		if err != nil {
			return nil, err
		}

		//u.Debugf("%T  %s", n, n)

		// Negateable nodes possibly can be collapsed to simpler form
		nn, isNegateable := n.(NegateableNode)
		if isNegateable {
			return nn.Node(), nil
		}

		return n, nil
	}
	if e.Identity != "" {
		n = &IdentityNode{}
		return n, n.FromExpr(e)
	}
	if e.Value != "" {
		switch e.Value {
		case "true", "false":
			n = &IdentityNode{}
		default:
			n = &StringNode{}
		}
		return n, n.FromExpr(e)
	}
	if len(e.Args) > 0 {
		n = &ArrayNode{}
		return n, n.FromExpr(e)
	}
	return nil, fmt.Errorf("Unrecognized expression %+v", e)
}
