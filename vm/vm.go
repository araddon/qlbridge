package vm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	ErrUnknownOp       = fmt.Errorf("expr: unknown op type")
	ErrUnknownNodeType = fmt.Errorf("expr: unknown node type")
	ErrExecute         = fmt.Errorf("Could not execute")
	_                  = u.EMPTY

	SchemaInfoEmpty = &NoSchema{}

	// our DataTypes we support, a limited sub-set of go
	floatRv   = reflect.ValueOf(float64(1.2))
	int64Rv   = reflect.ValueOf(int64(1))
	int32Rv   = reflect.ValueOf(int32(1))
	stringRv  = reflect.ValueOf("")
	stringsRv = reflect.ValueOf([]string{""})
	boolRv    = reflect.ValueOf(true)
	mapIntRv  = reflect.ValueOf(map[string]int64{"hi": int64(1)})
	timeRv    = reflect.ValueOf(time.Time{})
	nilRv     = reflect.ValueOf(nil)
)

type State struct {
	ExprVm // reference to the VM operating on this state
	// We make a reflect value of self (state) as we use []reflect.ValueOf often
	rv reflect.Value
	expr.ContextReader
	Writer expr.ContextWriter
}

func NewState(vm ExprVm, read expr.ContextReader, write expr.ContextWriter) *State {
	s := &State{
		ExprVm:        vm,
		ContextReader: read,
		Writer:        write,
	}
	s.rv = reflect.ValueOf(s)
	return s
}

type EvalBaseContext struct {
	expr.ContextReader
}
type EvaluatorFunc func(ctx expr.EvalContext) (value.Value, bool)

type ExprVm interface {
	Execute(writeContext expr.ContextWriter, readContext expr.ContextReader) error
}

type NoSchema struct {
}

func (m *NoSchema) Key() string { return "" }

// A node vm is a vm for parsing, evaluating a single tree-node
//
type Vm struct {
	*expr.Tree
}

func (m *Vm) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func NewVm(exprText string) (*Vm, error) {
	t, err := expr.ParseExpression(exprText)
	if err != nil {
		return nil, err
	}
	m := &Vm{
		Tree: t,
	}
	return m, nil
}

// Execute applies a parse expression to the specified context's
func (m *Vm) Execute(writeContext expr.ContextWriter, readContext expr.ContextReader) (err error) {
	//defer errRecover(&err)
	s := &State{
		ExprVm:        m,
		ContextReader: readContext,
	}
	s.rv = reflect.ValueOf(s)
	//u.Debugf("vm.Execute:  %#v", m.Tree.Root)
	v, ok := s.Walk(m.Tree.Root)
	if ok {
		// Special Vm that doesnt' have named fields, single tree expression
		//u.Debugf("vm.Walk val:  %v", v)
		writeContext.Put(SchemaInfoEmpty, readContext, v)
		return nil
	}
	return ErrExecute
}

// errRecover is the handler that turns panics into returns from the top
// level of
func errRecover(errp *error) {
	e := recover()
	if e != nil {
		switch err := e.(type) {
		case runtime.Error:
			panic(e)
		case error:
			*errp = err
		default:
			panic(e)
		}
	}
}

// creates a new Value with a nil group and given value.
// TODO:  convert this to an interface method on nodes called Value()
func numberNodeToValue(t *expr.NumberNode) (v value.Value) {
	//u.Debugf("nodeToValue()  isFloat?%v", t.IsFloat)
	if t.IsInt {
		v = value.NewIntValue(t.Int64)
	} else if t.IsFloat {
		v = value.NewNumberValue(value.ToFloat64(reflect.ValueOf(t.Text)))
	} else {
		u.Errorf("Could not find type? %v", t.Type())
	}
	//u.Debugf("return nodeToValue()	%v  %T  arg:%T", v, v, t)
	return v
}

func Evaluator(arg expr.Node) EvaluatorFunc {
	u.Debugf("Evaluator() node=%T  %v", arg, arg)
	switch argVal := arg.(type) {
	case *expr.NumberNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return numberNodeToValue(argVal), true }
	case *expr.BinaryNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkBinary(ctx, argVal), true }
	case *expr.UnaryNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkUnary(ctx, argVal) }
	case *expr.FuncNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkFunc(ctx, argVal) }
	case *expr.IdentityNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkIdentity(ctx, argVal) }
	case *expr.StringNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return value.NewStringValue(argVal.Text), true }
	case *expr.TriNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkTri(ctx, argVal) }
	case *expr.MultiArgNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkMulti(ctx, argVal) }
	default:
		u.Errorf("Unknonwn node type:  %T", argVal)
		panic(ErrUnknownNodeType)
	}
}

func Eval(ctx expr.EvalContext, arg expr.Node) (value.Value, bool) {
	//u.Debugf("Eval() node=%T  %v", arg, arg)
	// can we switch to arg.Type()
	switch argVal := arg.(type) {
	case *expr.NumberNode:
		return numberNodeToValue(argVal), true
	case *expr.BinaryNode:
		return walkBinary(ctx, argVal), true
	case *expr.UnaryNode:
		return walkUnary(ctx, argVal)
	case *expr.TriNode:
		return walkTri(ctx, argVal)
	case *expr.MultiArgNode:
		return walkMulti(ctx, argVal)
	case *expr.FuncNode:
		//return walkFunc(argVal)
		return walkFunc(ctx, argVal)
	case *expr.IdentityNode:
		return walkIdentity(ctx, argVal)
	case *expr.StringNode:
		return value.NewStringValue(argVal.Text), true
	default:
		u.Errorf("Unknonwn node type:  %T", argVal)
		panic(ErrUnknownNodeType)
	}
}

func (e *State) Walk(arg expr.Node) (value.Value, bool) {
	return Eval(e.ContextReader, arg)
}

func walkBinary(ctx expr.EvalContext, node *expr.BinaryNode) value.Value {
	ar, aok := Eval(ctx, node.Args[0])
	br, bok := Eval(ctx, node.Args[1])
	if !aok || !bok {
		//u.Warnf("not ok: %v  l:%v  r:%v  %T  %T", node, ar, br, ar, br)
		return nil
	}
	//u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, ar, br, ar, br)
	switch at := ar.(type) {
	case value.IntValue:
		switch bt := br.(type) {
		case value.IntValue:
			//u.Debugf("doing operate ints  %v %v  %v", at, node.Operator.V, bt)
			n := operateInts(node.Operator, at, bt)
			return n
		case value.NumberValue:
			//u.Debugf("doing operate ints/numbers  %v %v  %v", at, node.Operator.V, bt)
			n := operateNumbers(node.Operator, at.NumberValue(), bt)
			return n
		default:
			u.Errorf("unknown type:  %T %v", bt, bt)
			panic(ErrUnknownOp)
		}
	case value.NumberValue:
		switch bt := br.(type) {
		case value.IntValue:
			n := operateNumbers(node.Operator, at, bt.NumberValue())
			return n
		case value.NumberValue:
			n := operateNumbers(node.Operator, at, bt)
			return n
		default:
			u.Errorf("unknown type:  %T %v", bt, bt)
			panic(ErrUnknownOp)
		}
	case value.BoolValue:
		switch bt := br.(type) {
		case value.BoolValue:
			atv, btv := at.Value().(bool), bt.Value().(bool)
			switch node.Operator.T {
			case lex.TokenLogicAnd:
				return value.NewBoolValue(atv && btv)
			case lex.TokenLogicOr:
				return value.NewBoolValue(atv || btv)
			case lex.TokenEqualEqual:
				return value.NewBoolValue(atv == btv)
			case lex.TokenNE:
				return value.NewBoolValue(atv != btv)
			default:
				u.Infof("bool binary?:  %v  %v", at, bt)
				panic(ErrUnknownOp)
			}

		default:
			u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), bt, bt.Value())
			panic(ErrUnknownOp)
		}
	case value.StringValue:
		// TODO:  this doesn't make sense, we should be able to operatre on strings
		//    such as "Contains", "Split", ""indexof" etc
		if at.CanCoerce(int64Rv) {
			switch bt := br.(type) {
			case value.StringValue:
				n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
				return n
			case value.IntValue:
				n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
				return n
			case value.NumberValue:
				n := operateNumbers(node.Operator, at.NumberValue(), bt)
				return n
			default:
				u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), bt, bt.Value())
				panic(ErrUnknownOp)
			}
		} else {
			u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), br, br)
		}
		// case nil:
		// 	// TODO, remove this case?  is this valid?  used?
		// 	switch bt := br.(type) {
		// 	case StringValue:
		// 		n := operateNumbers(node.Operator, NumberNaNValue, bt.NumberValue())
		// 		return n
		// 	case IntValue:
		// 		n := operateNumbers(node.Operator, NumberNaNValue, bt.NumberValue())
		// 		return n
		// 	case NumberValue:
		// 		n := operateNumbers(node.Operator, NumberNaNValue, bt)
		// 		return n
		// 	case nil:
		// 		u.Errorf("a && b nil? at?%v  %v    %v", at, bt, node.Operator)
		// 	default:
		// 		u.Errorf("nil at?%v  %T      %v", at, bt, node.Operator)
		// 		panic(ErrUnknownOp)
		// 	}
		// default:
		u.Errorf("Unknown op?  %T  %T  %v", ar, at, ar)
		panic(ErrUnknownOp)
	}

	return nil
}

func walkIdentity(ctx expr.EvalContext, node *expr.IdentityNode) (value.Value, bool) {

	if node.IsBooleanIdentity() {
		//u.Debugf("walkIdentity() boolean: node=%T  %v Bool:%v", node, node, node.Bool())
		return value.NewBoolValue(node.Bool()), true
	}
	if ctx == nil {
		return value.NewStringValue(node.String()), true
	}
	//u.Debugf("walkIdentity() node=%T  %v", node, node)
	return ctx.Get(node.Text)
}

func walkUnary(ctx expr.EvalContext, node *expr.UnaryNode) (value.Value, bool) {

	a, ok := Eval(ctx, node.Arg)
	if !ok {
		u.Infof("whoops, %#v", node)
		return a, false
	}
	switch node.Operator.T {
	case lex.TokenNegate:
		switch argVal := a.(type) {
		case value.BoolValue:
			//u.Infof("found urnary bool:  res=%v   expr=%v", !argVal.v, node.StringAST())
			return value.NewBoolValue(!argVal.V), true
		default:
			//u.Errorf("urnary type not implementedUnknonwn node type:  %T", argVal)
			panic(ErrUnknownNodeType)
		}
	case lex.TokenMinus:
		if an, aok := a.(value.NumericValue); aok {
			return value.NewNumberValue(-an.Float()), true
		}
	default:
		u.Warnf("urnary not implemented:   %#v", node)
	}

	return value.NewNilValue(), false
}

// TriNode evaluator
//
//     A   BETWEEN   B  AND C
//
func walkTri(ctx expr.EvalContext, node *expr.TriNode) (value.Value, bool) {

	a, aok := Eval(ctx, node.Args[0])
	b, bok := Eval(ctx, node.Args[1])
	c, cok := Eval(ctx, node.Args[2])
	u.Infof("tri:  %T:%v  %v  %T:%v   %T:%v", a, a, node.Operator, b, b, c, c)
	if !aok || !bok || !cok {
		u.Infof("Could not evaluate args, %#v", node.String())
		return value.BoolValueFalse, false
	}
	switch node.Operator.T {
	case lex.TokenBetween:
		switch a.Type() {
		case value.IntType:
			u.Infof("found tri:  %v %v %v  expr=%v", a, b, c, node.StringAST())
			if aiv, ok := a.(value.IntValue); ok {
				if biv, ok := b.(value.IntValue); ok {
					if civ, ok := c.(value.IntValue); ok {
						if aiv.Int() > biv.Int() && aiv.Int() < civ.Int() {
							return value.NewBoolValue(true), true
						} else {
							return value.NewBoolValue(false), true
						}
					}
				}
			}
			return value.BoolValueFalse, false
		case value.NumberType:
			u.Infof("found tri:  %v %v %v  expr=%v", a, b, c, node.StringAST())
			if afv, ok := a.(value.NumberValue); ok {
				if bfv, ok := b.(value.NumberValue); ok {
					if cfv, ok := c.(value.NumberValue); ok {
						if afv.Float() > bfv.Float() && afv.Float() < cfv.Float() {
							return value.NewBoolValue(true), false
						} else {
							return value.NewBoolValue(false), true
						}
					}
				}
			}
			return value.BoolValueFalse, false
		default:
			u.Warnf("tri node walk not implemented:   %#v", node)
		}
	default:
		u.Warnf("tri node walk not implemented:   %#v", node)
	}

	return value.NewNilValue(), false
}

// MultiNode evaluator
//
//     A   IN   (b,c,d)
//
func walkMulti(ctx expr.EvalContext, node *expr.MultiArgNode) (value.Value, bool) {

	a, aok := Eval(ctx, node.Args[0])
	u.Infof("multi:  %T:%v  %v", a, a, node.Operator)
	if !aok {
		u.Infof("Could not evaluate args, %#v", node.Args[0])
		return value.BoolValueFalse, false
	}
	switch node.Operator.T {
	case lex.TokenIN:
		for i := 1; i < len(node.Args); i++ {
			v, ok := Eval(ctx, node.Args[i])
			if ok {
				u.Debugf("in? %v %v", a, v)
				if eq, err := value.Equal(a, v); eq && err == nil {
					return value.NewBoolValue(true), true
				}
			} else {
				u.Warnf("could not evaluate arg: %v", node.Args[i])
			}
		}
		return value.NewBoolValue(false), true
	default:
		u.Warnf("tri node walk not implemented:   %#v", node)
	}

	return value.NewNilValue(), false
}

func walkFunc(ctx expr.EvalContext, node *expr.FuncNode) (value.Value, bool) {

	// u.Debugf("walk node --- %v   ", node.StringAST())

	// we create a set of arguments to pass to the function, first arg
	// is this Context
	var ok bool
	funcArgs := []reflect.Value{reflect.ValueOf(ctx)}
	for _, a := range node.Args {

		//u.Debugf("arg %v  %T %v", a, a, a.Type().Kind())

		var v interface{}

		switch t := a.(type) {
		case *expr.StringNode: // String Literal
			v = value.NewStringValue(t.Text)
		case *expr.IdentityNode: // Identity node = lookup in context

			if t.IsBooleanIdentity() {
				v = value.NewBoolValue(t.Bool())
			} else {
				v, ok = ctx.Get(t.Text)
				if !ok {
					// nil arguments are valid
					v = value.NewNilValue()
				}
			}

		case *expr.NumberNode:
			v = numberNodeToValue(t)
		case *expr.FuncNode:
			//u.Debugf("descending to %v()", t.Name)
			v, ok = walkFunc(ctx, t)
			if !ok {
				return value.NewNilValue(), false
			}
			//u.Debugf("result of %v() = %v, %T", t.Name, v, v)
			//v = extractScalar()
		case *expr.UnaryNode:
			//v = extractScalar(e.walkUnary(t))
			v, ok = walkUnary(ctx, t)
			if !ok {
				return value.NewNilValue(), false
			}
		case *expr.BinaryNode:
			//v = extractScalar(e.walkBinary(t))
			v = walkBinary(ctx, t)
		default:
			panic(fmt.Errorf("expr: unknown func arg type"))
		}

		if v == nil {
			//u.Warnf("Nil vals?  %v  %T  arg:%T", v, v, a)
			// What do we do with Nil Values?
			switch a.(type) {
			case *expr.StringNode: // String Literal
				u.Warnf("NOT IMPLEMENTED T:%T v:%v", a, a)
			case *expr.IdentityNode: // Identity node = lookup in context
				v = value.NewStringValue("")
			default:
				u.Warnf("unknown type:  %v  %T", v, v)
			}

			funcArgs = append(funcArgs, reflect.ValueOf(v))
		} else {
			//u.Debugf(`found func arg:  key="%v"  %T  arg:%T`, v, v, a)
			funcArgs = append(funcArgs, reflect.ValueOf(v))
		}

	}
	// Get the result of calling our Function (Value,bool)
	//u.Debugf("Calling %v func:%v(%v)", node.F.F, node.F.Name, funcArgs)
	fnRet := node.F.F.Call(funcArgs)
	//u.Infof("fnRet: %v", fnRet)
	// check if has an error response?
	if len(fnRet) > 1 && !fnRet[1].Bool() {
		// What do we do if not ok?
		return value.EmptyStringValue, false
	}
	//u.Debugf("response %v %v  %T", node.F.Name, fnRet[0].Interface(), fnRet[0].Interface())
	return fnRet[0].Interface().(value.Value), true
}

func operateNumbers(op lex.Token, av, bv value.NumberValue) value.Value {
	switch op.T {
	case lex.TokenPlus, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide, lex.TokenMinus,
		lex.TokenModulus:
		if math.IsNaN(av.V) || math.IsNaN(bv.V) {
			return value.NewNumberValue(math.NaN())
		}
	}

	//
	a, b := av.V, bv.V
	switch op.T {
	case lex.TokenPlus: // +
		return value.NewNumberValue(a + b)
	case lex.TokenStar, lex.TokenMultiply: // *
		return value.NewNumberValue(a * b)
	case lex.TokenMinus: // -
		return value.NewNumberValue(a - b)
	case lex.TokenDivide: //    /
		return value.NewNumberValue(a / b)
	case lex.TokenModulus: //    %
		// is this even valid?   modulus on floats?
		return value.NewNumberValue(float64(int64(a) % int64(b)))

	// Below here are Boolean Returns
	case lex.TokenEqualEqual, lex.TokenEqual: //  ==
		//u.Infof("==?  %v  %v", av, bv)
		if a == b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenGT: //  >
		if a > b {
			//r = 1
			return value.BoolValueTrue
		} else {
			//r = 0
			return value.BoolValueFalse
		}
	case lex.TokenNE: //  !=    or <>
		if a != b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLT: // <
		if a < b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenGE: // >=
		if a >= b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLE: // <=
		if a <= b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLogicOr, lex.TokenOr: //  ||
		if a != 0 || b != 0 {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLogicAnd: //  &&
		if a != 0 && b != 0 {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	}
	panic(fmt.Errorf("expr: unknown operator %s", op))
}

func operateInts(op lex.Token, av, bv value.IntValue) value.Value {
	//if math.IsNaN(a) || math.IsNaN(b) {
	//	return math.NaN()
	//}
	a, b := av.V, bv.V
	//u.Infof("a op b:   %v %v %v", a, op.V, b)
	switch op.T {
	case lex.TokenPlus: // +
		//r = a + b
		return value.NewIntValue(a + b)
	case lex.TokenStar, lex.TokenMultiply: // *
		//r = a * b
		return value.NewIntValue(a * b)
	case lex.TokenMinus: // -
		//r = a - b
		return value.NewIntValue(a - b)
	case lex.TokenDivide: //    /
		//r = a / b
		//u.Debugf("divide:   %v / %v = %v", a, b, a/b)
		return value.NewIntValue(a / b)
	case lex.TokenModulus: //    %
		//r = a / b
		//u.Debugf("modulus:   %v / %v = %v", a, b, a/b)
		return value.NewIntValue(a % b)

	// Below here are Boolean Returns
	case lex.TokenEqualEqual: //  ==
		if a == b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenGT: //  >
		if a > b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenNE: //  !=    or <>
		if a != b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLT: // <
		if a < b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenGE: // >=
		if a >= b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLE: // <=
		if a <= b {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLogicOr: //  ||
		if a != 0 || b != 0 {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	case lex.TokenLogicAnd: //  &&
		if a != 0 && b != 0 {
			return value.BoolValueTrue
		} else {
			return value.BoolValueFalse
		}
	}
	panic(fmt.Errorf("expr: unknown operator %s", op))
}

func uoperate(op string, a float64) (r float64) {
	switch op {
	case "!":
		if a == 0 {
			r = 1
		} else {
			r = 0
		}
	case "-":
		r = -a
	default:
		panic(fmt.Errorf("expr: unknown operator %s", op))
	}
	return
}

// extractScalar will return a float64 if res contains exactly one scalar.
func extractScalarXXX(v value.Value) interface{} {
	// if len(res.Results) == 1 && res.Results[0].Type() == TYPE_SCALAR {
	// 	return float64(res.Results[0].Value.Value().(Scalar))
	// }
	// return res
	return nil
}
