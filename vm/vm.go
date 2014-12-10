package vm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlbridge/lex"
)

var (
	ErrUnknownOp       = fmt.Errorf("expr: unknown op type")
	ErrUnknownNodeType = fmt.Errorf("expr: unknown node type")
	_                  = u.EMPTY
)

type State struct {
	ExprVm // reference to the VM operating on this state
	// We make a reflect value of self (state) as we use []reflect.ValueOf often
	rv    reflect.Value
	read  ContextReader
	write ContextWriter
}

type ExprVm interface {
	Execute(writeContext ContextWriter, readContext ContextReader) error
}

// A node vm is a vm for parsing, evaluating a single tree-node
//
type Vm struct {
	*Tree
}

func (m *Vm) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func NewVm(expr string) (*Vm, error) {
	t, err := ParseExpression(expr)
	if err != nil {
		return nil, err
	}
	m := &Vm{
		Tree: t,
	}
	return m, nil
}

// Execute applies a parse expression to the specified context's
func (m *Vm) Execute(writeContext ContextWriter, readContext ContextReader) (err error) {
	//defer errRecover(&err)
	s := &State{
		ExprVm: m,
		read:   readContext,
	}
	s.rv = reflect.ValueOf(s)
	u.Debugf("vm.Execute:  %#v", m.Tree.Root)
	v := s.walk(m.Tree.Root)
	writeContext.Put("", v)
	//writeContext.Put()
	return
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
func nodeToValue(t *NumberNode) (v Value) {
	//u.Debugf("nodeToValue()  isFloat?%v", t.IsFloat)
	if t.IsInt {
		v = NewIntValue(t.Int64)
	} else if t.IsFloat {
		v = NewNumberValue(ToFloat64(reflect.ValueOf(t.Text)))
	} else {
		u.Errorf("Could not find type? %v", t.Type())
	}
	//u.Debugf("return nodeToValue()	%v  %T  arg:%T", v, v, t)
	return v
}

func (e *State) walk(arg ExprArg) Value {
	u.Debugf("walk() node=%T  %v", arg, arg)
	switch argVal := arg.(type) {
	case *NumberNode:
		return nodeToValue(argVal)
	case *BinaryNode:
		return e.walkBinary(argVal)
	case *UnaryNode:
		return e.walkUnary(argVal)
	case *FuncNode:
		return e.walkFunc(argVal)
	case *IdentityNode:
		return e.walkIdentity(argVal)
	default:
		u.Errorf("Unknonwn node type:  %T", argVal)
		panic(ErrUnknownNodeType)
	}
}

// func (e *State) walkArg(arg ExprArg) Value {
// 	u.Debugf("walkArg() arg=%T  %v", arg, arg)
// 	switch node := arg.(type) {
// 	case *NumberNode:
// 		return nodeToValue(node)
// 	case *BinaryNode:
// 		return e.walkBinary(node)
// 	case *UnaryNode:
// 		return e.walkUnary(node)
// 	case *FuncNode:
// 		return e.walkFunc(node)
// 	default:
// 		panic(ErrUnknownNodeType)
// 	}
// }

func (e *State) walkBinary(node *BinaryNode) Value {
	ar := e.walk(node.Args[0])
	br := e.walk(node.Args[1])
	u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, ar, br, ar, br)
	switch at := ar.(type) {
	case IntValue:
		switch bt := br.(type) {
		case IntValue:
			u.Debug("doing operate ints")
			n := operateInts(node.Operator, at, bt)
			return n
		case NumberValue:
			n := operateNumbers(node.Operator, at.NumberValue(), bt)
			return n
		default:
			u.Errorf("unknown type:  %T %v", bt, bt)
			panic(ErrUnknownOp)
		}
	case NumberValue:
		switch bt := br.(type) {
		case IntValue:
			n := operateNumbers(node.Operator, at, bt.NumberValue())
			return n
		case NumberValue:
			n := operateNumbers(node.Operator, at, bt)
			return n
		default:
			u.Errorf("unknown type:  %T %v", bt, bt)
			panic(ErrUnknownOp)
		}
	case StringValue:
		if at.CanCoerce(int64Rv) {
			switch bt := br.(type) {
			case StringValue:
				n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
				return n
			case IntValue:
				n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
				return n
			case NumberValue:
				n := operateNumbers(node.Operator, at.NumberValue(), bt)
				return n
			default:
				u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), bt, bt.Value())
				panic(ErrUnknownOp)
			}
		} else {
			u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), br, br)
		}
	case nil:
		switch bt := br.(type) {
		case StringValue:
			n := operateNumbers(node.Operator, NumberNilValue, bt.NumberValue())
			return n
		case IntValue:
			n := operateNumbers(node.Operator, NumberNilValue, bt.NumberValue())
			return n
		case NumberValue:
			n := operateNumbers(node.Operator, NumberNilValue, bt)
			return n
		case nil:
			u.Errorf("a && b nil? at?%v  %v    %v", at, bt, node.Operator)
		default:
			u.Errorf("nil at?%v  %T      %v", at, bt, node.Operator)
			panic(ErrUnknownOp)
		}
	default:
		u.Errorf("Unknown op?  %T  %T  %v", ar, at, ar)
		panic(ErrUnknownOp)
	}

	return nil
}

func (e *State) walkIdentity(node *IdentityNode) Value {
	//u.Debugf("walkIdentity() node=%T  %v", node, node)
	return e.read.Get(node.Text)
}

func (e *State) walkUnary(node *UnaryNode) Value {
	// a := e.walk(node.Arg)
	// for _, r := range a.Results {
	// 	if an, aok := r.Value.(Scalar); aok && math.IsNaN(float64(an)) {
	// 		r.Value = Scalar(math.NaN())
	// 		continue
	// 	}
	// 	switch rt := r.Value.(type) {
	// 	case Scalar:
	// 		r.Value = Scalar(uoperate(node.OpStr, float64(rt)))
	// 	case Number:
	// 		r.Value = Number(uoperate(node.OpStr, float64(rt)))
	// 	default:
	// 		panic(ErrUnknownOp)
	// 	}
	// }
	// return a
	return nil
}

func (e *State) walkFunc(node *FuncNode) Value {

	u.Debugf("walk node --- %v   ", node.StringAST())

	//we create a set of arguments to pass to the function, first arg
	// is this *State
	funcArgs := []reflect.Value{e.rv}
	for _, a := range node.Args {

		u.Debugf("arg %v  %T %v", a, a, a.Type().Kind())

		var v interface{}
		switch t := a.(type) {
		case *StringNode: // String Literal
			v = t.Text
		case *IdentityNode: // Identity node = lookup in context
			v = e.read.Get(t.Text)
		case *NumberNode:
			v = nodeToValue(t)
		case *FuncNode:
			//u.Debugf("descending to %v()", t.Name)
			v = e.walkFunc(t)
			u.Debugf("result of %v() = %v, %T", t.Name, v, v)
			//v = extractScalar()
		case *UnaryNode:
			//v = extractScalar(e.walkUnary(t))
			v = e.walkUnary(t)
		case *BinaryNode:
			//v = extractScalar(e.walkBinary(t))
			v = e.walkBinary(t)
		default:
			panic(fmt.Errorf("expr: unknown func arg type"))
		}

		if v == nil {
			u.Warnf("Nil vals?  %v  %T  arg:%T", v, v, a)
			// What do we do with Nil Values?
			switch a.(type) {
			case *StringNode: // String Literal

			case *IdentityNode: // Identity node = lookup in context
				v = NewStringValue("")
			default:
				u.Warnf("unknown type:  %v  %T", v, v)
			}

			funcArgs = append(funcArgs, reflect.ValueOf(v))
		} else {
			u.Debugf("%v  %T  arg:%T", v, v, a)
			funcArgs = append(funcArgs, reflect.ValueOf(v))
		}

	}
	// Get the result of calling our Function
	u.Debugf("Calling func:%v(%v)", node.F.Name, funcArgs)
	fr := node.F.F.Call(funcArgs)
	res := fr[0].Interface().(Value)
	if len(fr) > 1 && !fr[1].IsNil() {
		err := fr[1].Interface().(error)
		if err != nil {
			panic(err)
		}
	}
	return res
}

func operateNumbers(op ql.Token, av, bv NumberValue) Value {
	switch op.T {
	case ql.TokenPlus, ql.TokenStar, ql.TokenMultiply, ql.TokenDivide, ql.TokenMinus,
		ql.TokenModulus:
		if math.IsNaN(av.v) || math.IsNaN(bv.v) {
			return NewNumberValue(math.NaN())
		}
	}

	//
	a, b := av.v, bv.v
	switch op.T {
	case ql.TokenPlus: // +
		return NewNumberValue(a + b)
	case ql.TokenStar, ql.TokenMultiply: // *
		return NewNumberValue(a * b)
	case ql.TokenMinus: // -
		return NewNumberValue(a - b)
	case ql.TokenDivide: //    /
		return NewNumberValue(a / b)
	case ql.TokenModulus: //    %
		// is this even valid?   modulus on floats?
		return NewNumberValue(float64(int64(a) % int64(b)))

	// Below here are Boolean Returns
	case ql.TokenEqualEqual: //  ==
		if a == b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenGT: //  >
		if a > b {
			//r = 1
			return BoolValueTrue
		} else {
			//r = 0
			return BoolValueFalse
		}
	case ql.TokenNE: //  !=    or <>
		if a != b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLT: // <
		if a < b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenGE: // >=
		if a >= b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLE: // <=
		if a <= b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLogicOr, ql.TokenOr: //  ||
		if a != 0 || b != 0 {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLogicAnd: //  &&
		if a != 0 && b != 0 {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	}
	panic(fmt.Errorf("expr: unknown operator %s", op))
}

func operateInts(op ql.Token, av, bv IntValue) Value {
	//if math.IsNaN(a) || math.IsNaN(b) {
	//	return math.NaN()
	//}
	a, b := av.v, bv.v
	switch op.T {
	case ql.TokenPlus: // +
		//r = a + b
		return NewIntValue(a + b)
	case ql.TokenStar, ql.TokenMultiply: // *
		//r = a * b
		return NewIntValue(a * b)
	case ql.TokenMinus: // -
		//r = a - b
		return NewIntValue(a - b)
	case ql.TokenDivide: //    /
		//r = a / b
		u.Debugf("divide:   %v / %v = %v", a, b, a/b)
		return NewIntValue(a / b)
	case ql.TokenModulus: //    %
		//r = a / b
		u.Debugf("modulus:   %v / %v = %v", a, b, a/b)
		return NewIntValue(a % b)

	// Below here are Boolean Returns
	case ql.TokenEqualEqual: //  ==
		if a == b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenGT: //  >
		if a > b {
			//r = 1
			return BoolValueTrue
		} else {
			//r = 0
			return BoolValueFalse
		}
	case ql.TokenNE: //  !=    or <>
		if a != b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLT: // <
		if a < b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenGE: // >=
		if a >= b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLE: // <=
		if a <= b {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLogicOr: //  ||
		if a != 0 || b != 0 {
			return BoolValueTrue
		} else {
			return BoolValueFalse
		}
	case ql.TokenLogicAnd: //  &&
		if a != 0 && b != 0 {
			return BoolValueTrue
		} else {
			return BoolValueFalse
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
func extractScalarXXX(v Value) interface{} {
	// if len(res.Results) == 1 && res.Results[0].Type() == TYPE_SCALAR {
	// 	return float64(res.Results[0].Value.Value().(Scalar))
	// }
	// return res
	return nil
}
