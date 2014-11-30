package exprvm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser/lex"
)

var (
	ErrUnknownOp       = fmt.Errorf("expr: unknown op type")
	ErrUnknownNodeType = fmt.Errorf("expr: unknown node type")
	_                  = u.EMPTY
)

type Context interface {
	Get(key string) Value
}

type ContextSimple struct {
	data map[string]Value
}

func (m ContextSimple) Get(key string) Value {
	return m.data[key]
}

type state struct {
	*Vm
	// We make a reflect value of self (state) as we use []reflect.ValueOf often
	rv      reflect.Value
	now     time.Time
	context Context
}

type Vm struct {
	*Tree
}

func (m *Vm) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func NewVm(expr string) (*Vm, error) {
	t, err := ParseTree(expr)
	if err != nil {
		return nil, err
	}
	m := &Vm{
		Tree: t,
	}
	return m, nil
}

// Execute applies a parse expression to the specified env context, and
// returns a Value Type
func (m *Vm) Execute(c Context) (v Value, err error) {
	//defer errRecover(&err)
	s := &state{
		Vm:      m,
		context: c,
		now:     time.Now(),
	}
	s.rv = reflect.ValueOf(s)
	u.Infof("tree.Root:  %#v", m.Tree.Root)
	v = s.walk(m.Tree.Root)
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
	//u.Infof("nodeToValue()  isFloat?%v", t.IsFloat)
	if t.IsInt {
		v = NewIntValue(t.Int64)
	} else if t.IsFloat {
		v = NewNumberValue(toFloat64(reflect.ValueOf(t.Text)))
	} else {
		u.Errorf("Could not find type? %v", t.Type())
	}
	//u.Infof("return nodeToValue()	%v  %T  arg:%T", v, v, t)
	return v
}

func (e *state) walk(arg ExprArg) Value {
	u.Infof("walk() node=%T  %v", arg, arg)
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
		panic(ErrUnknownNodeType)
	}
}

// func (e *state) walkArg(arg ExprArg) Value {
// 	u.Infof("walkArg() arg=%T  %v", arg, arg)
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

func (e *state) walkBinary(node *BinaryNode) Value {
	ar := e.walk(node.Args[0])
	br := e.walk(node.Args[1])
	u.Infof("walkBinary: %v  %v  %T  %T", node, ar, br, ar, br)
	switch at := ar.(type) {
	case IntValue:
		switch bt := br.(type) {
		case IntValue:
			n := operateInts(node.Operator, at, bt)
			return n
		case NumberValue:
			n := operateNumbers(node.Operator, at.NumberValue(), bt)
			return n
		default:
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
			panic(ErrUnknownOp)
		}
	default:
		u.Errorf("Unknown op?  %T  %T  %v", ar, at, ar)
		panic(ErrUnknownOp)
	}

	return nil
}

func (e *state) walkIdentity(node *IdentityNode) Value {
	u.Infof("walkIdentity() node=%T  %v", node, node)
	return e.context.Get(node.Text)
}

func (e *state) walkUnary(node *UnaryNode) Value {
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

func (e *state) walkFunc(node *FuncNode) Value {

	u.Infof("walk node --- %v   ", node.StringAST())

	//f := reflect.ValueOf(node.F.F)
	funcArgs := []reflect.Value{e.rv}
	for _, a := range node.Args {

		u.Infof("arg %v  %T %v", a, a, a.Type().Kind())

		var v interface{}
		switch t := a.(type) {
		case *StringNode:
			v = t.Text
		case *IdentityNode:
			v = e.context.Get(t.Text)
		case *NumberNode:
			v = nodeToValue(t)
		case *FuncNode:
			u.Infof("descending to %v()", t.Name)
			v = e.walkFunc(t)
			u.Infof("result of %v() = %v, %T", t.Name, v, v)
			//v = extractScalar()
		case *UnaryNode:
			v = extractScalar(e.walkUnary(t))
		case *BinaryNode:
			v = extractScalar(e.walkBinary(t))
		default:
			panic(fmt.Errorf("expr: unknown func arg type"))
		}
		u.Infof("%v  %T  arg:%T", v, v, a)
		funcArgs = append(funcArgs, reflect.ValueOf(v))
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
	// if node.Type().Kind() == reflect.String {
	// 	for _, r := range res.Results {
	// 		r.AddComputation(node.String(), r.Value.(Number))
	// 	}
	// }
	return res
}

func operateNumbers(op ql.Token, av, bv NumberValue) Value {
	if math.IsNaN(av.v) || math.IsNaN(bv.v) {
		return NewNumberValue(math.NaN())
	}
	//
	a, b := av.v, bv.v
	switch op.T {
	case ql.TokenPlus: // +
		return NewNumberValue(a + b)
	case ql.TokenStar: // *
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

func operateInts(op ql.Token, av, bv IntValue) Value {
	//if math.IsNaN(a) || math.IsNaN(b) {
	//	return math.NaN()
	//}
	a, b := av.v, bv.v
	switch op.T {
	case ql.TokenPlus: // +
		//r = a + b
		return NewIntValue(a + b)
	case ql.TokenStar: // *
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
func extractScalar(v Value) interface{} {
	// if len(res.Results) == 1 && res.Results[0].Type() == TYPE_SCALAR {
	// 	return float64(res.Results[0].Value.Value().(Scalar))
	// }
	// return res
	return nil
}
