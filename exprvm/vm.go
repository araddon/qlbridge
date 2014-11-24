package exprvm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"

	u "github.com/araddon/gou"
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

// wrap creates a new Value with a nil group and given value.
func wrap(v float64) Value {
	panic("not implemented")
}

func (e *state) walk(node Node) Value {
	u.Infof("walk type=%T", node)
	switch node := node.(type) {
	case *NumberNode:
		return wrap(node.Float64)
	case *BinaryNode:
		return e.walkBinary(node)
	case *UnaryNode:
		return e.walkUnary(node)
	case *FuncNode:
		return e.walkFunc(node)
	default:
		panic(ErrUnknownNodeType)
	}
}

func (e *state) walkBinary(node *BinaryNode) Value {
	// ar := e.walk(node.Args[0])
	// br := e.walk(node.Args[1])
	// res := Results{
	// 	IgnoreUnjoined:      ar.IgnoreUnjoined || br.IgnoreUnjoined,
	// 	IgnoreOtherUnjoined: ar.IgnoreOtherUnjoined || br.IgnoreOtherUnjoined,
	// }
	// u := e.union(ar, br, node.String())
	// for _, v := range u {
	// 	var value Value
	// 	r := Result{
	// 		Group:        v.Group,
	// 		Computations: v.Computations,
	// 	}
	// 	an, aok := v.A.(Scalar)
	// 	bn, bok := v.B.(Scalar)
	// 	if (aok && math.IsNaN(float64(an))) || (bok && math.IsNaN(float64(bn))) {
	// 		value = Scalar(math.NaN())
	// 	} else {
	// 		switch at := v.A.(type) {
	// 		case Scalar:
	// 			switch bt := v.B.(type) {
	// 			case Scalar:
	// 				n := Scalar(operate(node.OpStr, float64(at), float64(bt)))
	// 				r.AddComputation(node.String(), Number(n))
	// 				value = n
	// 			case Number:
	// 				n := Number(operate(node.OpStr, float64(at), float64(bt)))
	// 				r.AddComputation(node.String(), n)
	// 				value = n
	// 			default:
	// 				panic(ErrUnknownOp)
	// 			}
	// 		case Number:
	// 			switch bt := v.B.(type) {
	// 			case Scalar:
	// 				n := Number(operate(node.OpStr, float64(at), float64(bt)))
	// 				r.AddComputation(node.String(), Number(n))
	// 				value = n
	// 			case Number:
	// 				n := Number(operate(node.OpStr, float64(at), float64(bt)))
	// 				r.AddComputation(node.String(), n)
	// 				value = n
	// 			default:
	// 				panic(ErrUnknownOp)
	// 			}
	// 		default:
	// 			panic(ErrUnknownOp)
	// 		}
	// 	}
	// 	r.Value = value
	// 	res.Results = append(res.Results, &r)
	// }
	// return &res
	return nil
}

func operate(op string, a, b float64) (r float64) {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.NaN()
	}
	switch op {
	case "+":
		r = a + b
	case "*":
		r = a * b
	case "-":
		r = a - b
	case "/":
		r = a / b
	case "==":
		if a == b {
			r = 1
		} else {
			r = 0
		}
	case ">":
		if a > b {
			r = 1
		} else {
			r = 0
		}
	case "!=":
		if a != b {
			r = 1
		} else {
			r = 0
		}
	case "<":
		if a < b {
			r = 1
		} else {
			r = 0
		}
	case ">=":
		if a >= b {
			r = 1
		} else {
			r = 0
		}
	case "<=":
		if a <= b {
			r = 1
		} else {
			r = 0
		}
	case "||":
		if a != 0 || b != 0 {
			r = 1
		} else {
			r = 0
		}
	case "&&":
		if a != 0 && b != 0 {
			r = 1
		} else {
			r = 0
		}
	default:
		panic(fmt.Errorf("expr: unknown operator %s", op))
	}
	return
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
			v = t.Float64
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

// extractScalar will return a float64 if res contains exactly one scalar.
func extractScalar(v Value) interface{} {
	// if len(res.Results) == 1 && res.Results[0].Type() == TYPE_SCALAR {
	// 	return float64(res.Results[0].Value.Value().(Scalar))
	// }
	// return res
	return nil
}
