package exprvm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"
)

type Context struct{}

type state struct {
	*Expr
	now        time.Time
	context    Context
	unjoinedOk bool
}

var (
	ErrUnknownOp       = fmt.Errorf("expr: unknown op type")
	ErrUnknownNodeType = fmt.Errorf("expr: unknown node type")
)

type Expr struct {
	*Tree
}

func (e *Expr) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.String())
}

func New(expr string) (*Expr, error) {
	t, err := ParseTree(expr)
	if err != nil {
		return nil, err
	}
	e := &Expr{
		Tree: t,
	}
	return e, nil
}

// Execute applies a parse expression to the specified OpenTSDB context, and
// returns one result per group. T may be nil to ignore timings.
func (e *Expr) Execute(c Context) (r *Results, err error) {
	defer errRecover(&err)
	s := &state{
		Expr:    e,
		context: c,
		now:     time.Now(),
	}
	r = s.walk(e.Tree.Root)
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

func marshalFloat(n float64) ([]byte, error) {
	if math.IsNaN(n) {
		return json.Marshal("NaN")
	} else if math.IsInf(n, 1) {
		return json.Marshal("+Inf")
	} else if math.IsInf(n, -1) {
		return json.Marshal("-Inf")
	}
	return json.Marshal(n)
}

type Result struct {
	Computations
	Value
	Group interface{}
}

type Results struct {
	Results []*Result
	// If true, ungrouped joins from this set will be ignored.
	IgnoreUnjoined bool
	// If true, ungrouped joins from the other set will be ignored.
	IgnoreOtherUnjoined bool
	// If non nil, will set any NaN value to it.
	NaNValue *float64
}

func (r *Results) NaN() Number {
	if r.NaNValue != nil {
		return Number(*r.NaNValue)
	}
	return Number(math.NaN())
}

type Computations []Computation

type Computation struct {
	Text  string
	Value interface{}
}

func (r *Result) AddComputation(text string, value interface{}) {
	r.Computations = append(r.Computations, Computation{"TODO", value})
}

type Union struct {
	Computations
	A, B  Value
	Group interface{}
}

// wrap creates a new Result with a nil group and given value.
func wrap(v float64) *Results {
	panic("not implemented")
	return &Results{
		Results: []*Result{
			{
				Value: nil,
			},
		},
	}
}

func (u *Union) ExtendComputations(o *Result) {
	u.Computations = append(u.Computations, o.Computations...)
}

// union returns the combination of a and b where one is a subset of the other.
func (e *state) union(a, b *Results, expression string) []*Union {
	// const unjoinedGroup = "unjoined group (%v)"
	// var us []*Union
	// if len(a.Results) == 0 || len(b.Results) == 0 {
	// 	return us
	// }
	// am := make(map[*Result]bool)
	// bm := make(map[*Result]bool)
	// for _, ra := range a.Results {
	// 	am[ra] = true
	// }
	// for _, rb := range b.Results {
	// 	bm[rb] = true
	// }
	// for _, ra := range a.Results {
	// 	for _, rb := range b.Results {
	// 		u := &Union{
	// 			A: ra.Value,
	// 			B: rb.Value,
	// 		}
	// 		// Comented Out
	// 		// if ra.Group.Equal(rb.Group) || len(ra.Group) == 0 || len(rb.Group) == 0 {
	// 		// 	g := ra.Group
	// 		// 	if len(ra.Group) == 0 {
	// 		// 		g = rb.Group
	// 		// 	}
	// 		// 	u.Group = g
	// 		// } else if ra.Group.Subset(rb.Group) {
	// 		// 	u.Group = ra.Group
	// 		// } else if rb.Group.Subset(ra.Group) {
	// 		// 	u.Group = rb.Group
	// 		// } else {
	// 		// 	continue
	// 		// }
	// 		delete(am, ra)
	// 		delete(bm, rb)
	// 		u.ExtendComputations(ra)
	// 		u.ExtendComputations(rb)
	// 		us = append(us, u)
	// 	}
	// }
	// if !e.unjoinedOk {
	// 	if !a.IgnoreUnjoined && !b.IgnoreOtherUnjoined {
	// 		for r := range am {
	// 			u := &Union{
	// 				A:     r.Value,
	// 				B:     b.NaN(),
	// 				Group: r.Group,
	// 			}
	// 			r.AddComputation(expression, fmt.Sprintf(unjoinedGroup, u.B))
	// 			u.ExtendComputations(r)
	// 			us = append(us, u)
	// 		}
	// 	}
	// 	if !b.IgnoreUnjoined && !a.IgnoreOtherUnjoined {
	// 		for r := range bm {
	// 			u := &Union{
	// 				A:     a.NaN(),
	// 				B:     r.Value,
	// 				Group: r.Group,
	// 			}
	// 			r.AddComputation(expression, fmt.Sprintf(unjoinedGroup, u.A))
	// 			u.ExtendComputations(r)
	// 			us = append(us, u)
	// 		}
	// 	}
	// }
	//return us
	return nil
}

func (e *state) walk(node Node) *Results {
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

func (e *state) walkBinary(node *BinaryNode) *Results {
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

func (e *state) walkUnary(node *UnaryNode) *Results {
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

func (e *state) walkFunc(node *FuncNode) *Results {
	f := reflect.ValueOf(node.F.F)
	var in []reflect.Value
	for _, a := range node.Args {
		var v interface{}
		switch t := a.(type) {
		case *StringNode:
			v = t.Text
		case *NumberNode:
			v = t.Float64
		case *FuncNode:
			v = extractScalar(e.walkFunc(t))
		case *UnaryNode:
			v = extractScalar(e.walkUnary(t))
		case *BinaryNode:
			v = extractScalar(e.walkBinary(t))
		default:
			panic(fmt.Errorf("expr: unknown func arg type"))
		}
		in = append(in, reflect.ValueOf(v))
	}
	fr := f.Call(append([]reflect.Value{reflect.ValueOf(e)}, in...))
	res := fr[0].Interface().(*Results)
	if len(fr) > 1 && !fr[1].IsNil() {
		err := fr[1].Interface().(error)
		if err != nil {
			panic(err)
		}
	}
	if node.Return().Kind() == reflect.String {
		for _, r := range res.Results {
			r.AddComputation(node.String(), r.Value.(Number))
		}
	}
	return res
}

// extractScalar will return a float64 if res contains exactly one scalar.
func extractScalar(res *Results) interface{} {
	// if len(res.Results) == 1 && res.Results[0].Type() == TYPE_SCALAR {
	// 	return float64(res.Results[0].Value.Value().(Scalar))
	// }
	// return res
	return nil
}
