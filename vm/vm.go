package vm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/ast"
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

type ExprVm interface {
	Execute(writeContext ContextWriter, readContext ContextReader) error
}

type NoSchema struct {
}

func (m *NoSchema) Key() string { return "" }

// A node vm is a vm for parsing, evaluating a single tree-node
//
type Vm struct {
	*ast.Tree
}

func (m *Vm) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func NewVm(expr string) (*Vm, error) {
	t, err := ast.ParseExpression(expr)
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
		Reader: readContext,
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
func numberNodeToValue(t *ast.NumberNode) (v value.Value) {
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
	case lex.TokenEqualEqual: //  ==
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
