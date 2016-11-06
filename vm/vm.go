// VM implements the virtual machine runtime/evaluator
// for the SQL, FilterQL, and Expression evalutors.
package vm

import (
	"encoding/json"
	"fmt"
	"strconv"

	"math"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/lytics/datemath"
	"github.com/mb0/glob"

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
	defer errRecover(&err)
	s := &State{
		ExprVm:        m,
		ContextReader: readContext,
	}
	s.rv = reflect.ValueOf(s)
	//u.Debugf("vm.Execute:  %#v", m.Tree.Root)
	v, ok := s.Walk(m.Tree.Root)
	//u.Infof("v:%v  ok?%v for %s", v, ok, m.String())

	// vm unable to walk tree
	if !ok {
		return ErrExecute
	}

	// vm returned an error value
	if errv, ok := v.(value.ErrorValue); ok {
		return errv
	}

	// Special Vm that doesnt' have named fields, single tree expression
	//u.Debugf("vm.Walk val:  %v", v)
	writeContext.Put(SchemaInfoEmpty, readContext, v)
	return nil
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
func numberNodeToValue(t *expr.NumberNode) (value.Value, bool) {
	//u.Debugf("nodeToValue()  isFloat?%v", t.IsFloat)
	var v value.Value
	if t.IsInt {
		v = value.NewIntValue(t.Int64)
	} else if t.IsFloat {
		fv, ok := value.ToFloat64(reflect.ValueOf(t.Text))
		if !ok {
			u.Warnf("Could not perform numeric conversion for %q", t.Text)
			return value.NilValueVal, false
		}
		v = value.NewNumberValue(fv)
	} else {
		u.Warnf("Could not find numeric conversion for %v", t.Type())
		return value.NilValueVal, false
	}
	//u.Debugf("return nodeToValue()	%v  %T  arg:%T", v, v, t)
	return v, true
}

func Evaluator(arg expr.Node) EvaluatorFunc {
	//u.Debugf("Evaluator() node=%T  %v", arg, arg)
	switch argVal := arg.(type) {
	case *expr.NumberNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return numberNodeToValue(argVal) }
	case *expr.BinaryNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkBinary(ctx, argVal) }
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
	case *expr.ArrayNode:
		return func(ctx expr.EvalContext) (value.Value, bool) { return walkArray(ctx, argVal) }
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
		return numberNodeToValue(argVal)
	case *expr.BinaryNode:
		return walkBinary(ctx, argVal)
	case *expr.UnaryNode:
		return walkUnary(ctx, argVal)
	case *expr.TriNode:
		return walkTri(ctx, argVal)
	case *expr.ArrayNode:
		return walkArray(ctx, argVal)
	case *expr.FuncNode:
		return walkFunc(ctx, argVal)
	case *expr.IdentityNode:
		return walkIdentity(ctx, argVal)
	case *expr.StringNode:
		return value.NewStringValue(argVal.Text), true
	case nil:
		return nil, false
	case *expr.NullNode:
		// WHERE (`users.user_id` != NULL)
		return value.NewNilValue(), true
	case *expr.ValueNode:
		if argVal.Value == nil {
			return nil, false
		}
		switch val := argVal.Value.(type) {
		case *value.NilValue, value.NilValue:
			return nil, false
		case value.SliceValue:
			//u.Warnf("got slice? %#v", argVal)
			return val, true
		}
		u.Errorf("Unknonwn node type:  %#v", argVal.Value)
		panic(ErrUnknownNodeType)
	default:
		u.Errorf("Unknonwn node type:  %#v", arg)
		panic(ErrUnknownNodeType)
	}
}

func (e *State) Walk(arg expr.Node) (value.Value, bool) {
	return Eval(e.ContextReader, arg)
}

// Binary operands:   =, ==, !=, OR, AND, >, <, >=, <=, LIKE, contains
//
//       x == y,   x = y
//       x != y
//       x OR y
//       x > y
//       x < =
//
func walkBinary(ctx expr.EvalContext, node *expr.BinaryNode) (value.Value, bool) {
	ar, aok := Eval(ctx, node.Args[0])
	br, bok := Eval(ctx, node.Args[1])

	//u.Debugf("walkBinary: aok?%v ar:%v %T  node=%s %T", aok, ar, ar, node.Args[0], node.Args[0])
	//u.Debugf("walkBinary: bok?%v br:%v %T  node=%s %T", bok, br, br, node.Args[1], node.Args[1])
	//u.Debugf("walkBinary: l:%v  r:%v  %T  %T node=%s", ar, br, ar, br, node)
	// If we could not evaluate either we can shortcut
	if !aok && !bok {
		switch node.Operator.T {
		case lex.TokenLogicOr, lex.TokenOr:
			return value.NewBoolValue(false), true
		case lex.TokenEqualEqual, lex.TokenEqual:
			// We don't alllow nil == nil here bc we have a NilValue type
			// that we would use for that
			return value.NewBoolValue(false), true
		case lex.TokenNE:
			return value.NewBoolValue(false), true
		case lex.TokenGT, lex.TokenGE, lex.TokenLT, lex.TokenLE, lex.TokenLike:
			return value.NewBoolValue(false), true
		}
		//u.Debugf("walkBinary not ok: op=%s %v  l:%v  r:%v  %T  %T", node.Operator, node, ar, br, ar, br)
		return nil, false
	}

	// Else if we can only evaluate one, we can short circuit as well
	if !aok || !bok {
		switch node.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd:
			return value.NewBoolValue(false), true
		case lex.TokenEqualEqual, lex.TokenEqual:
			return value.NewBoolValue(false), true
		case lex.TokenNE:
			// they are technically not equal?
			return value.NewBoolValue(true), true
		case lex.TokenGT, lex.TokenGE, lex.TokenLT, lex.TokenLE, lex.TokenLike:
			return value.NewBoolValue(false), true
		}
		//u.Debugf("walkBinary not ok: op=%s %v  l:%v  r:%v  %T  %T", node.Operator, node, ar, br, ar, br)
		// need to fall through to below
	}

	switch at := ar.(type) {
	case value.IntValue:
		switch bt := br.(type) {
		case value.IntValue:
			//u.Debugf("doing operate ints  %v %v  %v", at, node.Operator.V, bt)
			n := operateInts(node.Operator, at, bt)
			return n, true
		case value.StringValue:
			//u.Debugf("doing operatation int+string  %v %v  %v", at, node.Operator.V, bt)
			// Try int first
			bi, err := strconv.ParseInt(bt.Val(), 10, 64)
			if err == nil {
				n, err := operateIntVals(node.Operator, at.Val(), bi)
				if err != nil {
					return nil, false
				}
				return n, true
			}
			// Fallback to float
			bf, err := strconv.ParseFloat(bt.Val(), 64)
			if err == nil {
				n := operateNumbers(node.Operator, at.NumberValue(), value.NewNumberValue(bf))
				return n, true
			}
		case value.NumberValue:
			//u.Debugf("doing operate ints/numbers  %v %v  %v", at, node.Operator.V, bt)
			n := operateNumbers(node.Operator, at.NumberValue(), bt)
			return n, true
		case value.SliceValue:
			switch node.Operator.T {
			case lex.TokenIN:
				for _, val := range bt.Val() {
					switch valt := val.(type) {
					case value.StringValue:
						if at.Val() == valt.IntValue().Val() {
							return value.BoolValueTrue, true
						}
					case value.IntValue:
						if at.Val() == valt.Val() {
							return value.BoolValueTrue, true
						}
					case value.NumberValue:
						if at.Val() == valt.Int() {
							return value.BoolValueTrue, true
						}
					default:
						u.Debugf("Could not coerce to number: T:%T  v:%v", val, val)
					}
				}
				return value.NewBoolValue(false), true
			default:
				u.Debugf("unsupported op for SliceValue op:%v rhT:%T", node.Operator, br)
				return nil, false
			}
		case nil, value.NilValue:
			return nil, false
		default:
			u.Errorf("unknown type:  %T %v", bt, bt)
		}
	case value.NumberValue:
		switch bt := br.(type) {
		case value.IntValue:
			n := operateNumbers(node.Operator, at, bt.NumberValue())
			return n, true
		case value.NumberValue:
			n := operateNumbers(node.Operator, at, bt)
			return n, true
		case value.SliceValue:
			for _, val := range bt.Val() {
				switch valt := val.(type) {
				case value.StringValue:
					if at.Val() == valt.NumberValue().Val() {
						return value.BoolValueTrue, true
					}
				case value.IntValue:
					if at.Val() == valt.NumberValue().Val() {
						return value.BoolValueTrue, true
					}
				case value.NumberValue:
					if at.Val() == valt.Val() {
						return value.BoolValueTrue, true
					}
				default:
					u.Debugf("Could not coerce to number: T:%T  v:%v", val, val)
				}
			}
			return value.BoolValueFalse, true
		case value.StringValue:
			//u.Debugf("doing operatation num+string  %v %v  %v", at, node.Operator.V, bt)
			// Try int first
			if bf, err := strconv.ParseInt(bt.Val(), 10, 64); err == nil {
				n := operateNumbers(node.Operator, at, value.NewNumberValue(float64(bf)))
				return n, true
			}
			// Fallback to float
			if bf, err := strconv.ParseFloat(bt.Val(), 64); err == nil {
				n := operateNumbers(node.Operator, at, value.NewNumberValue(bf))
				return n, true
			}
		case nil, value.NilValue:
			return nil, false
		default:
			u.Errorf("unknown type:  %T %v", bt, bt)
		}
	case value.BoolValue:
		switch bt := br.(type) {
		case value.BoolValue:
			atv, btv := at.Value().(bool), bt.Value().(bool)
			switch node.Operator.T {
			case lex.TokenLogicAnd, lex.TokenAnd:
				return value.NewBoolValue(atv && btv), true
			case lex.TokenLogicOr, lex.TokenOr:
				return value.NewBoolValue(atv || btv), true
			case lex.TokenEqualEqual, lex.TokenEqual:
				return value.NewBoolValue(atv == btv), true
			case lex.TokenNE:
				return value.NewBoolValue(atv != btv), true
			default:
				u.Warnf("bool binary?:  %#v  %v %v", node, at, bt)
			}
		case nil, value.NilValue:
			switch node.Operator.T {
			case lex.TokenLogicAnd:
				return value.NewBoolValue(false), true
			case lex.TokenLogicOr, lex.TokenOr:
				return at, true
			case lex.TokenEqualEqual, lex.TokenEqual:
				return value.NewBoolValue(false), true
			case lex.TokenNE:
				return value.NewBoolValue(true), true
			// case lex.TokenGE, lex.TokenGT, lex.TokenLE, lex.TokenLT:
			// 	return value.NewBoolValue(false), true
			default:
				u.Warnf("right side nil binary:  %q", node)
				return nil, false
			}
		default:
			//u.Warnf("br: %#v", br)
			//u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), bt, bt.Value())
			return nil, false
		}
	case value.StringValue:
		switch bt := br.(type) {
		case value.StringValue:
			// Nice, both strings
			return operateStrings(node.Operator, at, bt), true
		case nil, value.NilValue:
			switch node.Operator.T {
			case lex.TokenEqualEqual, lex.TokenEqual:
				if at.Nil() {
					return value.NewBoolValue(true), true
				}
				return value.NewBoolValue(false), true
			case lex.TokenNE:
				if at.Nil() {
					return value.NewBoolValue(false), true
				}
				return value.NewBoolValue(true), true
			default:
				u.Debugf("unsupported op: %v", node.Operator)
				return nil, false
			}
		case value.SliceValue:
			switch node.Operator.T {
			case lex.TokenIN:
				for _, val := range bt.Val() {
					if at.Val() == val.ToString() {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true
			default:
				u.Debugf("unsupported op for SliceValue op:%v rhT:%T", node.Operator, br)
				return nil, false
			}
		case value.StringsValue:
			switch node.Operator.T {
			case lex.TokenIN:
				for _, val := range bt.Val() {
					if at.Val() == val {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true
			default:
				u.Debugf("unsupported op for Strings op:%v rhT:%T", node.Operator, br)
				return nil, false
			}
		case value.BoolValue:
			if value.IsBool(at.Val()) {
				//u.Warnf("bool eval:  %v %v %v  :: %v", value.BoolStringVal(at.Val()), node.Operator.T.String(), bt.Val(), value.NewBoolValue(value.BoolStringVal(at.Val()) == bt.Val()))
				switch node.Operator.T {
				case lex.TokenEqualEqual, lex.TokenEqual:
					return value.NewBoolValue(value.BoolStringVal(at.Val()) == bt.Val()), true
				case lex.TokenNE:
					return value.NewBoolValue(value.BoolStringVal(at.Val()) != bt.Val()), true
				default:
					u.Debugf("unsupported op: %v", node.Operator)
					return nil, false
				}
			} else {
				// Should we evaluate strings that are non-nil to be = true?
				u.Debugf("not handled: boolean %v %T=%v  expr: %s", node.Operator, at.Value(), at.Val(), node.String())
				return nil, false
			}
		case value.Map:
			switch node.Operator.T {
			case lex.TokenIN:
				_, hasKey := bt.Get(at.Val())
				if hasKey {
					return value.NewBoolValue(true), true
				}
				return value.NewBoolValue(false), true
			default:
				u.Debugf("unsupported op for Map op:%v rhT:%T", node.Operator, br)
				return nil, false
			}
		default:
			// TODO:  this doesn't make sense, we should be able to operate on other types
			if at.CanCoerce(int64Rv) {
				switch bt := br.(type) {
				case value.StringValue:
					n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
					return n, true
				case value.IntValue:
					n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
					return n, true
				case value.NumberValue:
					n := operateNumbers(node.Operator, at.NumberValue(), bt)
					return n, true
				default:
					u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), bt, bt.Value())
				}
			} else {
				u.Errorf("at?%T  %v  coerce?%v bt? %T     %v", at, at.Value(), at.CanCoerce(stringRv), br, br)
			}
		}
	case value.SliceValue:
		switch node.Operator.T {
		case lex.TokenContains:
			switch bval := br.(type) {
			case nil, value.NilValue:
				return nil, false
			case value.StringValue:
				// [x,y,z] contains str
				for _, val := range at.Val() {
					if strings.Contains(val.ToString(), bval.Val()) {
						return value.BoolValueTrue, true
					}
				}
				return value.BoolValueFalse, true
			case value.IntValue:
				// [] contains int
				for _, val := range at.Val() {
					//u.Infof("int contains? %v %v", val.Value(), br.Value())
					if eq, _ := value.Equal(val, br); eq {
						return value.BoolValueTrue, true
					}
				}
				return value.BoolValueFalse, true
			}
		case lex.TokenLike:
			switch bv := br.(type) {
			case value.StringValue:
				// [x,y,z] LIKE str
				for _, val := range at.Val() {
					if boolVal, ok := LikeCompare(val.ToString(), bv.Val()); ok && boolVal.Val() == true {
						return boolVal, true
					}
				}
				return value.BoolValueFalse, true
			default:
				//u.Warnf("un handled right side to Like  T:%T  %v", br, br)
			}
		case lex.TokenIntersects:
			switch bt := br.(type) {
			case nil, value.NilValue:
				return nil, false
			case value.SliceValue:
				for _, aval := range at.Val() {
					for _, bval := range bt.Val() {
						if eq, _ := value.Equal(aval, bval); eq {
							return value.BoolValueTrue, true
						}
					}
				}
				return value.BoolValueFalse, true
			case value.StringsValue:
				for _, aval := range at.Val() {
					for _, bstr := range bt.Val() {
						if aval.ToString() == bstr {
							return value.BoolValueTrue, true
						}
					}
				}
				return value.BoolValueFalse, true
			}
		}
		return nil, false
	case value.StringsValue:
		switch node.Operator.T {
		case lex.TokenContains:
			switch bv := br.(type) {
			case value.StringValue:
				// [x,y,z] contains str
				for _, val := range at.Val() {
					//u.Infof("str contains? %v %v", val, bv.Val())
					if strings.Contains(val, bv.Val()) {
						return value.BoolValueTrue, true
					}
				}
				return value.BoolValueFalse, true
			}
		case lex.TokenLike:

			switch bv := br.(type) {
			case value.StringValue:
				// [x,y,z] LIKE str
				for _, val := range at.Val() {
					boolVal, ok := LikeCompare(val, bv.Val())
					//u.Debugf("%s like %s ?? ok?%v  result=%v", val, bv.Val(), ok, boolVal)
					if ok && boolVal.Val() == true {
						return boolVal, true
					}
				}
				return value.BoolValueFalse, true
			}
		case lex.TokenIntersects:
			switch bt := br.(type) {
			case nil, value.NilValue:
				return nil, false
			case value.SliceValue:
				for _, astr := range at.Val() {
					for _, bval := range bt.Val() {
						if astr == bval.ToString() {
							return value.BoolValueTrue, true
						}
					}
				}
				return value.BoolValueFalse, true
			case value.StringsValue:
				for _, astr := range at.Val() {
					for _, bstr := range bt.Val() {
						if astr == bstr {
							return value.BoolValueTrue, true
						}
					}
				}
				return value.BoolValueFalse, true
			}
		}
		return nil, false
	case value.TimeValue:
		rht := time.Time{}
		lht := at.Val()
		var err error
		switch bv := br.(type) {
		case value.TimeValue:
			rht = bv.Val()
		case value.StringValue:
			te := bv.Val()
			if len(te) > 3 && strings.ToLower(te[:3]) == "now" {
				// Is date math
				rht, err = datemath.Eval(te[3:])
			} else {
				rht, err = dateparse.ParseAny(te)
			}
			if err != nil {
				u.Warnf("error? %s err=%v", te, err)
				return value.BoolValueFalse, false
			}
		case value.IntValue:
			// really?  we are going to try ints?
			rht, err = dateparse.ParseAny(bv.ToString())
			if err != nil {
				return value.BoolValueFalse, false
			}
			if rht.Year() < 1800 || rht.Year() > 2300 {
				return value.BoolValueFalse, false
			}
		default:
			//u.Warnf("un-handled? %#v", bv)
		}
		// if rht.IsZero() {
		// 	return nil, false
		// }
		switch node.Operator.T {
		case lex.TokenEqual, lex.TokenEqualEqual:
			if lht.Unix() == rht.Unix() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		case lex.TokenNE:
			if lht.Unix() != rht.Unix() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		case lex.TokenGT:
			// lhexpr > rhexpr
			if lht.Unix() > rht.Unix() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		case lex.TokenGE:
			// lhexpr >= rhexpr
			if lht.Unix() >= rht.Unix() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		case lex.TokenLT:
			// lhexpr < rhexpr
			if lht.Unix() < rht.Unix() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		case lex.TokenLE:
			// lhexpr <= rhexpr
			if lht.Unix() <= rht.Unix() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		default:
			u.Warnf("unhandled date op %v", node.Operator)
		}
		return nil, false
	case value.Map:
		rhvals := make([]string, 0)
		switch bv := br.(type) {
		case value.StringsValue:
			rhvals = bv.Val()
		case value.Slice:
			for _, arg := range bv.SliceValue() {
				rhvals = append(rhvals, arg.ToString())
			}
		default:
			u.Debugf("un-handled? %T", bv)
			return nil, false
		}

		switch node.Operator.T {
		case lex.TokenIN, lex.TokenIntersects:
			for _, val := range rhvals {
				if _, ok := at.Get(val); ok {
					return value.NewBoolValue(true), true
				}
			}
			return value.NewBoolValue(false), true
		}
		return nil, false
	case nil, value.NilValue:
		switch node.Operator.T {
		case lex.TokenLogicAnd:
			return value.NewBoolValue(false), true
		case lex.TokenLogicOr, lex.TokenOr:
			switch bt := br.(type) {
			case value.BoolValue:
				return bt, true
			default:
				return value.NewBoolValue(false), true
			}
		case lex.TokenEqualEqual, lex.TokenEqual:
			// does nil==nil  = true ??
			switch br.(type) {
			case nil, value.NilValue:
				return value.NewBoolValue(true), true
			default:
				return value.NewBoolValue(false), true
			}
		case lex.TokenNE:
			return value.NewBoolValue(true), true
		// case lex.TokenGE, lex.TokenGT, lex.TokenLE, lex.TokenLT:
		// 	return value.NewBoolValue(false), true
		case lex.TokenContains, lex.TokenLike, lex.TokenIN:
			return value.NewBoolValue(false), true
		default:
			//u.Debugf("left side nil binary:  %q", node)
			return nil, false
		}
	default:
		u.Debugf("Unknown op?  %T  %T  %v", ar, at, ar)
		return value.NewErrorValue(fmt.Sprintf("unsupported left side value: %T in %s", at, node)), false
	}

	return value.NewErrorValue(fmt.Sprintf("unsupported binary expression: %s", node)), false
}

func walkIdentity(ctx expr.EvalContext, node *expr.IdentityNode) (value.Value, bool) {

	if node.IsBooleanIdentity() {
		//u.Debugf("walkIdentity() boolean: node=%T  %v Bool:%v", node, node, node.Bool())
		return value.NewBoolValue(node.Bool()), true
	}
	if ctx == nil {
		return value.NewStringValue(node.Text), true
	}
	if node.HasLeftRight() {
		return ctx.Get(node.OriginalText())
	}
	return ctx.Get(node.Text)
}

func walkUnary(ctx expr.EvalContext, node *expr.UnaryNode) (value.Value, bool) {

	a, ok := Eval(ctx, node.Arg)
	if !ok {
		switch node.Operator.T {
		case lex.TokenExists:
			return value.NewBoolValue(false), true
		case lex.TokenNegate:
			return value.NewBoolValue(true), true
		}
		u.Debugf("unary could not evaluate for[ %s ] and %#v", node.String(), node)
		return a, false
	}

	switch node.Operator.T {
	case lex.TokenNegate:
		switch argVal := a.(type) {
		case value.BoolValue:
			//u.Debugf("found unary bool:  res=%v   expr=%v", !argVal.Val(), node)
			return value.NewBoolValue(!argVal.Val()), true
		case nil, value.NilValue:
			return value.NewBoolValue(false), false
		default:
			u.LogThrottle(u.WARN, 5, "unary type not implemented. Unknonwn node type: %T:%v node=%s", argVal, argVal, node.String())
			return value.NewNilValue(), false
		}
	case lex.TokenMinus:
		if an, aok := a.(value.NumericValue); aok {
			return value.NewNumberValue(-an.Float()), true
		}
	case lex.TokenExists:
		switch a.(type) {
		case nil, value.NilValue:
			return value.NewBoolValue(false), true
		}
		if a.Nil() {
			return value.NewBoolValue(false), true
		}
		return value.NewBoolValue(true), true
	default:
		u.Warnf("urnary not implemented for type %s %#v", node.Operator.T.String(), node)
	}

	return value.NewNilValue(), false
}

// ternary evaluator
//
//     A   BETWEEN   B  AND C
//
func walkTri(ctx expr.EvalContext, node *expr.TriNode) (value.Value, bool) {

	a, aok := Eval(ctx, node.Args[0])
	b, bok := Eval(ctx, node.Args[1])
	c, cok := Eval(ctx, node.Args[2])
	//u.Infof("tri:  %T:%v  %v  %T:%v   %T:%v", a, a, node.Operator, b, b, c, c)
	if !aok {
		return value.BoolValueFalse, false
	}
	if !bok || !cok {
		u.Debugf("Could not evaluate args, %#v", node.String())
		return value.BoolValueFalse, false
	}
	if a == nil || b == nil || c == nil {
		return value.BoolValueFalse, false
	}
	switch node.Operator.T {
	case lex.TokenBetween:
		switch at := a.(type) {
		case value.IntValue:

			av := at.Val()
			bv, ok := value.ValueToInt64(b)
			if !ok {
				return value.BoolValueFalse, false
			}
			cv, ok := value.ValueToInt64(c)
			if !ok {
				return value.BoolValueFalse, false
			}
			if av > bv && av < cv {
				return value.NewBoolValue(true), true
			}

			return value.NewBoolValue(false), true
		case value.NumberValue:

			av := at.Val()
			bv, ok := value.ValueToFloat64(b)
			if !ok {
				return value.BoolValueFalse, false
			}
			cv, ok := value.ValueToFloat64(c)
			if !ok {
				return value.BoolValueFalse, false
			}
			if av > bv && av < cv {
				return value.NewBoolValue(true), true
			}

			return value.NewBoolValue(false), true

		case value.TimeValue:

			av := at.Val()
			bv, ok := value.ValueToTime(b)
			if !ok {
				return value.BoolValueFalse, false
			}
			cv, ok := value.ValueToTime(c)
			if !ok {
				return value.BoolValueFalse, false
			}
			if av.Unix() > bv.Unix() && av.Unix() < cv.Unix() {
				return value.NewBoolValue(true), true
			}

			return value.NewBoolValue(false), true

		default:
			u.Warnf("between not implemented for type %s %#v", a.Type().String(), node)
		}
	default:
		u.Warnf("ternary node walk not implemented:   %#v", node)
	}

	return value.NewNilValue(), false
}

// Array evaluator:  evaluate multiple values into an array
//
//     (b,c,d)
//
func walkArray(ctx expr.EvalContext, node *expr.ArrayNode) (value.Value, bool) {

	vals := make([]value.Value, len(node.Args))

	for i := 0; i < len(node.Args); i++ {
		v, _ := Eval(ctx, node.Args[i])
		vals[i] = v
	}

	// we are returning an array of evaluated nodes
	return value.NewSliceValues(vals), true
}

func walkFunc(ctx expr.EvalContext, node *expr.FuncNode) (value.Value, bool) {

	//u.Debugf("walkFunc node: %v", node.String())

	// we create a set of arguments to pass to the function, first arg
	// is this Context
	var ok bool
	funcArgs := make([]reflect.Value, 0)
	if ctx != nil {
		funcArgs = append(funcArgs, reflect.ValueOf(ctx))
	} else {
		var nilArg expr.EvalContext
		funcArgs = append(funcArgs, reflect.ValueOf(&nilArg).Elem())
	}
	for _, a := range node.Args {

		//u.Debugf("arg %v  %T %v", a, a, a)

		var v interface{}

		switch t := a.(type) {
		case *expr.StringNode: // String Literal
			v = value.NewStringValue(t.Text)
		case *expr.IdentityNode: // Identity node = lookup in context

			if t.IsBooleanIdentity() {
				v = value.NewBoolValue(t.Bool())
			} else {
				v, ok = ctx.Get(t.Text)
				//u.Infof("%#v", ctx)
				//u.Debugf("get '%s'? %T %v %v", t.String(), v, v, ok)
				if !ok {
					// nil arguments are valid
					v = value.NewNilValue()
				}
			}

		case *expr.NumberNode:
			v, ok = numberNodeToValue(t)
		case *expr.FuncNode:
			//u.Debugf("descending to %v()", t.Name)
			v, ok = walkFunc(ctx, t)
			if !ok {
				// nil arguments are valid
				v = value.NewNilValue()
			}
			//u.Debugf("result of %v() = %v, %T", t.Name, v, v)
		case *expr.UnaryNode:
			v, ok = walkUnary(ctx, t)
			if !ok {
				// nil arguments are valid ??
				v = value.NewNilValue()
			}
		case *expr.BinaryNode:
			v, ok = walkBinary(ctx, t)
		case *expr.ValueNode:
			v = t.Value
		default:
			u.Errorf("expr: unknown func arg type %T %v", a, a)
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
				u.Warnf("un-handled type:  %v  %T", v, v)
			}

			funcArgs = append(funcArgs, reflect.ValueOf(v))
		} else {
			//u.Debugf(`found func arg:  "%v"  %T  arg:%T`, v, v, a)
			funcArgs = append(funcArgs, reflect.ValueOf(v))
		}

	}
	// Get the result of calling our Function (Value,bool)
	//u.Debugf("Calling func:%v(%v) %v", node.F.Name, funcArgs, node.F.F)
	if node.Missing {
		u.LogThrottle(u.WARN, 10, "missing function %s", node.F.Name)
		return nil, false
	}
	fnRet := node.F.F.Call(funcArgs)
	//u.Debugf("fnRet: %v    ok?%v", fnRet, fnRet[1].Bool())
	// check if has an error response?
	if len(fnRet) > 1 && !fnRet[1].Bool() {
		// What do we do if not ok?
		return nil, false
	}

	vv, ok := fnRet[0].Interface().(value.Value)
	if !ok {
		return nil, true
	}
	return vv, true
}

func operateNumbers(op lex.Token, av, bv value.NumberValue) value.Value {
	switch op.T {
	case lex.TokenPlus, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide, lex.TokenMinus,
		lex.TokenModulus:
		if math.IsNaN(av.Val()) || math.IsNaN(bv.Val()) {
			return value.NewNumberValue(math.NaN())
		}
	}

	//
	a, b := av.Val(), bv.Val()
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

func operateStrings(op lex.Token, av, bv value.StringValue) value.Value {

	//  Any other ops besides =, ==, !=, contains, like?
	a, b := av.Val(), bv.Val()
	switch op.T {
	case lex.TokenEqualEqual, lex.TokenEqual: //  ==
		//u.Infof("==?  %v  %v", av, bv)
		if a == b {
			return value.BoolValueTrue
		}
		return value.BoolValueFalse

	case lex.TokenNE: //  !=
		//u.Infof("!=?  %v  %v", av, bv)
		if a == b {
			return value.BoolValueFalse
		}
		return value.BoolValueTrue
	case lex.TokenContains:
		if strings.Contains(a, b) {
			return value.BoolValueTrue
		}
		return value.BoolValueFalse
	case lex.TokenLike: // a(value) LIKE b(pattern)
		bv, ok := LikeCompare(a, b)
		if !ok {
			return value.NewErrorValuef("invalid LIKE pattern: %q", a)
		}
		return bv
	case lex.TokenIN:
		if a == b {
			return value.BoolValueTrue
		}
		return value.BoolValueFalse
	}
	return value.NewErrorValuef("unsupported operator for strings: %s", op.T)
}

func LikeCompare(a, b string) (value.BoolValue, bool) {
	// Do we want to always do this replacement?   Or do this at parse time or config?
	if strings.Contains(b, "%") {
		b = strings.Replace(b, "%", "*", -1)
	}
	match, err := glob.Match(b, a)
	if err != nil {
		return value.BoolValueFalse, false
	}
	if match {
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}
func operateInts(op lex.Token, av, bv value.IntValue) value.Value {
	a, b := av.Val(), bv.Val()
	v, _ := operateIntVals(op, a, b)
	return v
}
func operateIntVals(op lex.Token, a, b int64) (value.Value, error) {
	//u.Infof("a op b:   %v %v %v", a, op.V, b)
	switch op.T {
	case lex.TokenPlus: // +
		//r = a + b
		return value.NewIntValue(a + b), nil
	case lex.TokenStar, lex.TokenMultiply: // *
		//r = a * b
		return value.NewIntValue(a * b), nil
	case lex.TokenMinus: // -
		//r = a - b
		return value.NewIntValue(a - b), nil
	case lex.TokenDivide: //    /
		//r = a / b
		//u.Debugf("divide:   %v / %v = %v", a, b, a/b)
		return value.NewIntValue(a / b), nil
	case lex.TokenModulus: //    %
		//r = a / b
		//u.Debugf("modulus:   %v / %v = %v", a, b, a/b)
		return value.NewIntValue(a % b), nil

	// Below here are Boolean Returns
	case lex.TokenEqualEqual, lex.TokenEqual: //  ==, =
		if a == b {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenGT: //  >
		if a > b {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenNE: //  !=    or <>
		if a != b {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenLT: // <
		if a < b {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenGE: // >=
		if a >= b {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenLE: // <=
		if a <= b {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenLogicOr, lex.TokenOr: //  ||
		if a != 0 || b != 0 {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	case lex.TokenLogicAnd: //  &&
		if a != 0 && b != 0 {
			return value.BoolValueTrue, nil
		} else {
			return value.BoolValueFalse, nil
		}
	}
	return nil, fmt.Errorf("expr: unknown operator %s", op)
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
