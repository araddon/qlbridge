package vm

import (
	"fmt"
	"reflect"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/ast"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

type State struct {
	ExprVm // reference to the VM operating on this state
	// We make a reflect value of self (state) as we use []reflect.ValueOf often
	rv     reflect.Value
	Reader ContextReader
	Writer ContextWriter
}

func NewState(vm ExprVm, read ContextReader, write ContextWriter) *State {
	s := &State{
		ExprVm: vm,
		Reader: read,
		Writer: write,
	}
	s.rv = reflect.ValueOf(s)
	return s
}

func (e *State) Walk(arg ast.Node) (value.Value, bool) {
	//u.Debugf("Walk() node=%T  %v", arg, arg)
	// Can we redo this as a Visit() pattern?
	//      return arg.EvalVisit(e)
	switch argVal := arg.(type) {
	case *ast.NumberNode:
		return numberNodeToValue(argVal), true
	case *ast.BinaryNode:
		return e.walkBinary(argVal), true
	case *ast.UnaryNode:
		return e.walkUnary(argVal)
	case *ast.FuncNode:
		//return e.walkFunc(argVal)
		return e.walkFunc(argVal)
	case *ast.IdentityNode:
		return e.walkIdentity(argVal)
	case *ast.StringNode:
		return value.NewStringValue(argVal.Text), true
	default:
		u.Errorf("Unknonwn node type:  %T", argVal)
		panic(ErrUnknownNodeType)
	}
}

func (e *State) walkBinary(node *ast.BinaryNode) value.Value {
	ar, aok := e.Walk(node.Args[0])
	br, bok := e.Walk(node.Args[1])
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

func (e *State) walkIdentity(node *ast.IdentityNode) (value.Value, bool) {

	if node.IsBooleanIdentity() {
		//u.Debugf("walkIdentity() boolean: node=%T  %v Bool:%v", node, node, node.Bool())
		return value.NewBoolValue(node.Bool()), true
	}
	//u.Debugf("walkIdentity() node=%T  %v", node, node)
	return e.Reader.Get(node.Text)
}

func (e *State) walkUnary(node *ast.UnaryNode) (value.Value, bool) {

	a, ok := e.Walk(node.Arg)
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

func (e *State) walkFunc(node *ast.FuncNode) (value.Value, bool) {

	//u.Debugf("walk node --- %v   ", node.StringAST())

	//we create a set of arguments to pass to the function, first arg
	// is this *State
	var ok bool
	funcArgs := []reflect.Value{e.rv}
	for _, a := range node.Args {

		//u.Debugf("arg %v  %T %v", a, a, a.Type().Kind())

		var v interface{}

		switch t := a.(type) {
		case *ast.StringNode: // String Literal
			v = value.NewStringValue(t.Text)
		case *ast.IdentityNode: // Identity node = lookup in context

			if t.IsBooleanIdentity() {
				v = value.NewBoolValue(t.Bool())
			} else {
				v, ok = e.Reader.Get(t.Text)
				if !ok {
					// nil arguments are valid
					v = value.NewNilValue()
				}
			}

		case *ast.NumberNode:
			v = numberNodeToValue(t)
		case *ast.FuncNode:
			//u.Debugf("descending to %v()", t.Name)
			v, ok = e.walkFunc(t)
			if !ok {
				return value.NewNilValue(), false
			}
			//u.Debugf("result of %v() = %v, %T", t.Name, v, v)
			//v = extractScalar()
		case *ast.UnaryNode:
			//v = extractScalar(e.walkUnary(t))
			v, ok = e.walkUnary(t)
			if !ok {
				return value.NewNilValue(), false
			}
		case *ast.BinaryNode:
			//v = extractScalar(e.walkBinary(t))
			v = e.walkBinary(t)
		default:
			panic(fmt.Errorf("expr: unknown func arg type"))
		}

		if v == nil {
			//u.Warnf("Nil vals?  %v  %T  arg:%T", v, v, a)
			// What do we do with Nil Values?
			switch a.(type) {
			case *ast.StringNode: // String Literal
				u.Warnf("NOT IMPLEMENTED T:%T v:%v", a, a)
			case *ast.IdentityNode: // Identity node = lookup in context
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
	u.Debugf("Calling func:%v(%v)", node.F.Name, funcArgs)
	fnRet := node.F.F.Call(funcArgs)
	u.Infof("fnRet: %v", fnRet)
	// check if has an error response?
	if len(fnRet) > 1 && !fnRet[1].Bool() {
		// What do we do if not ok?
		return value.EmptyStringValue, false
	}
	//u.Debugf("response %v %v  %T", node.F.Name, fnRet[0].Interface(), fnRet[0].Interface())
	return fnRet[0].Interface().(value.Value), true
}
