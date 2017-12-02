// VM implements the virtual machine runtime evaluator
// for the SQL, FilterQL, and Expression evalutors.
package vm

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/mb0/glob"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	// MaxDepth acts as a guard against potentially recursive queries
	MaxDepth = 10000
	// ErrMaxDepth If we hit max depth on recursion
	ErrMaxDepth = fmt.Errorf("Recursive Evaluation Error")
	// ErrUnknownOp an unrecognized Operator in expression
	ErrUnknownOp = fmt.Errorf("expr: unknown op type")
	// ErrUnknownNodeType Unhandled Node type for expression evaluation
	ErrUnknownNodeType = fmt.Errorf("expr: unknown node type")
	// ErrExecute could not evaluate an expression
	ErrExecute = fmt.Errorf("Could not execute")
)

// EvalBaseContext base context for expression evaluation
type EvalBaseContext struct {
	expr.EvalContext
}

// Eval - Evaluate the given expression (arg Node) against the given context.
// @ctx is the evaluation context ie the variables/values which the expression will be
// evaluated against.  It may be a simple reader of  message/data or any
// object whhich implements EvalContext.
func Eval(ctx expr.EvalContext, arg expr.Node) (value.Value, bool) {
	return evalDepth(ctx, arg, 0)
}

// errRecover is the handler that turns panics into returns from the top
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
func numberNodeToValue(t *expr.NumberNode) (value.Value, bool) {
	if t.IsInt {
		return value.NewIntValue(t.Int64), true
	} else if t.IsFloat {
		fv, ok := value.StringToFloat64(t.Text)
		if !ok {
			u.Debugf("Could not perform numeric conversion for %q", t.Text)
			return value.NilValueVal, false
		}
		return value.NewNumberValue(fv), true
	}
	u.Debugf("Could not find numeric conversion for %#v", t)
	return value.NilValueVal, false
}

// ResolveIncludes take an expression and resolve any includes so that
// it does not have to be resolved at runtime.  There is also a
// InlineIncludes alternative in expr pkg which actually re-writes the expression
// to remove includes and embed the expressions they refer to as part of this expression.
func ResolveIncludes(ctx expr.Includer, arg expr.Node) error {
	return resolveIncludesDepth(ctx, arg, 0)
}
func resolveIncludesDepth(ctx expr.Includer, arg expr.Node, depth int) error {
	if depth > MaxDepth {
		return ErrMaxDepth
	}
	// can we switch to arg.Type()
	switch n := arg.(type) {
	case *expr.BinaryNode:
		for _, narg := range n.Args {
			if err := resolveIncludesDepth(ctx, narg, depth+1); err != nil {
				return err
			}
		}
	case *expr.BooleanNode:
		for _, narg := range n.Args {
			if err := resolveIncludesDepth(ctx, narg, depth+1); err != nil {
				return err
			}
		}
	case *expr.UnaryNode:
		if err := resolveIncludesDepth(ctx, n.Arg, depth+1); err != nil {
			return err
		}
	case *expr.TriNode:
		for _, narg := range n.Args {
			if err := resolveIncludesDepth(ctx, narg, depth+1); err != nil {
				return err
			}
		}
	case *expr.ArrayNode:
		for _, narg := range n.Args {
			if err := resolveIncludesDepth(ctx, narg, depth+1); err != nil {
				return err
			}
		}
	case *expr.FuncNode:
		for _, narg := range n.Args {
			if err := resolveIncludesDepth(ctx, narg, depth+1); err != nil {
				return err
			}
		}
	case *expr.NumberNode, *expr.IdentityNode, *expr.StringNode, nil,
		*expr.ValueNode, *expr.NullNode:
		return nil
	case *expr.IncludeNode:
		return resolveInclude(ctx, n, depth+1)
	}
	return nil
}

func evalBool(ctx expr.EvalContext, arg expr.Node, depth int) (bool, bool) {
	val, ok := evalDepth(ctx, arg, depth)
	if !ok || val == nil {
		return false, false
	}
	if bv, isBool := val.(value.BoolValue); isBool {
		return bv.Val(), true
	}
	return false, false
}

func evalDepth(ctx expr.EvalContext, arg expr.Node, depth int) (value.Value, bool) {
	if depth > MaxDepth {
		return nil, false
	}

	switch argVal := arg.(type) {
	case *expr.NumberNode:
		return numberNodeToValue(argVal)
	case *expr.BinaryNode:
		return walkBinary(ctx, argVal, depth)
	case *expr.BooleanNode:
		return walkBoolean(ctx, argVal, depth)
	case *expr.UnaryNode:
		return walkUnary(ctx, argVal, depth)
	case *expr.TriNode:
		return walkTernary(ctx, argVal, depth)
	case *expr.ArrayNode:
		return walkArray(ctx, argVal, depth)
	case *expr.FuncNode:
		return walkFunc(ctx, argVal, depth)
	case *expr.IdentityNode:
		return walkIdentity(ctx, argVal)
	case *expr.StringNode:
		return value.NewStringValue(argVal.Text), true
	case nil:
		return nil, false
	case *expr.NullNode:
		// WHERE (`users.user_id` != NULL)
		return value.NewNilValue(), true
	case *expr.IncludeNode:
		return walkInclude(ctx, argVal, depth+1)
	case *expr.ValueNode:
		if argVal.Value == nil {
			return nil, false
		}
		switch val := argVal.Value.(type) {
		case *value.NilValue, value.NilValue:
			return nil, false
		case value.SliceValue:
			return val, true
		}
		u.Errorf("Unknonwn node type:  %#v", argVal.Value)
		panic(ErrUnknownNodeType)
	default:
		u.Errorf("Unknonwn node type:  %#v", arg)
		panic(ErrUnknownNodeType)
	}
}

func resolveInclude(ctx expr.Includer, inc *expr.IncludeNode, depth int) error {

	if inc.ExprNode != nil {
		return nil
	}

	incExpr, err := ctx.Include(inc.Identity.Text)
	if err != nil {
		if err == expr.ErrNoIncluder {
			return err
		}
		u.Debugf("Could not find include for filter:%s err=%v", inc.String(), err)
		return err
	}
	if incExpr == nil {
		u.Debugf("Includer %T returned a nil filter statement!", inc)
		return expr.ErrIncludeNotFound
	}
	if err = ResolveIncludes(ctx, incExpr); err != nil {
		return err
	}
	inc.ExprNode = incExpr
	return nil
}

func walkInclude(ctx expr.EvalContext, inc *expr.IncludeNode, depth int) (value.Value, bool) {

	if inc.ExprNode == nil {
		incCtx, ok := ctx.(expr.EvalIncludeContext)
		if !ok {
			u.Errorf("No Includer context? %T", ctx)
			return nil, false
		}
		if err := resolveInclude(incCtx, inc, depth); err != nil {
			return nil, false
		}
	}

	switch exp := inc.ExprNode.(type) {
	case *expr.IdentityNode:
		if exp.Text == "*" || exp.Text == "match_all" {
			return value.NewBoolValue(true), true
		}
	}

	matches, ok := evalBool(ctx, inc.ExprNode, depth+1)
	//u.Debugf("matches filter?%v ok=%v  f=%q", matches, ok, bn)
	if !ok {
		if inc.Negated() {
			return value.NewBoolValue(true), true
		}
		return nil, false
	}
	if inc.Negated() {
		return value.NewBoolValue(!matches), true
	}
	return value.NewBoolValue(matches), true
}

func walkBoolean(ctx expr.EvalContext, n *expr.BooleanNode, depth int) (value.Value, bool) {
	if depth > MaxDepth {
		u.Warnf("Recursive query death? %v", n)
		return nil, false
	}
	var and bool
	switch n.Operator.T {
	case lex.TokenAnd, lex.TokenLogicAnd:
		and = true
	case lex.TokenOr, lex.TokenLogicOr:
		and = false
	default:
		u.Warnf("un-recognized operator %v", n.Operator)
		return value.BoolValueFalse, false
	}

	//u.Debugf("filters and?%v  filter=%q", and, n)
	for _, bn := range n.Args {

		matches, ok := evalBool(ctx, bn, depth+1)
		//u.Debugf("matches filter?%v ok=%v  f=%q", matches, ok, bn)
		if !ok && and {
			return nil, false
		} else if !ok {
			continue
		}
		if !and && matches {
			// one of the expressions in an OR clause matched, shortcircuit true
			if n.Negated() {
				return value.BoolValueFalse, true
			}
			return value.BoolValueTrue, true
		}
		if and && !matches {
			// one of the expressions in an AND clause did not match, shortcircuit false
			if n.Negated() {
				return value.BoolValueTrue, true
			}
			return value.BoolValueFalse, true
		}
	}

	// no shortcircuiting, if and=true this means all expressions returned true...
	// ...if and=false (OR) this means all expressions returned false.
	if n.Negated() {
		return value.NewBoolValue(!and), true
	}
	return value.NewBoolValue(and), true
}

// Binary operands:   =, ==, !=, OR, AND, >, <, >=, <=, LIKE, contains
//
//       x == y,   x = y
//       x != y
//       x OR y
//       x > y
//       x < =
//
func walkBinary(ctx expr.EvalContext, node *expr.BinaryNode, depth int) (value.Value, bool) {
	val, ok := evalBinary(ctx, node, depth)
	//u.Debugf("eval: %#v  ok?%v", val, ok)
	if !ok {
		return nil, ok
	}
	// if node.Negated() {
	// 	bv, isBool := val.(value.BoolValue)
	// 	if isBool {
	// 		return value.NewBoolValue(!bv.Val()), true
	// 	}
	// 	// This should not be possible
	// 	u.Warnf("Negated binary but non bool response? %T expr:%s", val, node)
	// }
	return val, ok
}
func evalBinary(ctx expr.EvalContext, node *expr.BinaryNode, depth int) (value.Value, bool) {
	ar, aok := evalDepth(ctx, node.Args[0], depth+1)
	br, bok := evalDepth(ctx, node.Args[1], depth+1)

	// u.Debugf("walkBinary: aok?%v ar:%v %T  node=%s %T", aok, ar, ar, node.Args[0], node.Args[0])
	// u.Debugf("walkBinary: bok?%v br:%v %T  node=%s %T", bok, br, br, node.Args[1], node.Args[1])
	// u.Debugf("walkBinary: l:%v  r:%v  %T  %T node=%s", ar, br, ar, br, node)
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
		// u.Warnf("walkBinary not ok: op=%s %v  l:%v  r:%v  %T  %T", node.Operator, node, ar, br, ar, br)
		return nil, false
	}

	// Else if we can only evaluate right
	if !aok {
		switch node.Operator.T {
		case lex.TokenIntersects, lex.TokenContains, lex.TokenLike:
			return value.NewBoolValue(false), true
		case lex.TokenIN:
			return value.NewBoolValue(false), true
		}
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
		case lex.TokenIN:
			return value.NewBoolValue(false), true
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
					rhi, rhok := value.ValueToInt64(val)
					if rhok && rhi == at.Val() {
						return value.BoolValueTrue, true
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
		case value.Slice:
			switch node.Operator.T {
			case lex.TokenIN:
				for _, val := range bt.SliceValue() {
					if at.Val() == val.ToString() {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true
			case lex.TokenContains:
				for _, val := range bt.SliceValue() {
					if strings.Contains(at.Val(), val.ToString()) {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true
			case lex.TokenLike: // a(value) LIKE b(pattern)
				for _, val := range bt.SliceValue() {
					bv, ok := LikeCompare(at.Val(), val.ToString())
					if ok && bv.Val() {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true
			default:
				u.Debugf("unsupported op for SliceValue op:%v rhT:%T", node.Operator, br)
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
				}
			}
			switch node.Operator.T {
			case lex.TokenLogicOr, lex.TokenOr, lex.TokenEqualEqual, lex.TokenEqual, lex.TokenLogicAnd,
				lex.TokenAnd, lex.TokenIN, lex.TokenContains, lex.TokenLike:
				return value.NewBoolValue(false), true
			}
			// Should we evaluate strings that are non-nil to be = true?
			u.Debugf("not handled: boolean %v %T=%v  expr: %s", node.Operator, at.Value(), at.Val(), node.String())
			return nil, false
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

		case value.IntValue:
			n := operateNumbers(node.Operator, at.NumberValue(), bt.NumberValue())
			return n, true
		case value.NumberValue:
			n := operateNumbers(node.Operator, at.NumberValue(), bt)
			return n, true
		case value.TimeValue:
			lht, ok := value.ValueToTime(at)
			if !ok {
				return value.BoolValueFalse, false
			}
			return operateTime(node.Operator.T, lht, bt.Val())
		default:
			u.Errorf("at?%T  %v bt? %T     %v", at, at.Value(), bt, bt.Value())
		}
		return nil, false
	case value.SliceValue:
		switch node.Operator.T {
		case lex.TokenGT, lex.TokenGE, lex.TokenLT, lex.TokenLE, lex.TokenEqualEqual, lex.TokenEqual, lex.TokenNE:

			if at.Len() == 0 {
				return value.NewBoolValue(false), true
			}

			// Lets look at first arg, all in slice must be of same type
			switch at.Val()[0].(type) {
			case value.TimeValue:

				rht, ok := value.ValueToTime(br)
				if !ok {
					return value.BoolValueFalse, false
				}

				for _, arg := range at.Val() {

					lht, ok := arg.(value.TimeValue)
					if !ok {
						return value.NewErrorValue(fmt.Errorf("All values of slice must be same type %v", at)), false
					}

					if isTrue, _ := operateTime(node.Operator.T, lht.Val(), rht); isTrue.Val() {
						return value.BoolValueTrue, true
					}
				}
				return value.BoolValueFalse, true
			}

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
				// [a,b,c] contains int
				for _, val := range at.Val() {
					sliceInt, ok := value.ValueToInt64(val)
					if ok && sliceInt == bval.Val() {
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
		case lex.TokenLogicOr, lex.TokenOr, lex.TokenLogicAnd, lex.TokenAnd, lex.TokenIN:
			return value.NewBoolValue(false), true
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
		case lex.TokenLogicOr, lex.TokenOr, lex.TokenEqualEqual, lex.TokenEqual, lex.TokenLogicAnd,
			lex.TokenAnd, lex.TokenIN:
			return value.NewBoolValue(false), true
		}
		return nil, false
	case value.TimeValue:

		lht := at.Val()
		rht, ok := value.ValueToTime(br)
		if !ok {
			return value.BoolValueFalse, false
		}

		return operateTime(node.Operator.T, lht, rht)

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
			return value.NewBoolValue(false), false
		default:
			//u.Debugf("left side nil binary:  %q", node)
			return nil, false
		}
	default:
		u.Debugf("Unknown op?  %T  %T  %v", ar, at, ar)
		return value.NewErrorValue(fmt.Errorf("unsupported left side value: %T in %s", at, node)), false
	}

	return value.NewErrorValue(fmt.Errorf("unsupported binary expression: %s", node)), false
}

func walkIdentity(ctx expr.EvalContext, node *expr.IdentityNode) (value.Value, bool) {

	if node.IsBooleanIdentity() {
		return value.NewBoolValue(node.Bool()), true
	}
	if ctx == nil {
		return nil, false
	}
	if node.HasLeftRight() {
		return ctx.Get(node.OriginalText())
	}
	return ctx.Get(node.Text)
}

func walkUnary(ctx expr.EvalContext, node *expr.UnaryNode, depth int) (value.Value, bool) {

	a, ok := Eval(ctx, node.Arg)
	//u.Debugf("urnary a:%v ok:%v  %s", a, ok, node)
	if !ok {
		switch node.Operator.T {
		case lex.TokenExists:
			return value.NewBoolValue(false), true
		case lex.TokenNegate:
			return value.NewBoolValue(false), false
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

// walkTernary ternary evaluator
//
//     A   BETWEEN   B  AND C
//
func walkTernary(ctx expr.EvalContext, node *expr.TriNode, depth int) (value.Value, bool) {

	a, aok := Eval(ctx, node.Args[0])
	b, bok := Eval(ctx, node.Args[1])
	c, cok := Eval(ctx, node.Args[2])
	//u.Infof("tri:  %T:%v  %v  %T:%v   %T:%v", a, a, node.Operator, b, b, c, c)
	if !aok {
		return nil, false
	}
	if !bok || !cok {
		return nil, false
	}
	if a == nil || b == nil || c == nil {
		return nil, false
	}
	switch node.Operator.T {
	case lex.TokenBetween:
		switch at := a.(type) {
		case value.IntValue:

			av := at.Val()
			bv, ok := value.ValueToInt64(b)
			if !ok {
				return nil, false
			}
			cv, ok := value.ValueToInt64(c)
			if !ok {
				return nil, false
			}
			if av > bv && av < cv {
				return value.NewBoolValue(true), true
			}

			return value.NewBoolValue(false), true
		case value.NumberValue:

			av := at.Val()
			bv, ok := value.ValueToFloat64(b)
			if !ok {
				return nil, false
			}
			cv, ok := value.ValueToFloat64(c)
			if !ok {
				return nil, false
			}
			if av > bv && av < cv {
				return value.NewBoolValue(true), true
			}

			return value.NewBoolValue(false), true

		case value.TimeValue:

			av := at.Val()
			bv, ok := value.ValueToTime(b)
			if !ok {
				return nil, false
			}
			cv, ok := value.ValueToTime(c)
			if !ok {
				return nil, false
			}
			if av.Unix() > bv.Unix() && av.Unix() < cv.Unix() {
				return value.NewBoolValue(true), true
			}

			return value.NewBoolValue(false), true

		default:
			u.Warnf("between not implemented for type %s %#v", a.Type().String(), node)
		}
	default:
		u.Warnf("ternary node walk not implemented for node %#v", node)
	}

	return nil, false
}

// walkArray Array evaluator:  evaluate multiple values into an array
//
//     (b,c,d)
//
func walkArray(ctx expr.EvalContext, node *expr.ArrayNode, depth int) (value.Value, bool) {

	vals := make([]value.Value, len(node.Args))

	for i := 0; i < len(node.Args); i++ {
		v, _ := Eval(ctx, node.Args[i])
		vals[i] = v
	}

	// we are returning an array of evaluated nodes
	return value.NewSliceValues(vals), true
}

//  walkFunc evaluates a function
func walkFunc(ctx expr.EvalContext, node *expr.FuncNode, depth int) (value.Value, bool) {

	if node.F.CustomFunc == nil {
		return nil, false
	}
	if node.Eval == nil {
		u.LogThrottle(u.WARN, 10, "No Eval() for %s", node.Name)
		return nil, false
	}

	args := make([]value.Value, len(node.Args))

	for i, a := range node.Args {
		v, ok := Eval(ctx, a)
		if !ok {
			v = value.NewNilValue()
		}
		args[i] = v
	}
	return node.Eval(ctx, args)
}

func operateNumbers(op lex.Token, av, bv value.NumberValue) value.Value {
	switch op.T {
	case lex.TokenPlus, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide, lex.TokenMinus,
		lex.TokenModulus:
		if math.IsNaN(av.Val()) || math.IsNaN(bv.Val()) {
			return value.NewNumberValue(math.NaN())
		}
	}

	a, b := av.Val(), bv.Val()
	switch op.T {
	case lex.TokenPlus: // +
		return value.NewNumberValue(a + b)
	case lex.TokenStar, lex.TokenMultiply: // *
		return value.NewNumberValue(a * b)
	case lex.TokenMinus: // -
		return value.NewNumberValue(a - b)
	case lex.TokenDivide: //
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
		if a == b {
			return value.BoolValueTrue
		}
		return value.BoolValueFalse
	case lex.TokenNE: //  !=
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

func operateTime(op lex.TokenType, lht, rht time.Time) (value.BoolValue, bool) {
	// u.Debugf("time compare %v %v %v", lht, node.Operator.T, rht)
	// if rht.IsZero() {
	// 	return nil, false
	// }
	switch op {
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
		u.Debugf("unhandled date op %v", op)
	}
	return value.BoolValueFalse, false
}

// LikeCompare takes two strings and evaluates them for like equality
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
		if b == 0 {
			return nil, fmt.Errorf("Divide by Zero error")
		}
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
