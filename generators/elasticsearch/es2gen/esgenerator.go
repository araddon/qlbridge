package es2gen

import (
	"fmt"
	"strings"
	"time"

	"github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/vm"

	"github.com/araddon/qlbridge/generators/elasticsearch/gentypes"
)

var (
	// MaxDepth specifies the depth at which we are certain the filter generator is in an endless loop
	// This *shouldn't* happen, but is better than a stack overflow
	MaxDepth = 1000

	_ = gou.EMPTY
)

// copy-pasta from entity to avoid the import
// when we actually parameterize this we will need to do it differently anyway
func DayBucket(dt time.Time) int {
	return int(dt.UnixNano() / int64(24*time.Hour))
}

type FilterGenerator struct {
	ts     time.Time
	inc    expr.Includer
	mapper gentypes.FieldMapper
}

func NewGenerator(ts time.Time, inc expr.Includer, mapper gentypes.FieldMapper) *FilterGenerator {
	return &FilterGenerator{ts, inc, mapper}
}

func (fg *FilterGenerator) Walk(stmt *rel.FilterStatement) (*gentypes.Payload, error) {
	payload := &gentypes.Payload{Size: new(int)}
	f, err := fg.walkExpr(stmt.Filter, 0)
	if err != nil {
		return nil, err
	}
	payload.Filter = f

	//TODO order by -> sort
	return payload, nil
}

// expr dispatches to node-type-specific methods
func (fg *FilterGenerator) walkExpr(node expr.Node, depth int) (interface{}, error) {
	if depth > MaxDepth {
		return nil, fmt.Errorf("hit max depth on segment generation. bad query?")
	}
	//gou.Debugf("%d fg.expr T:%T  %#v", depth, node, node)
	var err error
	var filter interface{}
	switch n := node.(type) {
	case *expr.UnaryNode:
		// Urnaries do their own negation
		filter, err = fg.unaryExpr(n, depth+1)
	case *expr.BooleanNode:
		// Also do their own negation
		filter, err = fg.booleanExpr(n, depth+1)
	case *expr.BinaryNode:
		filter, err = fg.binaryExpr(n, depth+1)
	case *expr.TriNode:
		filter, err = fg.triExpr(n, depth+1)
	case *expr.IdentityNode:
		iv := strings.ToLower(n.Text)
		switch iv {
		case "match_all", "*":
			return MatchAll, nil
		}
		//HACK As a special case support true as "match_all"; we could support
		//    false -> MatchNone, but that seems useless and wasteful of ES cpu.
		if n.Bool() {
			return MatchAll, nil
		}
		gou.Warnf("What is this? %v", n)
	case *expr.IncludeNode:
		if incErr := vm.ResolveIncludes(fg.inc, n); incErr != nil {
			return nil, incErr
		}
		filter, err = fg.walkExpr(n.ExprNode, depth+1)
	case *expr.FuncNode:
		filter, err = fg.funcExpr(n, depth+1)
	default:
		gou.Warnf("not handled %v", node)
		return nil, fmt.Errorf("qlindex: unsupported node in expression: %T (%s)", node, node)
	}
	if err != nil {
		// Convert MissingField errors to a logical `false`
		if _, ok := err.(*gentypes.MissingFieldError); ok {
			//gou.Debugf("depth=%d filters=%s missing field: %s", depth, node, err)
			return MatchNone, nil
		}
		return nil, err
	}

	nn, isNegateable := node.(expr.NegateableNode)
	if isNegateable {
		if nn.Negated() {
			return NotFilter(filter), nil
		}
	}
	return filter, nil
}

func (fg *FilterGenerator) unaryExpr(node *expr.UnaryNode, depth int) (interface{}, error) {
	//gou.Debugf("urnary %v", node.Operator.T.String())
	switch node.Operator.T {
	case lex.TokenExists:
		ft, err := esName(fg.mapper, node.Arg)
		if err != nil {
			//gou.Debugf("exists err: %q", err)
			return nil, err
		}
		//gou.Debugf("exists %s", ft)
		return Exists(ft), nil

	case lex.TokenNegate:
		inner, err := fg.walkExpr(node.Arg, depth+1)
		if err != nil {
			return nil, err
		}
		return NotFilter(inner), nil
	default:
		return nil, fmt.Errorf("qlindex: unsupported unary operator: %s", node.Operator.T)
	}
}

// filters returns a boolean expression
func (fg *FilterGenerator) booleanExpr(bn *expr.BooleanNode, depth int) (interface{}, error) {
	if depth > MaxDepth {
		return nil, fmt.Errorf("hit max depth on segment generation. bad query?")
	}
	and := true
	switch bn.Operator.T {
	case lex.TokenAnd, lex.TokenLogicAnd:
	case lex.TokenOr, lex.TokenLogicOr:
		and = false
	default:
		return nil, fmt.Errorf("qlindex: unexpected op %v", bn.Operator)
	}

	items := make([]interface{}, 0, len(bn.Args))
	for _, fe := range bn.Args {
		it, err := fg.walkExpr(fe, depth+1)
		if err != nil {
			// Convert MissingField errors to a logical `false`
			if _, ok := err.(*gentypes.MissingFieldError); ok {
				//gou.Debugf("depth=%d filters=%s missing field: %s", depth, fs, err)
				if !and {
					// Simply skip missing fields in ORs
					continue
				}
				// Convert ANDs to false
				return MatchNone, nil
			}
			return nil, err
		}
		items = append(items, it)
	}

	if len(items) == 1 {
		// Be nice and omit the useless boolean filter since there's only 1 item
		return items[0], nil
	}

	var bf *BoolFilter
	if and {
		bf = AndFilter(items)
	} else {
		bf = OrFilter(items)
	}

	return bf, nil
}

func (fg *FilterGenerator) binaryExpr(node *expr.BinaryNode, depth int) (interface{}, error) {
	// Type check binary expression arguments as they must be:
	// Identifier-Operator-Literal
	lhs, err := esName(fg.mapper, node.Args[0])
	if err != nil {
		return nil, err
	}

	switch op := node.Operator.T; op {
	case lex.TokenGE, lex.TokenLE, lex.TokenGT, lex.TokenLT:
		return makeRange(lhs, op, node.Args[1])

	case lex.TokenEqual, lex.TokenEqualEqual: // the VM supports both = and ==
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported second argument for equality: %T", node.Args[1])
		}
		if lhs.Nested() {
			fieldName, _ := lhs.PrefixAndValue(rhs)
			return Nested(lhs, Term(fieldName, rhs)), nil
			//return nil, fmt.Errorf("qlindex: == not supported for nested types %q", lhs.String())
		}
		return Term(lhs.Field, rhs), nil

	case lex.TokenNE: // ident(0) != literal(1)
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported second argument for equality: %T", node.Args[1])
		}
		if lhs.Nested() {
			fieldName, _ := lhs.PrefixAndValue(rhs)
			return NotFilter(Nested(lhs, Term(fieldName, rhs))), nil
		}
		return NotFilter(Term(lhs.Field, rhs)), nil

	case lex.TokenContains: // ident CONTAINS literal
		rhsstr := ""
		switch rhst := node.Args[1].(type) {
		case *expr.StringNode:
			rhsstr = rhst.Text
		case *expr.IdentityNode:
			rhsstr = rhst.Text
		case *expr.NumberNode:
			rhsstr = rhst.Text
		default:
			return nil, fmt.Errorf("qlindex: unsupported non-string argument for CONTAINS pattern: %T", node.Args[1])
		}
		return makeWildcard(lhs, rhsstr)

	case lex.TokenLike: // ident LIKE literal
		rhsstr := ""
		switch rhst := node.Args[1].(type) {
		case *expr.StringNode:
			rhsstr = rhst.Text
		case *expr.IdentityNode:
			rhsstr = rhst.Text
		case *expr.NumberNode:
			rhsstr = rhst.Text
		default:
			return nil, fmt.Errorf("qlindex: unsupported non-string argument for LIKE pattern: %T", node.Args[1])
		}
		return makeWildcard(lhs, rhsstr)

	case lex.TokenIN, lex.TokenIntersects:
		// Build up list of arguments
		array, ok := node.Args[1].(*expr.ArrayNode)
		if !ok {
			return nil, fmt.Errorf("qlindex: second argument to %s must be an array, found: %T", op, node.Args[1])
		}
		args := make([]interface{}, 0, len(array.Args))
		for _, nodearg := range array.Args {
			strarg, ok := scalar(nodearg)
			if !ok {
				return nil, fmt.Errorf("qlindex: non-scalar argument in %s clause: %T", op, nodearg)
			}
			args = append(args, strarg)
		}

		return In(lhs, args), nil

	default:
		return nil, fmt.Errorf("qlindex: unsupported binary expression: %s", op)
	}
}

func (fg *FilterGenerator) triExpr(node *expr.TriNode, depth int) (interface{}, error) {
	switch op := node.Operator.T; op {
	case lex.TokenBetween: // a BETWEEN b AND c
		// Type check ternary expression arguments as they must be:
		// Identifier(0) BETWEEN Literal(1) AND Literal(2)
		lhs, err := esName(fg.mapper, node.Args[0])
		if err != nil {
			return nil, err
		}
		lower, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for first argument of BETWEEN expression: %T", node.Args[1])
		}
		upper, ok := scalar(node.Args[2])
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for second argument of BETWEEN expression: %T", node.Args[1])
		}
		return makeBetween(lhs, lower, upper)
	}
	return nil, fmt.Errorf("qlindex: unsupported ternary expression: %s", node.Operator.T)
}

func (fg *FilterGenerator) funcExpr(node *expr.FuncNode, depth int) (interface{}, error) {
	switch node.Name {
	case "timewindow":
		// see entity.EvalTimeWindow for code implementation. Checks if the contextual time is within the time buckets provided
		// by the parameters
		if len(node.Args) != 3 {
			return nil, fmt.Errorf("qlindex: 'timewindow' function requires 3 arguments, got %d", len(node.Args))
		}
		//  We are applying the function to the named field, but the caller *can't* just use the fieldname (which would
		// evaluate to nothing, as the field isn't

		lhs, err := esName(fg.mapper, node.Args[0])
		if err != nil {
			return nil, err
		}

		threshold, ok := node.Args[1].(*expr.NumberNode)
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be number, got %T", node.Args[1])
		}

		if !threshold.IsInt {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be number, got %T", node.Args[2])
		}

		window, ok := node.Args[2].(*expr.NumberNode)
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be number, got %T", node.Args[2])
		}

		if !window.IsInt {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be integer, got float", node.Args[2])
		}

		return makeTimeWindowQuery(lhs, threshold.Int64, window.Int64, int64(DayBucket(fg.ts)))
	}
	return nil, fmt.Errorf("qlindex: unsupported function: %s", node.Name)
}
