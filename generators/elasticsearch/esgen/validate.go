package esgen

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/vm"

	"github.com/araddon/qlbridge/generators/elasticsearch/gentypes"
)

var (
	_ = u.EMPTY

	// Ensure our schema implments filter validation
	_ gentypes.FilterValidate = (*TypeValidator)(nil)
)

/*
	// FieldMapper translates a given column name to a field-type
	FieldMapper interface {
		Column(col string) value.ValueType
		// Map a FilterStatement column to an Elasticsearch field or false if the field
		// doesn't exist.
		Map(qlcol string) (*FieldType, bool)
	}
	// FilterValidate interface Will walk a filter statement validating columns, types
	// against underlying Schema.
	FilterValidate func(fs *rel.FilterStatement) error
*/
type TypeValidator struct {
	fm gentypes.FieldMapper
}

func NewValidator(fm gentypes.FieldMapper) *TypeValidator {
	return &Validator{fm: fm}
}

func (m *Validator) FilterValidate(stmt *rel.FilterStatement) error {
	return m.walkExpr(stmt.Filter)
}

func (m *TypeValidator) walkExpr(node expr.Node) error {

	//gou.Debugf("%d m.expr T:%T  %#v", depth, node, node)
	switch n := node.(type) {
	case *expr.UnaryNode:
		return m.unaryExpr(n)
	case *expr.BooleanNode:
		return m.booleanExpr(n)
	case *expr.BinaryNode:
		return m.binaryExpr(n)
	case *expr.TriNode:
		return m.triExpr(n)
	case *expr.IdentityNode:
		vt := m.fm.Column(n.Text)

	case *expr.IncludeNode:
		// We assume included statement has don't its own validation
		return nil
	case *expr.FuncNode:
		return m.funcExpr(n)
	default:
		gou.Warnf("not handled type validation %v %T", node, node)
		return fmt.Errorf("esgen: unsupported node in expression: %T (%s)", node, node)
	}
}

func (m *TypeValidator) unaryExpr(node *expr.UnaryNode, depth int) error {
	//gou.Debugf("urnary %v", node.Operator.T.String())
	switch node.Operator.T {
	case lex.TokenExists:
		ft, err := esName(m.mapper, node.Arg)
		if err != nil {
			//gou.Debugf("exists err: %q", err)
			return nil, err
		}
		//gou.Debugf("exists %s", ft)
		return Exists(ft), nil

	case lex.TokenNegate:
		inner, err := m.walkExpr(node.Arg, depth+1)
		if err != nil {
			return nil, err
		}
		return NotFilter(inner), nil
	default:
		return nil, fmt.Errorf("esgen: unsupported unary operator: %s", node.Operator.T)
	}
}

// filters returns a boolean expression
func (m *TypeValidator) booleanExpr(bn *expr.BooleanNode, depth int) error {
	if depth > MaxDepth {
		return nil, fmt.Errorf("hit max depth on segment generation. bad query?")
	}
	and := true
	switch bn.Operator.T {
	case lex.TokenAnd, lex.TokenLogicAnd:
	case lex.TokenOr, lex.TokenLogicOr:
		and = false
	default:
		return nil, fmt.Errorf("esgen: unexpected op %v", bn.Operator)
	}

	items := make([]interface{}, 0, len(bn.Args))
	for _, fe := range bn.Args {
		it, err := m.walkExpr(fe, depth+1)
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

func (m *TypeValidator) binaryExpr(node *expr.BinaryNode, depth int) error {
	// Type check binary expression arguments as they must be:
	// Identifier-Operator-Literal
	lhs, err := esName(m.mapper, node.Args[0])
	if err != nil {
		return nil, err
	}

	switch op := node.Operator.T; op {
	case lex.TokenGE, lex.TokenLE, lex.TokenGT, lex.TokenLT:
		return makeRange(lhs, op, node.Args[1])

	case lex.TokenEqual, lex.TokenEqualEqual: // the VM supports both = and ==
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("esgen: unsupported second argument for equality: %T", node.Args[1])
		}
		if lhs.Nested() {
			fieldName, _ := lhs.PrefixAndValue(rhs)
			return Nested(lhs, Term(fieldName, rhs)), nil
			//return nil, fmt.Errorf("esgen: == not supported for nested types %q", lhs.String())
		}
		return Term(lhs.Field, rhs), nil

	case lex.TokenNE: // ident(0) != literal(1)
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("esgen: unsupported second argument for equality: %T", node.Args[1])
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
			return nil, fmt.Errorf("esgen: unsupported non-string argument for CONTAINS pattern: %T", node.Args[1])
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
			return nil, fmt.Errorf("esgen: unsupported non-string argument for LIKE pattern: %T", node.Args[1])
		}
		return makeWildcard(lhs, rhsstr)

	case lex.TokenIN, lex.TokenIntersects:
		// Build up list of arguments
		array, ok := node.Args[1].(*expr.ArrayNode)
		if !ok {
			return nil, fmt.Errorf("esgen: second argument to %s must be an array, found: %T", op, node.Args[1])
		}
		args := make([]interface{}, 0, len(array.Args))
		for _, nodearg := range array.Args {
			strarg, ok := scalar(nodearg)
			if !ok {
				return nil, fmt.Errorf("esgen: non-scalar argument in %s clause: %T", op, nodearg)
			}
			args = append(args, strarg)
		}

		return In(lhs, args), nil

	default:
		return nil, fmt.Errorf("esgen: unsupported binary expression: %s", op)
	}
}

func (m *TypeValidator) triExpr(node *expr.TriNode, depth int) error {
	switch op := node.Operator.T; op {
	case lex.TokenBetween: // a BETWEEN b AND c
		// Type check ternary expression arguments as they must be:
		// Identifier(0) BETWEEN Literal(1) AND Literal(2)
		lhs, err := esName(m.mapper, node.Args[0])
		if err != nil {
			return nil, err
		}
		lower, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("esgen: unsupported type for first argument of BETWEEN expression: %T", node.Args[1])
		}
		upper, ok := scalar(node.Args[2])
		if !ok {
			return nil, fmt.Errorf("esgen: unsupported type for second argument of BETWEEN expression: %T", node.Args[1])
		}
		return makeBetween(lhs, lower, upper)
	}
	return nil, fmt.Errorf("esgen: unsupported ternary expression: %s", node.Operator.T)
}

func (m *TypeValidator) funcExpr(node *expr.FuncNode, depth int) error {
	switch node.Name {
	case "timewindow":
		// see entity.EvalTimeWindow for code implementation. Checks if the contextual time is within the time buckets provided
		// by the parameters
		if len(node.Args) != 3 {
			return nil, fmt.Errorf("esgen: 'timewindow' function requires 3 arguments, got %d", len(node.Args))
		}
		//  We are applying the function to the named field, but the caller *can't* just use the fieldname (which would
		// evaluate to nothing, as the field isn't

		lhs, err := esName(m.mapper, node.Args[0])
		if err != nil {
			return nil, err
		}

		threshold, ok := node.Args[1].(*expr.NumberNode)
		if !ok {
			return nil, fmt.Errorf("esgen: unsupported type for 'timewindow' argument. must be number, got %T", node.Args[1])
		}

		if !threshold.IsInt {
			return nil, fmt.Errorf("esgen: unsupported type for 'timewindow' argument. must be number, got %T", node.Args[2])
		}

		window, ok := node.Args[2].(*expr.NumberNode)
		if !ok {
			return nil, fmt.Errorf("esgen: unsupported type for 'timewindow' argument. must be number, got %T", node.Args[2])
		}

		if !window.IsInt {
			return nil, fmt.Errorf("esgen: unsupported type for 'timewindow' argument. must be integer, got float", node.Args[2])
		}

		return makeTimeWindowQuery(lhs, threshold.Int64, window.Int64, int64(DayBucket(m.ts)))
	}
	return nil, fmt.Errorf("esgen: unsupported function: %s", node.Name)
}
