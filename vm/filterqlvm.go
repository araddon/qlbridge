package vm

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// a static nil includer whose job is to return errors
	// for vm's that don't have an includer
	noIncluder = &expr.IncludeContext{}
)

type filterql struct {
	expr.EvalContext
	expr.Includer
}

// EvalFilerSelect evaluates a FilterSelect statement from read, into write context
//
// @writeContext = Write results of projection
// @readContext  = Message input, ie evaluate for Where/Filter clause
func EvalFilterSelect(sel *rel.FilterSelect, writeContext expr.ContextWriter, readContext expr.EvalContext) (bool, bool) {

	ctx, ok := readContext.(expr.EvalIncludeContext)
	if !ok {
		ctx = &expr.IncludeContext{ContextReader: readContext}
	}
	// Check and see if we are where Guarded, which would discard the entire message
	if sel.FilterStatement != nil {

		matches, ok := Matches(ctx, sel.FilterStatement)
		if !ok {
			return false, ok
		}
		if !matches {
			return false, ok
		}
	}

	for _, col := range sel.Columns {

		if col.Guard != nil {
			ifColValue, ok := Eval(readContext, col.Guard)
			if !ok {
				u.Debugf("Could not evaluate if:  T:%T  v:%v", col.Guard, col.Guard.String())
				continue
			}
			switch ifVal := ifColValue.(type) {
			case value.BoolValue:
				if ifVal.Val() == false {
					continue // filter out this col
				}
			default:
				continue
			}

		}

		v, ok := Eval(readContext, col.Expr)
		if ok {
			writeContext.Put(col, readContext, v)
		}

	}

	return true, true
}

// Matches executes a FilterQL statement against an evaluation context
// returning true if the context matches.
func MatchesInc(inc expr.Includer, cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return matchesExpr(filterql{cr, inc}, stmt.Filter, 0)
}

// Matches executes a FilterQL statement against an evaluation context
// returning true if the context matches.
func Matches(cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return matchesExpr(cr, stmt.Filter, 0)
}

// MatchesExpr executes a expr.Node expression against an evaluation context
// returning true if the context matches.
func MatchesExpr(cr expr.EvalContext, node expr.Node) (bool, bool) {
	return matchesExpr(cr, node, 0)
}

func matchesExpr(cr expr.EvalContext, n expr.Node, depth int) (bool, bool) {
	switch exp := n.(type) {
	case *expr.IdentityNode:
		if exp.Text == "*" || exp.Text == "match_all" {
			return true, true
		}
	}
	val, ok := Eval(cr, n)
	if !ok || val == nil {
		return false, ok
	}
	if bv, isBool := val.(value.BoolValue); isBool {
		return bv.Val(), ok
	}
	return false, true
}
