package vm

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

// EvalSql Is a partial SQL statement evaluator (that doesn't get it all right).  See
// exec package for full sql evaluator.  Can be used to evaluate a read context and write
// results to write context.  This does not project columns prior to running WHERE.
//
// @writeContext = Write out results of projection
// @readContext  = Message to evaluate does it match where clause?  if so proceed to projection
func EvalSql(sel *rel.SqlSelect, writeContext expr.ContextWriter, readContext expr.EvalContext) (bool, error) {

	// Check and see if we are where Guarded, which would discard the entire message
	if sel.Where != nil {

		whereValue, ok := Eval(readContext, sel.Where.Expr)
		if !ok {
			return false, nil
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				return false, nil
			}
			// ok, continue
		default:
			return false, nil
		}
	}

	for _, col := range sel.Columns {

		if col.Guard != nil {
			ifColValue, ok := Eval(readContext, col.Guard)
			if !ok {
				continue
			}
			switch ifVal := ifColValue.(type) {
			case value.BoolValue:
				if ifVal.Val() == false {
					continue // filter out this col
				}
			default:
				continue // filter out
			}

		}

		v, ok := Eval(readContext, col.Expr)
		if !ok {
			u.Debugf("Could not evaluate: %s  ctx: %#v", col.Expr, readContext)
		} else {
			// Write out the result of the evaluation
			writeContext.Put(col, readContext, v)
		}
	}

	return true, nil
}
