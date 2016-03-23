package vm

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

// Eval applies a sql statement to the specified context
//
//     @writeContext = Write out results of projection
//     @readContext  = Message to evaluate does it match where clause?  if so proceed to projection
//
func EvalSql(sel *rel.SqlSelect, writeContext expr.ContextWriter, readContext expr.ContextReader) (bool, error) {

	// Check and see if we are where Guarded, which would discard the entire message
	if sel.Where != nil {

		whereValue, ok := Eval(readContext, sel.Where.Expr)
		if !ok {
			// TODO:  seriously re-think this.   If the where clause is not able to evaluate
			//     such as  WHERE contains(ip,"10.120.") due to missing IP, does that mean it is
			//      logically true?   Would we not need to correctly evaluate and = true to filter?
			//      Marek made a good point, they would need to expand logical statement to include OR
			return false, nil
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				return false, nil
			}
		default:
			if whereVal.Nil() {
				return false, nil
			}
		}
	}

	//u.Infof("colct=%v  sql=%v", len(sel.Columns), sel.String())
	for _, col := range sel.Columns {

		//u.Debugf("Eval Col.As:%v mt:%v %#v Has IF Guard?%v ", col.As, col.MergeOp.String(), col, col.Guard != nil)
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
				if ifColValue.Nil() {
					continue // filter out this col
				}
			}

		}

		v, ok := Eval(readContext, col.Expr)
		if !ok {
			u.Warnf("Could not evaluate %s", col.Expr)
		} else {
			//u.Debugf(`writeContext.Put("%v",%v)  %s`, col.As, v.Value(), col.String())
			writeContext.Put(col, readContext, v)
		}

	}

	return true, nil
}
