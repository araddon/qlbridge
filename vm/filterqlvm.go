package vm

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// Includer defines an interface used for resolving INCLUDE clauses into a
// *FilterStatement. Implementations should return an error if the name cannot
// be resolved.
type Includer interface {
	Include(name string) (*rel.FilterStatement, error)
}

type filterql struct {
	inc Includer
}

// EvalFilerSelect applies a FilterSelect statement to the specified contexts
//
//     @writeContext = Write results of projection
//     @readContext  = Message input, ie evaluate for Where/Filter clause
//
func EvalFilterSelect(sel *rel.FilterSelect, inc Includer, writeContext expr.ContextWriter, readContext expr.ContextReader) (bool, error) {

	// Check and see if we are where Guarded, which would discard the entire message
	if sel.FilterStatement != nil {

		matcher := NewFilterVm(inc)
		matches, err := matcher.Matches(readContext, sel.FilterStatement)
		//u.Infof("matches? %v err=%v for %s", matches, err, sel.FilterStatement.String())
		if err != nil {
			return false, err
		}
		if !matches {
			return false, nil
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

func NewFilterVm(i Includer) *filterql {
	return &filterql{inc: i}
}

// Matches executes a FilterQL query against an entity returning true if the
// entity matches.
func (q *filterql) Matches(cr expr.ContextReader, stmt *rel.FilterStatement) (bool, error) {
	return q.matchesFilters(cr, stmt.Filter)
}

func (q *filterql) matchesFilters(cr expr.ContextReader, fs *rel.Filters) (bool, error) {
	var and bool
	switch fs.Op {
	case lex.TokenAnd, lex.TokenLogicAnd:
		and = true
	case lex.TokenOr, lex.TokenLogicOr:
		and = false
	default:
		return false, fmt.Errorf("unexpected op %v", fs.Op)
	}

	//u.Infof("filters and?%v  filter=%q", and, fs.String())
	for _, filter := range fs.Filters {

		matches, err := q.matchesFilter(cr, filter)
		//u.Debugf("matches filter?%v  err=%q  f=%q", matches, err, filter.String())
		if err != nil {
			return false, err
		}
		if !and && matches {
			// one of the expressions in an OR clause matched, shortcircuit true
			if fs.Negate {
				return false, nil
			}
			return true, nil
		}
		if and && !matches {
			// one of the expressions in an AND clause did not match, shortcircuit false
			if fs.Negate {
				return true, nil
			}
			return false, nil
		}
	}
	// no shortcircuiting, if and=true this means all expressions returned true...
	// ...if and=false (OR) this means all expressions returned false.
	if fs.Negate {
		return !and, nil
	}
	return and, nil
}

func (q *filterql) matchesFilter(cr expr.ContextReader, exp *rel.FilterExpr) (bool, error) {
	switch {
	case exp.Include != "":

		if exp.IncludeFilter == nil {
			filterStmt, err := q.inc.Include(exp.Include)
			if err != nil {
				u.Warn(err)
				return false, err
			}
			if filterStmt == nil {
				u.Errorf("Includer %T returned a nil filter statement!", q.inc)
				return false, fmt.Errorf("failed to resolve INCLUDE %q: %v", exp.Include, err)
			}
			exp.IncludeFilter = filterStmt
		}
		doesMatch, err := q.Matches(cr, exp.IncludeFilter)
		if err != nil {
			return false, err
		}

		//u.Debugf("include? %q  negate?%v", exp.IncludeFilter.String(), exp.Negate)
		if exp.Negate {
			return !doesMatch, nil
		}
		return doesMatch, nil

	case exp.Expr != nil:
		// Hand it off to the single expression Evaluator
		out, ok := Eval(cr, exp.Expr)
		//u.Debugf("expr? %q out?%#v  ok?%v", exp.Expr.String(), out, ok)
		if !ok || out == nil {
			// VM unable to evaluate expression -> treat it as false
			if exp.Negate {
				return true, nil
			}
			return false, nil
		}
		//u.Infof("out? negate?%v  nil?%v err?%v  %#v", exp.Negate, out.Nil(), out.Err(), out.Value())
		if out.Nil() {
			return false, fmt.Errorf("unexpected empty output from %q", exp.Expr)
		}
		if out.Err() {
			return false, fmt.Errorf("evaluation error: %v", out.Value())
		}
		if out.Type() != value.BoolType {
			return false, fmt.Errorf("expression returned non-bool type: %s", out.Type())
		}
		outVal := out.Value().(bool)
		if exp.Negate {
			return !outVal, nil
		}
		return outVal, nil

	case exp.Filter != nil:
		doesMatch, err := q.matchesFilters(cr, exp.Filter)
		if err != nil {
			return false, err
		}

		//u.Debugf("filter? %q  negate?%v", exp.IncludeFilter.String(), exp.Negate)
		if exp.Negate {
			return !doesMatch, nil
		}
		return doesMatch, nil

	case exp.MatchAll:
		return true, nil

	default:
		return false, fmt.Errorf("empty expression")
	}
}
