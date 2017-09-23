package vm

import (
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/datemath"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

// DateConverter can help inspect a boolean expression to determine if there is
// date-math in it.  If there is datemath, can calculate the time boundary
// where the expression may possibly change from true to false.
// - Must be boolean expression
// - Only calculates the first boundary
// - Only calculates POSSIBLE boundary, given complex logic (ors etc) may NOT change.
type DateConverter struct {
	HasDateMath bool      // Does this have date math in it all?
	Node        expr.Node // The expression we are extracting datemath from
	TimeStrings []string  // List of each extracted timestring
	bt          time.Time // The possible boundary time when expression flips true/false
	at          time.Time // The Time to use as "now" or reference point
	ctx         expr.EvalIncludeContext
	err         error
}

// NewDateConverter takes a node expression
func NewDateConverter(ctx expr.EvalIncludeContext, n expr.Node) (*DateConverter, error) {
	dc := &DateConverter{
		Node: n,
		at:   time.Now(),
		ctx:  ctx,
	}
	dc.findDateMath(n)
	if dc.err == nil && len(dc.TimeStrings) > 0 {
		dc.HasDateMath = true
	}
	if dc.err != nil {
		return nil, dc.err
	}
	return dc, nil
}
func (d *DateConverter) addBoundary(bt time.Time) {
	if d.bt.IsZero() {
		d.bt = bt
		return
	}
	if bt.Before(d.bt) {
		d.bt = bt
	}
}
func (d *DateConverter) addValue(lhv value.Value, op lex.TokenType, val string) {

	ct, ok := value.ValueToTime(lhv)
	if !ok {
		u.Debugf("Could not convert %T: %v to time.Time", lhv, lhv)
		return
	}

	// Given Anchor Time At calculate Relative Time Rt
	rt, err := datemath.EvalAnchor(d.at, val)
	if err != nil {
		d.err = err
		return
	}

	// Ct = Comparison time, left hand side of expression
	// At = Anchor Time
	// Rt = Relative time result of Anchor Time offset by datemath "now-3d"
	// Bt = Boundary time = calculated time at which expression will change boolean expression value
	switch op {
	case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE:
		// none of these are supported operators for finding boundary
		return
	case lex.TokenGT, lex.TokenGE:
		// 1) ----------- Ct --------------     Rt < Ct
		//        Rt                            Ct > "now+-1d" = true but will be true when at + (ct - rt)
		//         ------Bt
		//
		// 2) ------------- Ct ------------     Ct < Rt
		//                        Rt            Ct > "now+-1d" = false, and will always be false
		//
		if rt.Before(ct) {
			bt := d.at.Add(ct.Sub(rt))
			d.addBoundary(bt)
		} else {
			// Is false, and always will be false no candidates
		}
	case lex.TokenLT, lex.TokenLE:
		// 3) ------ Ct -------------------     Ct < Rt
		//              Rt                      Ct < "now+-1d" = true (and always will be)
		//
		// 4) ----------- Ct --------------     Rt < Ct
		//     At----Rt                         Ct < "now+-1d" = true, but will be in true when at + (ct - rt)
		//           Bt---|
		//
		if ct.Before(rt) {
			// Is true, and always will be true no candidates
		} else {
			bt := d.at.Add(ct.Sub(rt))
			d.addBoundary(bt)
		}
	}
}

// Boundary given all the date-maths in this node find the boundary time where
// this expression possibly will change boolean value.
// If no boundaries exist, returns time.Time{} (zero time)
func (d *DateConverter) Boundary() time.Time {
	return d.bt
}

// Determine if this expression node uses datemath (ie, "now-4h")
func (d *DateConverter) findDateMath(node expr.Node) {

	switch n := node.(type) {
	case *expr.BinaryNode:

		for i, arg := range n.Args {
			switch narg := arg.(type) {
			case *expr.StringNode:
				val := strings.ToLower(narg.Text)
				if strings.HasPrefix(val, `now`) {

					d.TimeStrings = append(d.TimeStrings, val)

					// If left side is datemath   "now-3d" < ident then re-write to have ident on left
					var lhv value.Value
					op := n.Operator.T
					var ok bool
					if i == 0 {
						lhv, ok = Eval(d.ctx, n.Args[1])
						if !ok {
							continue
						}
						// Reverse equation to put identity on left side
						// "now-1d" < last_visit    =>   "last_visit" > "now-1d"
						switch n.Operator.T {
						case lex.TokenGT:
							op = lex.TokenLT
						case lex.TokenGE:
							op = lex.TokenLE
						case lex.TokenLT:
							op = lex.TokenGT
						case lex.TokenLE:
							op = lex.TokenGE
						default:
							// lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE:
							// none of these are supported operators for finding boundary
							continue
						}
					} else if i == 1 {
						lhv, ok = Eval(d.ctx, n.Args[0])
						if !ok {
							continue
						}
					}

					d.addValue(lhv, op, val)
					continue
				}
			default:
				d.findDateMath(arg)
			}
		}

	case *expr.BooleanNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *expr.UnaryNode:
		d.findDateMath(n.Arg)
	case *expr.TriNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *expr.FuncNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *expr.ArrayNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *expr.IncludeNode:
		if err := resolveInclude(d.ctx, n, 0); err != nil {
			d.err = err
			return
		}
		if n.ExprNode != nil {
			d.findDateMath(n.ExprNode)
		}
	case *expr.NumberNode, *expr.ValueNode, *expr.IdentityNode, *expr.StringNode:
		// Scalar/Literal values cannot be datemath, must be binary-expression
	}
}
