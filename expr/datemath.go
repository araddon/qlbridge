package expr

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/lytics/datemath"
)

type timeSlice []time.Time

func (p timeSlice) Len() int {
	return len(p)
}

func (p timeSlice) Less(i, j int) bool {
	return p[i].Before(p[j])
}

func (p timeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// DateConverter can help inspect an expression to determine if there is
// date-math in it.
type DateConverter struct {
	HasDateMath bool
	Node        Node
	TimeStrings []string
	maths       timeSlice
	ts          time.Time
	err         error
}

// NewDateConverter takes a node expression
func NewDateConverter(n Node) (*DateConverter, error) {
	dc := &DateConverter{Node: n, ts: time.Now()}
	dc.findDateMath(n)
	if dc.err == nil && len(dc.maths) > 0 {
		dc.HasDateMath = true
	}
	if dc.err != nil {
		return nil, dc.err
	}
	return dc, nil
}

func (d *DateConverter) addValue(op lex.TokenType, val string) {
	ts, err := datemath.EvalAnchor(d.ts, val)
	if err != nil {
		d.err = err
		return
	}

	d.TimeStrings = append(d.TimeStrings, val)

	// 1) ----------- N --------------  T1 < F < N
	//     T1------F                    T1 < "now-1d" = true
	//
	// 2) ----------- N --------------  T1 < F < N
	//     T1--------------F            T1 < "now-1d" = false but will be in 1d - (N - T1)
	//
	// 3) ------ N -------------------  N < T1 < F
	//              T1--------------F   T1 < "now-1d" = false (and never will be)
	d.maths = append(d.maths, ts)

	/*
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
	*/
}

// NextTimeCheck given all the date-maths in this node find the sorted
// minimum time at which to check
func (d *DateConverter) NextTimeCheck() time.Time {

	return time.Time{}
}

// Determine if this expression node uses datemath (ie, "now-4h")
func (d *DateConverter) findDateMath(node Node) {

	switch n := node.(type) {
	case *BinaryNode:
		if len(n.Args) > 2 {
			d.err = fmt.Errorf("not enough args")
			return
		}
		for _, arg := range n.Args {
			switch narg := arg.(type) {
			case *StringNode:
				val := strings.ToLower(narg.Text)
				if strings.HasPrefix(val, `now`) {
					d.addValue(n.Operator.T, val)
				}
			}
		}

	case *BooleanNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *UnaryNode:
		d.findDateMath(n.Arg)
	case *TriNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *FuncNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *ArrayNode:
		for _, arg := range n.Args {
			d.findDateMath(arg)
		}
	case *NumberNode:
		// this cannot possibly be a date math
	default:
		u.Debugf("No case for %T", n)
	}
}
