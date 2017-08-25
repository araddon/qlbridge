package expr

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
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
func NewDateConverter(n Node) *DateConverter {
	dc := &DateConverter{Node: n, ts: time.Now()}
	dc.findDateMath(n)
	if dc.err == nil && len(dc.maths) > 0 {
		dc.HasDateMath = true
	}
	return dc
}

func (d *DateConverter) addValue(val string) {
	ts, err := datemath.EvalAnchor(d.ts, val)
	if err != nil {
		d.err = err
		return
	}
	d.TimeStrings = append(d.TimeStrings, val)
	d.maths = append(d.maths, ts)
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
			switch n := arg.(type) {
			case *StringNode:
				val := strings.ToLower(n.Text)
				if strings.HasPrefix(val, `now`) {
					d.addValue(val)
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
