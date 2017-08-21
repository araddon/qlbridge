package expr

import (
	"strings"
)

// Determine if this expression node uses datemath (ie, "now-4h")
// - only works on right-hand of equation
// - doesn't work on args of a function
func HasDateMath(node Node) bool {

	switch n := node.(type) {
	case *BinaryNode:
		switch rh := n.Args[1].(type) {
		case *StringNode, *ValueNode:
			if strings.HasPrefix(strings.ToLower(rh.String()), `"now`) {
				return true
			}
		case *BinaryNode:
			if HasDateMath(rh) {
				return true
			}
		}
	case *BooleanNode:
		for _, arg := range n.Args {
			if HasDateMath(arg) {
				return true
			}
		}
	case *UnaryNode:
		return HasDateMath(n.Arg)
	}
	return false
}
