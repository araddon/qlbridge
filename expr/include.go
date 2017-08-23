package expr

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/lex"
)

const (
	maxIncludeDepth = 100
)

var (

	// If we hit max depth
	ErrMaxDepth = fmt.Errorf("Recursive Evaluation Error")
)

// InlineIncludes take an expression and resolve any includes so that
// the included expression is "Inline"
func InlineIncludes(ctx Includer, n Node) (Node, error) {
	return inlineIncludesDepth(ctx, n, 0)
}
func inlineIncludesDepth(ctx Includer, arg Node, depth int) (Node, error) {
	if depth > maxIncludeDepth {
		return nil, ErrMaxDepth
	}

	switch n := arg.(type) {
	case NodeArgs:
		args := n.ChildrenArgs()
		for i, narg := range args {
			newNode, err := inlineIncludesDepth(ctx, narg, depth+1)
			if err != nil {
				return nil, err
			}
			if newNode != nil {
				args[i] = newNode
			}
		}
		return arg, nil
	case *NumberNode, *IdentityNode, *StringNode, nil,
		*ValueNode, *NullNode:
		return nil, nil
	case *IncludeNode:
		return resolveInclude(ctx, n, depth+1)
	}
	return nil, fmt.Errorf("unrecognized node type %T", arg)
}

func resolveInclude(ctx Includer, inc *IncludeNode, depth int) (Node, error) {

	if inc.inlineExpr == nil {
		n, err := ctx.Include(inc.Identity.Text)
		if err != nil {
			// ErrNoIncluder is pretty common so don't log it
			if err == ErrNoIncluder {
				return nil, err
			}
			u.Debugf("Could not find include for filter:%s err=%v", inc.String(), err)
			return nil, err
		}
		if n == nil {
			u.Debugf("Includer %T returned a nil filter statement!", inc)
			return nil, ErrIncludeNotFound
		}
		// Now inline, the inlines
		n, err = InlineIncludes(ctx, n)
		if err != nil {
			return nil, err
		}
		if inc.Negated() {
			inc.inlineExpr = NewUnary(lex.Token{T: lex.TokenNegate, V: "NOT"}, n)
		} else {
			inc.inlineExpr = n
		}

	}
	return inc.inlineExpr, nil
}
