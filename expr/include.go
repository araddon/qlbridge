package expr

import (
	"fmt"

	"github.com/araddon/qlbridge/lex"
)

const (
	maxIncludeDepth = 100
)

var (
	// If we hit max depth
	ErrMaxDepth = fmt.Errorf("Recursive Evaluation Error")
)

// FindIncludes recursively descend down a node looking for all Include identities
func FindIncludes(node Node) []string {
	return findAllIncludes(node, nil)
}

// InlineIncludes take an expression and resolve any includes so that
// the included expression is "Inline"
func InlineIncludes(ctx Includer, n Node) (Node, error) {
	return doInlineIncludes(ctx, n, 0)
}
func doInlineIncludes(ctx Includer, n Node, depth int) (Node, error) {
	// We need to make a copy, so we lazily use the To/From pb
	// We need the copy because we are going to mutate this node
	// but AST is assumed to be immuteable, and shared, since we are breaking
	// this contract we copy
	npb := n.NodePb()
	newNode := NodeFromNodePb(npb)
	return inlineIncludesDepth(ctx, newNode, depth)
}
func inlineIncludesDepth(ctx Includer, arg Node, depth int) (Node, error) {
	if depth > maxIncludeDepth {
		return nil, ErrMaxDepth
	}

	switch n := arg.(type) {
	// FuncNode, BinaryNode, BooleanNode, TriNode, UnaryNode, ArrayNode
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
	case *IncludeNode:
		return resolveInclude(ctx, n, depth+1)
	default:
		//*NumberNode, *IdentityNode, *StringNode, nil,
		//*ValueNode, *NullNode:
		return arg, nil
	}
}

func resolveInclude(ctx Includer, inc *IncludeNode, depth int) (Node, error) {

	// if inc.inlineExpr != nil {
	// 	return inc.inlineExpr, nil
	// }

	n, err := ctx.Include(inc.Identity.Text)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, ErrIncludeNotFound
	}

	// Now inline, the inlines
	n, err = doInlineIncludes(ctx, n, depth)
	if err != nil {
		return nil, err
	}
	if inc.Negated() {
		inc.inlineExpr = NewUnary(lex.Token{T: lex.TokenNegate, V: "NOT"}, n)
	} else {
		inc.inlineExpr = n
	}

	inc.ExprNode = inc.inlineExpr

	return inc.inlineExpr, nil
}

func findAllIncludes(node Node, current []string) []string {
	switch n := node.(type) {
	case *IncludeNode:
		current = append(current, n.Identity.Text)
	case *BinaryNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *BooleanNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *UnaryNode:
		current = findAllIncludes(n.Arg, current)
	case *TriNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *ArrayNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	case *FuncNode:
		for _, arg := range n.Args {
			current = findAllIncludes(arg, current)
		}
	}
	return current
}
