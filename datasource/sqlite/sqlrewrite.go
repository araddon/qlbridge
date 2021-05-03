package sqlite

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/lytics/qlbridge/vm"
)

type rewrite struct {
	sel           *rel.SqlSelect
	result        *rel.SqlSelect
	needsPolyFill bool // do we request that features be polyfilled?
}

func newRewriter(stmt *rel.SqlSelect) *rewrite {
	m := &rewrite{
		sel:    stmt,
		result: rel.NewSqlSelect(),
	}
	return m
}

// WalkSourceSelect An interface implemented by this connection allowing the planner
// to push down as much logic into mongo as possible
func (m *rewrite) rewrite() (string, error) {

	var err error

	if m.sel.Where != nil {
		m.result.Where = m.sel.Where
		m.result.Where.Expr, err = m.walkNode(m.sel.Where.Expr)
		if err != nil {
			return "", err
		}
	}

	// Evaluate the Select columns make sure we can pass them down or polyfill
	err = m.walkSelectList()
	if err != nil {
		u.Warnf("Could Not evaluate Columns/Aggs %s %v", m.sel.Columns.String(), err)
		return "", err
	}
	m.result.From = m.sel.From

	if len(m.sel.GroupBy) > 0 {
		err = m.walkGroupBy()
		if err != nil {
			u.Warnf("Could Not evaluate GroupBys %s %v", m.sel.GroupBy.String(), err)
			return "", err
		}
	}

	if len(m.sel.OrderBy) > 0 {
		// this should be same right?
		m.result.OrderBy = m.sel.OrderBy
	}

	return m.result.String(), nil
}

// eval() returns ( value, isOk, isIdentity )
func (m *rewrite) eval(arg expr.Node) (value.Value, bool, bool) {
	switch arg := arg.(type) {
	case *expr.NumberNode, *expr.StringNode:
		val, ok := vm.Eval(nil, arg)
		return val, ok, false
	case *expr.IdentityNode:
		if arg.IsBooleanIdentity() {
			return value.NewBoolValue(arg.Bool()), true, false
		}
		return value.NewStringValue(arg.Text), true, true
	case *expr.ArrayNode:
		val, ok := vm.Eval(nil, arg)
		return val, ok, false
	}
	return nil, false, false
}

// Aggregations from the <select_list>
//
//    SELECT <select_list> FROM ... WHERE
//
func (m *rewrite) walkSelectList() error {

	m.result.Columns = m.sel.Columns

	for i := len(m.result.Columns) - 1; i >= 0; i-- {
		col := m.result.Columns[i]
		//u.Debugf("i=%d of %d  %v %#v ", i, len(m.sel.Columns), col.Key(), col)
		if col.Expr != nil {
			switch curNode := col.Expr.(type) {
			// case *expr.NumberNode:
			// 	return nil, value.NewNumberValue(curNode.Float64), nil
			// case *expr.BinaryNode:
			// 	return m.walkBinary(curNode)
			// case *expr.TriNode: // Between
			// 	return m.walkTri(curNode)
			// case *expr.UnaryNode:
			// 	return m.walkUnary(curNode)
			case *expr.FuncNode:
				// All Func Nodes are Aggregates?
				newNode, err := m.selectFunc(curNode)
				if err == nil {
					col.Expr = newNode
				} else if err != nil {
					u.Error(err)
					return err
				}
				//u.Debugf("esm: %v:%v", col.As, esm)
				//u.Debugf(curNode.String())
			// case *expr.ArrayNode:
			// 	return m.walkArrayNode(curNode)
			// case *expr.IdentityNode:
			// 	return nil, value.NewStringValue(curNode.Text), nil
			// case *expr.StringNode:
			// 	return nil, value.NewStringValue(curNode.Text), nil
			case *expr.IdentityNode:
				//u.Debugf("likely a projection, not agg T:%T  %v", curNode, curNode)
			default:
				u.Warnf("unrecognized not agg T:%T  %v", curNode, curNode)
				//panic("Unrecognized node type")
			}
		}

	}
	return nil
}

// Group By Clause:  Mongo is a little weird where they move the
// group by expressions INTO the aggregation clause:
//
//    operation(field) FROM x GROUP BY x,y,z
//
//    db.article.aggregate([{"$group":{_id: "$author", count: {"$sum":1}}}]);
//
func (m *rewrite) walkGroupBy() error {

	for _, col := range m.sel.GroupBy {
		if col.Expr != nil {
			switch col.Expr.(type) {
			case *expr.IdentityNode, *expr.FuncNode:
				newExpr, err := m.walkNode(col.Expr)
				//fld := strings.Replace(expr.FindFirstIdentity(col.Expr), ".", "", -1)
				if err == nil {
					col.Expr = newExpr
				} else {
					u.Error(err)
					return err
				}

			}
		}
	}

	return nil
}

// expressions when used ast part of <select_list>
func (m *rewrite) selectFunc(cur expr.Node) (expr.Node, error) {
	switch curNode := cur.(type) {
	// case *expr.NumberNode:
	// 	return nil, value.NewNumberValue(curNode.Float64), nil
	// case *expr.BinaryNode:
	// 	return m.walkBinary(curNode)
	// case *expr.TriNode: // Between
	// 	return m.walkTri(curNode)
	// case *expr.UnaryNode:
	// 	//return m.walkUnary(curNode)
	// 	u.Warnf("not implemented: %#v", curNode)
	case *expr.FuncNode:
		return m.walkProjectionFunc(curNode)
	// case *expr.ArrayNode:
	// 	return m.walkArrayNode(curNode)
	// case *expr.IdentityNode:
	// 	return nil, value.NewStringValue(curNode.Text), nil
	// case *expr.StringNode:
	// 	return nil, value.NewStringValue(curNode.Text), nil
	default:
		u.Warnf("likely ?? not agg T:%T  %v", cur, cur)
		//panic("Unrecognized node type")
	}
	return cur, nil
}

// Walk() an expression, and its logic to create an appropriately
// nested bson document for mongo queries if possible.
//
// - if can't express logic we need to allow qlbridge to poly-fill
//
func (m *rewrite) walkNode(cur expr.Node) (expr.Node, error) {
	//u.Debugf("WalkNode: %#v", cur)
	switch curNode := cur.(type) {
	case *expr.NumberNode, *expr.StringNode:
		return curNode, nil
	case *expr.BinaryNode:
		return m.walkFilterBinary(curNode)
	case *expr.TriNode: // Between
		return m.walkFilterTri(curNode)
	case *expr.UnaryNode:
		//return m.walkUnary(curNode)
		u.Warnf("not implemented: %#v", curNode)
		return nil, fmt.Errorf("Not implemented urnary function: %v", curNode.String())
	case *expr.FuncNode:
		return m.walkFilterFunc(curNode)
	case *expr.IdentityNode:
		u.Warnf("we are trying to project?   %v", curNode.String())
		return curNode, nil
	case *expr.ArrayNode:
		return m.walkArrayNode(curNode)
	default:
		u.Debugf("unrecognized T:%T  %v", cur, cur)
	}

	return cur, nil
}

// Tri Nodes expressions:
//
//     <expression> [NOT] BETWEEN <expression> AND <expression>
//
func (m *rewrite) walkFilterTri(node *expr.TriNode) (expr.Node, error) {

	/*
		arg1val, aok, _ := m.eval(node.Args[0])
		if !aok {
			return nil, fmt.Errorf("Could not evaluate args: %v", node.String())
		}
		arg2val, bok := vm.Eval(nil, node.Args[1])
		arg3val, cok := vm.Eval(nil, node.Args[2])

		switch node.Operator.T {
		case lex.TokenBetween:
			u.Warnf("between? %T", arg2val.Value())
		default:
			u.Warnf("not implemented ")
		}
	*/

	return node, nil
}

// Array Nodes expressions:
//
//    year IN (1990,1992)  =>
//
func (m *rewrite) walkArrayNode(node *expr.ArrayNode) (expr.Node, error) {

	return node, nil
}

// Binary Node:   operations for >, >=, <, <=, =, !=, AND, OR, Like, IN
//
//    x = y             =>   db.users.find({field: {"$eq": value}})
//    x != y            =>   db.inventory.find( { qty: { $ne: 20 } } )
//
//    x like "list%"    =>   db.users.find( { user_id: /^list/ } )
//    x like "%list%"   =>   db.users.find( { user_id: /bc/ } )
//    x IN [a,b,c]      =>   db.users.find( { user_id: {"$in":[a,b,c] } } )
//
func (m *rewrite) walkFilterBinary(node *expr.BinaryNode) (expr.Node, error) {

	// If we have to recurse deeper for AND, OR operators
	switch node.Operator.T {
	case lex.TokenNE:
		if node.Args[1].String() == "NULL" {
			//u.Errorf("we found something wrong werener")
			//return expr.NewIdentityNodeVal(fmt.Sprintf("%s IS NOT NULL", node.Args[0])), nil
			node.Operator.V = "IS NOT NULL"
			node.Args[1] = expr.NewIdentityNodeVal("")
			//u.Warnf("rh %#v", node.Args[1])
		}
		return node, nil
		// case lex.TokenLogicOr:
		// 	lh, err := m.walkNode(node.Args[0])
		// 	rh, err2 := m.walkNode(node.Args[1])
		// 	if err != nil || err2 != nil {
		// 		u.Errorf("could not get children nodes? %v %v %v", err, err2, node)
		// 		return nil, fmt.Errorf("could not evaluate: %v", node.String())
		// 	}
		// 	node.Args[0] = lh
		// 	node.Args[1] = rh
		// 	return node, nil
	}

	//u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, lhval, rhval, lhval, rhval)
	switch node.Operator.T {
	case lex.TokenEqual, lex.TokenEqualEqual:

	case lex.TokenNE:
		// db.inventory.find( { qty: { $ne: 20 } } )

	case lex.TokenLE:
		// db.inventory.find( { qty: { $lte: 20 } } )

	case lex.TokenLT:
		// db.inventory.find( { qty: { $lt: 20 } } )

	case lex.TokenGE:
		// db.inventory.find( { qty: { $gte: 20 } } )

	case lex.TokenGT:
		// db.inventory.find( { qty: { $gt: 20 } } )

	case lex.TokenLike:
		// { $text: { $search: <string>, $language: <string> } }
		// { <field>: { $regex: /pattern/, $options: '<options>' } }

	case lex.TokenIN:
		// switch vt := node.Args[1].(type) {
		// case value.SliceValue:
		// default:
		// 	u.Warnf("not implemented type %#v", rhval)
		// }

	default:
		u.Warnf("not implemented: %v", node.Operator)
	}

	return node, nil
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
// doesn't really exist, then map that function to a mongo operation
//
//    exists(fieldname)
//    regex(fieldname,value)
//
func (m *rewrite) walkFilterFunc(node *expr.FuncNode) (expr.Node, error) {
	switch funcName := strings.ToLower(node.Name); funcName {
	case "exists", "missing":

	default:
		u.Warnf("not implemented %T", funcName)
	}

	return node, nil
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
// doesn't really exist, then map that function to an Mongo Aggregation/MapReduce function
//
//    min, max, avg, sum, cardinality, terms
//
// Single Value Aggregates:
//       min, max, avg, sum, cardinality, count
//
// MultiValue aggregates:
//      terms, ??
//
func (m *rewrite) walkProjectionFunc(node *expr.FuncNode) (expr.Node, error) {
	switch funcName := strings.ToLower(node.Name); funcName {
	case "count":
		return node, nil
	default:
		u.Warnf("not implemented %v", funcName)
	}
	return node, nil
}

func eval(cur expr.Node) (value.Value, bool) {
	switch curNode := cur.(type) {
	case *expr.IdentityNode:
		if curNode.IsBooleanIdentity() {
			return value.NewBoolValue(curNode.Bool()), true
		}
		return value.NewStringValue(curNode.Text), true
	case *expr.StringNode:
		return value.NewStringValue(curNode.Text), true
	default:
		//u.Errorf("unrecognized T:%T  %v", cur, cur)
	}
	return value.NilValueVal, false
}
