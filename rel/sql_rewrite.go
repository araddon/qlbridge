package rel

import (
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
)

// RewriteSelect We are removing Column Aliases "user_id as uid"
// as well as functions - used when we are going to defer projection, aggs
func RewriteSelect(m *SqlSelect) {
	originalCols := m.Columns
	m.Columns = make(Columns, 0, len(originalCols)+5)
	rewriteIntoProjection(m, originalCols)
	rewriteIntoProjection(m, m.GroupBy)
	if m.Where != nil {
		colsToAdd := expr.FindAllIdentityField(m.Where.Expr)
		addIntoProjection(m, colsToAdd)
	}
	rewriteIntoProjection(m, m.OrderBy)
}

// RewriteSqlSource this Source to act as a stand-alone query to backend
// @parentStmt = the parent statement that this a partial source to
func RewriteSqlSource(m *SqlSource, parentStmt *SqlSelect) *SqlSelect {

	if m.Source != nil {
		return m.Source
	}
	// Rewrite this SqlSource for the given parent, ie
	//   1)  find the column names we need to request from source including those used in join/where
	//   2)  rewrite the where for this partial query
	//   3)  any columns in join expression that are not equal between
	//          sides should be aliased towards the left-hand join portion
	//   4)  if we need different sort for our join algo?

	newCols := make(Columns, 0)
	if !parentStmt.Star {
		for idx, col := range parentStmt.Columns {
			left, _, hasLeft := col.LeftRight()
			if !hasLeft {
				// Was not left/right qualified, so use as is?  or is this an error?
				//  what is official sql grammar on this?
				newCol := col.Copy()
				newCol.ParentIndex = idx
				newCol.Index = len(newCols)
				newCols = append(newCols, newCol)

			} else if hasLeft && left == m.Alias {
				newCol := col.CopyRewrite(m.Alias)
				newCol.ParentIndex = idx
				newCol.SourceIndex = len(newCols)
				newCol.Index = len(newCols)
				newCols = append(newCols, newCol)
			}
		}
	}

	// TODO:
	//  - rewrite the Sort
	//  - rewrite the group-by
	sql2 := &SqlSelect{Columns: newCols, Star: parentStmt.Star}
	m.joinNodes = make([]expr.Node, 0)
	if m.SubQuery != nil {
		if len(m.SubQuery.From) != 1 {
			u.Errorf("Not supported, nested subQuery %v", m.SubQuery.String())
		} else {
			sql2.From = append(sql2.From, &SqlSource{Name: m.SubQuery.From[0].Name})
		}
	} else {
		sql2.From = append(sql2.From, &SqlSource{Name: m.Name})
	}

	for _, from := range parentStmt.From {
		// We need to check each participant in the Join for possible
		// columns which need to be re-written
		sql2.Columns = columnsFromJoin(m, from.JoinExpr, sql2.Columns)

		// We also need to create an expression used for evaluating
		// the values of Join "Keys"
		if from.JoinExpr != nil {
			joinNodesForFrom(parentStmt, m, from.JoinExpr, 0)
		}
	}

	if parentStmt.Where != nil {
		node, cols := rewriteWhere(parentStmt, m, parentStmt.Where.Expr, make(Columns, 0))
		if node != nil {
			sql2.Where = &SqlWhere{Expr: node}
		}
		if len(cols) > 0 {
			parentIdx := len(parentStmt.Columns)
			for _, col := range cols {
				col.Index = len(sql2.Columns)
				col.ParentIndex = parentIdx
				parentIdx++
				sql2.Columns = append(sql2.Columns, col)
			}
		}
	}
	m.Source = sql2
	m.cols = sql2.UnAliasedColumns()
	return sql2
}
func rewriteIntoProjection(sel *SqlSelect, m Columns) {
	if len(m) == 0 {
		return
	}
	colsToAdd := make([]string, 0)
	for _, c := range m {
		// u.Infof("source=%-15s as=%-15s exprT:%T expr=%s  star:%v", c.As, c.SourceField, c.Expr, c.Expr, c.Star)
		switch n := c.Expr.(type) {
		case *expr.IdentityNode:
			colsToAdd = append(colsToAdd, c.SourceField)
		case *expr.FuncNode:

			idents := expr.FindAllIdentities(n)
			for _, in := range idents {
				_, r, _ := in.LeftRight()
				colsToAdd = append(colsToAdd, r)
			}

		case nil:
			if c.Star {
				colsToAdd = append(colsToAdd, "*")
			} else {
				u.Warnf("unhandled column? %T  %s", n, n)
			}

		default:
			u.Warnf("unhandled column? %T  %s", n, n)
		}
	}
	addIntoProjection(sel, colsToAdd)
}
func addIntoProjection(sel *SqlSelect, newCols []string) {
	notExists := make(map[string]bool)
	for _, colName := range newCols {
		colName = strings.ToLower(colName)
		found := false
		for _, c := range sel.Columns {
			if c.SourceField == colName {
				// already in projection
				found = true
				break
			}
		}
		if !found {
			notExists[colName] = true
			if colName == "*" {
				sel.AddColumn(Column{Star: true})
			} else {
				nc := NewColumn(colName)
				sel.AddColumn(*nc)
			}
		}
	}
}
func rewriteWhere(stmt *SqlSelect, from *SqlSource, node expr.Node, cols Columns) (expr.Node, Columns) {
	//u.Debugf("rewrite where %s", node)
	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, hasLeft := nt.LeftRight(); hasLeft {
			//u.Debugf("rewriteWhere  from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := expr.IdentityNode{Text: right}
				cols = append(cols, NewColumn(right))
				//u.Warnf("nice, found it! in = %v  cols:%d", in, len(cols))
				return &in, cols
			} else {
				//u.Warnf("what to do? source:%v    %v", from.alias, nt.String())
			}
		} else {
			//u.Debugf("returning original: %s", nt)
			return node, cols
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode:
		return nt, cols
	case *expr.BinaryNode:
		//u.Infof("binaryNode  T:%v", nt.Operator.T.String())
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			var n1, n2 expr.Node
			n1, cols = rewriteWhere(stmt, from, nt.Args[0], cols)
			n2, cols = rewriteWhere(stmt, from, nt.Args[1], cols)

			if n1 != nil && n2 != nil {
				return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}, cols
			} else if n1 != nil {
				return n1, cols
			} else if n2 != nil {
				return n2, cols
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenGT, lex.TokenGE, lex.TokenLE, lex.TokenNE:
			var n1, n2 expr.Node
			n1, cols = rewriteWhere(stmt, from, nt.Args[0], cols)
			n2, cols = rewriteWhere(stmt, from, nt.Args[1], cols)
			//u.Debugf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			if n1 != nil && n2 != nil {
				return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}, cols
				// } else if n1 != nil {
				// 	return n1
				// } else if n2 != nil {
				// 	return n2
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		default:
			//u.Warnf("un-implemented op: %#v", nt)
		}
	default:
		u.Warnf("%T node types are not suppored yet for where rewrite", node)
	}
	//u.Warnf("nil?? %T  %s  %#v", node, node, node)
	return nil, cols
}

func joinNodesForFrom(stmt *SqlSelect, from *SqlSource, node expr.Node, depth int) expr.Node {

	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, hasLeft := nt.LeftRight(); hasLeft {
			//u.Debugf("joinNodesForFrom  from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				identNode := expr.IdentityNode{Text: right}
				//u.Debugf("%d nice, found it! identnode=%q fromnode:%q", depth, identNode.String(), nt.String())
				if depth == 1 {
					from.joinNodes = append(from.joinNodes, &identNode)
					return nil
				}
				return &identNode
			} else {
				// This is for other side of join, ignore
				//u.Warnf("what to do? source:%v    %v", from.alias, nt.String())
			}
		} else {
			u.Warnf("dropping join expr node: %q", nt.String())
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode, *expr.ValueNode:
		//u.Warnf("skipping? %v", nt.String())
		return nt
	case *expr.FuncNode:
		//u.Warnf("%v  try join from func node: %v", depth, nt.String())
		args := make([]expr.Node, len(nt.Args))
		for i, arg := range nt.Args {
			args[i] = rewriteNode(from, arg)
			if args[i] == nil {
				// What???
				//u.Infof("error, from:%q   arg:%q", from.String(), arg.String())
				return nil
			}
		}
		fn := expr.NewFuncNode(nt.Name, nt.F)
		fn.Args = args
		if depth == 1 {
			//u.Infof("adding func: %s", fn.String())
			from.joinNodes = append(from.joinNodes, fn)
			return nil
		}
		return fn
	case *expr.BinaryNode:
		//u.Infof("%v binaryNode  %v", depth, nt.String())
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			n1 := joinNodesForFrom(stmt, from, nt.Args[0], depth+1)
			n2 := joinNodesForFrom(stmt, from, nt.Args[1], depth+1)

			if n1 != nil && n2 != nil {
				//u.Debugf("%d neither nil:  n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				//return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
			} else if n1 != nil {
				//u.Debugf("%d n1 not nil: n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				return n1
			} else if n2 != nil {
				//u.Debugf("%d n2 not nil n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				return n2
			} else {
				//u.Warnf("%d n1=%#v  n2=%#v    %#v", depth, n1, n2, nt)
			}
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenGT, lex.TokenGE, lex.TokenLE, lex.TokenNE:
			n1 := joinNodesForFrom(stmt, from, nt.Args[0], depth+1)
			n2 := joinNodesForFrom(stmt, from, nt.Args[1], depth+1)

			if n1 != nil && n2 != nil {
				//u.Debugf("%d neither nil:  n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				//return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
			} else if n1 != nil {
				//u.Debugf("%d n1 not nil: n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				// 	return n1
				if depth == 1 {
					//u.Infof("adding node: %s", n1.String())
					from.joinNodes = append(from.joinNodes, n1)
					return nil
				}
			} else if n2 != nil {
				//u.Debugf("%d  n2 not nil n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				if depth == 1 {
					//u.Infof("adding node: %s", n1.String())
					from.joinNodes = append(from.joinNodes, n2)
					return nil
				}
				// 	return n2
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		default:
			u.Warnf("un-implemented op: %#v", nt)
		}
	default:
		u.Warnf("%T node types are not suppored yet for where rewrite", node)
	}
	return nil
}

// We need to find all columns used in the given Node (where/join expression)
//  to ensure we have those columns in projection for sub-queries
func columnsFromJoin(from *SqlSource, node expr.Node, cols Columns) Columns {
	if node == nil {
		return cols
	}
	//u.Debugf("columnsFromJoin()  T:%T  node=%q", node, node.String())
	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			//u.Debugf("from.Name:%v AS %v   Joinnode l:%v  r:%v    %#v", from.Name, from.alias, left, right, nt)
			//u.Warnf("check cols against join expr arg: %#v", nt)
			if left == from.alias {
				found := false
				for _, col := range cols {
					colLeft, colRight, _ := col.LeftRight()
					//u.Debugf("left='%s'  colLeft='%s' right='%s'  %#v", left, colLeft, colRight,  col)
					//u.Debugf("col:  From %s AS '%s'   '%s'.'%s'  JoinExpr: '%v'.'%v' col:%#v", from.Name, from.alias, colLeft, colRight, left, right, col)
					if left == colLeft || colRight == right {
						found = true
						//u.Infof("columnsFromJoin from.Name:%v l:%v  r:%v", from.alias, left, right)
					} else {
						//u.Warnf("not? from.Name:%v l:%v  r:%v   col: P:%p %#v", from.alias, left, right, col, col)
					}
				}
				if !found {
					//u.Debugf("columnsFromJoin from.Name:%v l:%v  r:%v", from.alias, left, right)
					newCol := &Column{As: right, SourceField: right, Expr: &expr.IdentityNode{Text: right}}
					newCol.Index = len(cols)
					newCol.ParentIndex = -1 // if -1, we don't need in parent index
					cols = append(cols, newCol)
					//u.Warnf("added col %s idx:%d pidx:%v", right, newCol.Index, newCol.Index)
				}
			}
		}
	case *expr.FuncNode:
		//u.Warnf("columnsFromJoin func node: %s", nt.String())
		for _, arg := range nt.Args {
			cols = columnsFromJoin(from, arg, cols)
		}
	case *expr.BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			cols = columnsFromJoin(from, nt.Args[0], cols)
			cols = columnsFromJoin(from, nt.Args[1], cols)
		case lex.TokenEqual, lex.TokenEqualEqual:
			cols = columnsFromJoin(from, nt.Args[0], cols)
			cols = columnsFromJoin(from, nt.Args[1], cols)
		default:
			u.Warnf("un-implemented op: %v", nt.Operator)
		}
	default:
		u.LogTracef(u.INFO, "whoops")
		u.Warnf("%T node types are not suppored yet for join rewrite %s", node, from.String())
	}
	return cols
}

// Remove any aliases
func rewriteNode(from *SqlSource, node expr.Node) expr.Node {
	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			//u.Debugf("rewriteNode from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := expr.IdentityNode{Text: right}
				//u.Warnf("nice, found it! in = %v", in)
				return &in
			}
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode, *expr.ValueNode:
		//u.Warnf("skipping? %v", nt.String())
		return nt
	case *expr.BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			n1 := rewriteNode(from, nt.Args[0])
			n2 := rewriteNode(from, nt.Args[1])
			return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}
		case lex.TokenEqual, lex.TokenEqualEqual:
			n := rewriteNode(from, nt.Args[0])
			if n != nil {
				return n
			}
			n = rewriteNode(from, nt.Args[1])
			if n != nil {
				return n
			}
			u.Warnf("Could not find node: %#v", node)
		default:
			u.Warnf("un-implemented op: %v", nt.Operator)
		}
	case *expr.FuncNode:
		fn := expr.NewFuncNode(nt.Name, nt.F)
		fn.Args = make([]expr.Node, len(nt.Args))
		for i, arg := range nt.Args {
			fn.Args[i] = rewriteNode(from, arg)
			if fn.Args[i] == nil {
				// What???
				u.Warnf("error, nil node: %s", arg.String())
				return nil
			}
		}
		return fn
	default:
		u.Warnf("%T node types are not suppored yet for column rewrite", node)
	}
	return nil
}
