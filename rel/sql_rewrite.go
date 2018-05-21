package rel

import (
	fmt "fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/schema"
)

type (
	rewriteSelect struct {
		sel         *SqlSelect
		cols        map[string]bool
		matchSource string
		features    *schema.DataSourceFeatures
		result      *RewriteSelectResult
	}
	// RewriteSelectResult describes the result of a re-write statement to
	// tell the planner which poly-fill features are needed based on re-write.
	RewriteSelectResult struct {
		NeedsProjection bool
		NeedsWhere      bool
		NeedsGroupBy    bool
	}
)

func newRewriteSelect(sel *SqlSelect) *rewriteSelect {
	rw := &rewriteSelect{
		sel:      sel,
		cols:     make(map[string]bool),
		features: schema.FeaturesDefault(),
		result:   &RewriteSelectResult{},
	}
	return rw
}

// ReWriteStatement given SqlStatement
func ReWriteStatement(input SqlStatement) error {
	switch stmt := input.(type) {
	case *SqlSelect:
		return rewriteSelectStatement(stmt)
	default:
		return fmt.Errorf("Rewrite not implemented for %T", input)
	}
}

// rewriteSelectStatement We are removing Column Aliases "user_id as uid"
// as well as functions - used when we are going to defer projection, aggs
func rewriteSelectStatement(sel *SqlSelect) error {
	rw := newRewriteSelect(sel)

	originalCols := sel.Columns
	sel.Columns = make(Columns, 0, len(originalCols)+5)
	if err := rw.intoProjection(sel, originalCols); err != nil {
		return err
	}
	if err := rw.intoProjection(sel, sel.GroupBy); err != nil {
		return err
	}
	if sel.Where != nil {
		cols := expr.FindAllIdentityField(sel.Where.Expr)
		for _, col := range cols {
			nc := NewColumn(col)
			nc.ParentIndex = -1
			rw.addColumn(*nc)
		}
	}
	if err := rw.intoProjection(sel, sel.OrderBy); err != nil {
		return err
	}
	return nil
}

// RewriteSqlSource this SqlSource to act as a stand-alone query to backend
// @parentStmt = the parent statement that this a partial source to
func RewriteSqlSource(source *SqlSource, parentStmt *SqlSelect) (*SqlSelect, error) {

	if source.Source != nil {
		return source.Source, nil
	}
	// Rewrite this SqlSource for the given parent, ie
	//   1)  find the column names we need to request from source including those used in join/where
	//   2)  rewrite the where for this partial query
	//   3)  any columns in join expression that are not equal between
	//          sides should be aliased towards the left-hand join portion
	//   4)  if we need different sort for our join algo?

	sql2 := &SqlSelect{Columns: make(Columns, 0), Star: parentStmt.Star}
	rw := newRewriteSelect(sql2)
	rw.matchSource = source.Alias
	originalCols := parentStmt.Columns

	if err := rw.intoProjection(sql2, originalCols); err != nil {
		return nil, err
	}
	//u.Debugf("after into projection: %s", sql2.Columns)
	// TODO:
	//  - rewrite the Sort
	//  - rewrite the group-by

	source.joinNodes = make([]expr.Node, 0)
	if source.SubQuery != nil {
		if len(source.SubQuery.From) != 1 {
			u.Errorf("Not supported, nested subQuery %v", source.SubQuery.String())
		} else {
			sql2.From = append(sql2.From, &SqlSource{Name: source.SubQuery.From[0].Name})
		}
	} else {
		sql2.From = append(sql2.From, &SqlSource{Name: source.Name})
	}

	for _, from := range parentStmt.From {
		// We need to check each participant in the Join for possible
		// columns which need to be re-written
		rw.columnsFromExpression(source, from.JoinExpr)

		// We also need to create an expression used for evaluating
		// the values of Join "Keys"
		if from.JoinExpr != nil {
			rw.joinNodesForFrom(parentStmt, source, from.JoinExpr, 0)
		}
	}
	//u.Debugf("after FROM: %s", sql2.Columns)

	if parentStmt.Where != nil {
		node := rw.rewriteWhere(parentStmt, source, parentStmt.Where.Expr)
		if node != nil {
			sql2.Where = &SqlWhere{Expr: node}
		}
		/*
			if len(cols) > 0 {
				parentIdx := len(parentStmt.Columns)
				for _, col := range cols {
					col.Index = len(sql2.Columns)
					col.ParentIndex = parentIdx
					parentIdx++
					sql2.Columns = append(sql2.Columns, col)
				}
			}
		*/
	}
	//u.Debugf("after WHERE: %s", sql2.Columns)
	source.Source = sql2
	source.cols = sql2.UnAliasedColumns()
	return sql2, nil
}
func (m *rewriteSelect) addColumn(col Column) {
	col.Index = len(m.sel.Columns)
	if col.Star {
		if _, found := m.cols["*"]; found {
			//u.Debugf("dupe %+v", col)
			return
		}
		m.cols["*"] = true
		m.sel.AddColumn(col)
		return
	}
	if _, found := m.cols[col.SourceField]; found {
		//u.Debugf("dupe %+v", col)
		return
	}

	//u.Infof("adding col %+v", col)
	m.cols[col.SourceField] = true
	m.sel.AddColumn(col)
}
func (m *rewriteSelect) intoProjection(sel *SqlSelect, cols Columns) error {
	if len(cols) == 0 {
		return nil
	}
	/*
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
	*/
	for i, c := range cols {
		left, _, hasLeft := c.LeftRight()
		if !hasLeft {
			// ??
		} else if hasLeft && left == m.matchSource {
			// ok
			c = c.CopyRewrite(m.matchSource)
		} else {
			//u.Warnf("no.... %v", c)
			continue
		}

		//u.Infof("as=%-15s source=%-15s exprT:%T expr=%s  star:%v", c.As, c.SourceField, c.Expr, c.Expr, c.Star)
		switch n := c.Expr.(type) {
		case *expr.IdentityNode:
			nc := NewColumn(strings.ToLower(c.SourceField))
			nc.ParentIndex = i
			nc.Expr = n
			m.addColumn(*nc)
		case *expr.FuncNode:
			// TODO:  use features.
			idents := expr.FindAllIdentities(n)
			for _, in := range idents {
				_, right, _ := in.LeftRight()
				nc := NewColumn(strings.ToLower(right))
				nc.ParentIndex = i
				nc.Expr = in
				m.addColumn(*nc)
			}
		case *expr.NumberNode, *expr.NullNode, *expr.StringNode:
			// literals
			nc := NewColumn(strings.ToLower(n.String()))
			nc.ParentIndex = i
			nc.Expr = n
			m.addColumn(*nc)
		case nil:
			if c.Star {
				nc := c.Copy()
				m.addColumn(*nc)
			} else {
				u.Warnf("unhandled column? %T  %s", n, n)
			}
		default:
			u.Warnf("unhandled column? %T  %s", n, n)
		}
	}
	return nil
}

// func (m *rewriteSelect) addIntoProjection(sel *SqlSelect, colsToAdd map[string]int) {
// 	for colName, idx := range colsToAdd {
// 		colName = strings.ToLower(colName)
// 		if colName == "*" {
// 			m.addColumn(Column{Star: true, ParentIndex: idx})
// 		} else {
// 			nc := NewColumn(colName)
// 			nc.ParentIndex = idx
// 			m.addColumn(*nc)
// 		}
// 	}
// }
func (m *rewriteSelect) rewriteWhere(stmt *SqlSelect, from *SqlSource, node expr.Node) expr.Node {
	//u.Debugf("rewrite where %s", node)
	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, hasLeft := nt.LeftRight(); hasLeft {
			//u.Debugf("rewriteWhere  from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := expr.IdentityNode{Text: right}
				nc := *NewColumn(right)
				nc.ParentIndex = -1
				m.addColumn(nc)
				return &in
			} else {
				//u.Warnf("what to do? source:%v    %v", from.alias, nt.String())
			}
		} else {
			//u.Debugf("returning original: %s", nt)
			return node
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode:
		return nt
	case *expr.BinaryNode:
		//u.Infof("binaryNode  T:%v", nt.Operator.T.String())
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			var n1, n2 expr.Node
			n1 = m.rewriteWhere(stmt, from, nt.Args[0])
			n2 = m.rewriteWhere(stmt, from, nt.Args[1])

			if n1 != nil && n2 != nil {
				return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}
			} else if n1 != nil {
				return n1
			} else if n2 != nil {
				return n2
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenGT, lex.TokenGE, lex.TokenLE, lex.TokenNE:
			var n1, n2 expr.Node
			n1 = m.rewriteWhere(stmt, from, nt.Args[0])
			n2 = m.rewriteWhere(stmt, from, nt.Args[1])
			//u.Debugf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			if n1 != nil && n2 != nil {
				return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}
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
	case *expr.FuncNode:
		// TODO:  use features.
		idents := expr.FindAllIdentities(nt)
		for _, in := range idents {
			_, right, _ := in.LeftRight()
			nc := *NewColumn(right)
			nc.ParentIndex = -1
			nc.Expr = in
			m.addColumn(nc)
		}

	default:
		u.Warnf("%T node types are not suppored yet for where rewrite", node)
	}
	//u.Warnf("nil?? %T  %s  %#v", node, node, node)
	return nil
}

func (m *rewriteSelect) joinNodesForFrom(stmt *SqlSelect, from *SqlSource, node expr.Node, depth int) expr.Node {

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
			n1 := m.joinNodesForFrom(stmt, from, nt.Args[0], depth+1)
			n2 := m.joinNodesForFrom(stmt, from, nt.Args[1], depth+1)

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
			n1 := m.joinNodesForFrom(stmt, from, nt.Args[0], depth+1)
			n2 := m.joinNodesForFrom(stmt, from, nt.Args[1], depth+1)

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

// We need to find all columns used in the given Node (where or join expression)
// to ensure we have those columns in projection.
func (m *rewriteSelect) columnsFromExpression(from *SqlSource, node expr.Node) error {
	if node == nil {
		return nil
	}
	//u.Debugf("columnsFromJoin()  T:%T  node=%q", node, node.String())
	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			if left != from.alias {
				return nil
			}
			if _, found := m.cols[strings.ToLower(right)]; found {
				return nil
			}

			newCol := Column{As: right, SourceField: right, Expr: &expr.IdentityNode{Text: right}}
			newCol.ParentIndex = -1 // if -1, we don't need in parent projection
			m.addColumn(newCol)
			//u.Warnf("added col %s idx:%d pidx:%v", right, newCol.Index, newCol.Index)
		}

	case *expr.FuncNode:
		//u.Warnf("columnsFromJoin func node: %s", nt.String())
		for _, arg := range nt.Args {
			m.columnsFromExpression(from, arg)
		}
	case *expr.BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			m.columnsFromExpression(from, nt.Args[0])
			m.columnsFromExpression(from, nt.Args[1])
		case lex.TokenEqual, lex.TokenEqualEqual:
			m.columnsFromExpression(from, nt.Args[0])
			m.columnsFromExpression(from, nt.Args[1])
		default:
			u.Warnf("un-implemented op: %v", nt.Operator)
		}
	default:
		u.LogTracef(u.INFO, "whoops")
		u.Warnf("%T node types are not suppored yet for join rewrite %s", node, from.String())
	}
	return nil
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
