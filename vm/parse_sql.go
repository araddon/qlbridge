package vm

import (
	"errors"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser/lex"
)

// Sql is a traditional sql command (insert, update, select)
type SqlRequest struct {
	Columns Columns
	From    string
	Where   *Tree
}

func NewSqlRequest() *SqlRequest {
	req := &SqlRequest{}
	req.Columns = make(Columns, 0)
	return req
}

func (m *SqlRequest) String() string {
	return fmt.Sprintf("SELECT ", "hello")
}

// Array of Columns
type Columns []*Column

func (m *Columns) AddColumn(col *Column) {
	*m = append(*m, col)
	//u.Infof("add col: %s ct=%d", asName, len(*m))
}

func (m *Columns) String() string {
	s := make([]string, len(*m))
	for i, col := range *m {
		s[i] = col.String()
	}
	return strings.Join(s, ", ")
}

// Column
type Column struct {
	As      string
	Comment string
	Tree    *Tree
	Guard   *Tree
}

func (m *Column) String() string {
	return m.As
}

// Parses ql.Tokens and returns an request.
func ParseSql(sqlQuery string) (*SqlRequest, error) {
	l := ql.NewSqlLexer(sqlQuery)
	p := SqlParser{l: l}
	return p.parse()
}

// generic SQL parser evaluates should be sufficient for most
//  sql compatible languages
type SqlParser struct {
	l          *ql.Lexer
	firstToken ql.Token
	curToken   ql.Token
}

// parse the request
func (m *SqlParser) parse() (*SqlRequest, error) {
	m.firstToken = m.l.NextToken()
	switch m.firstToken.T {
	case ql.TokenSelect:
		return m.parseSqlSelect()
		// case ql.TokenTypeSqlUpdate:
		// 	return this.parseSqlUpdate()
	}
	return nil, fmt.Errorf("Unrecognized request type")
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *SqlParser) parseSqlSelect() (*SqlRequest, error) {
	req := NewSqlRequest()
	m.curToken = m.l.NextToken()

	// columns
	if m.curToken.T != ql.TokenStar {
		if err := m.parseColumns(req); err != nil {
			u.Error(err)
			return nil, err
		}
	} else {
		// * mark as star?  TODO
		return nil, fmt.Errorf("select * not implemented")
	}

	// from
	u.Debugf("token:  %#v", m.curToken)
	if m.curToken.T != ql.TokenFrom {
		return nil, fmt.Errorf("expected From but got: %v", m.curToken)
	} else {
		// table name
		m.curToken = m.l.NextToken()
		u.Debugf("found from?  %#v  %s", m.curToken, m.curToken.T.String())
		if m.curToken.T != ql.TokenIdentity && m.curToken.T != ql.TokenValue {
			u.Warnf("No From? %v toktype:%v", m.curToken.V, m.curToken.T.String())
			return nil, errors.New("expected from name")
		} else {
			req.From = m.curToken.V
		}
	}

	m.curToken = m.l.NextToken()
	u.Debugf("cur ql.Token: %s", m.curToken.T.String())
	if errreq := m.parseWhere(req); errreq != nil {
		return nil, errreq
	}
	// we are good
	return req, nil
}

func (m *SqlParser) parseColumns(stmt *SqlRequest) error {

	stmt.Columns = make(Columns, 0)
	var col *Column

	for {

		u.Debug(m.curToken.String())
		switch m.curToken.T {
		case ql.TokenUdfExpr:
			// we have a udf/functional expression column
			col = &Column{As: m.curToken.V, Tree: NewTree(m.l)}
			m.parseNode(col.Tree)

		case ql.TokenIdentity:
			//u.Warnf("TODO")
			col = &Column{As: m.curToken.V, Tree: NewTree(m.l)}
			m.parseNode(col.Tree)
		}
		u.Debugf("after colstart?:   %v  ", m.curToken)

		// since we can loop inside switch statement
		switch m.curToken.T {
		case ql.TokenAs:
			m.curToken = m.l.NextToken()
			u.Debug(m.curToken)
			switch m.curToken.T {
			case ql.TokenIdentity, ql.TokenValue:
				col.As = m.curToken.V
				m.curToken = m.l.NextToken()
				continue
			}
			return fmt.Errorf("expected identity but got: %v", m.curToken.String())
		case ql.TokenFrom, ql.TokenInto:
			// This indicates we have come to the End of the columns
			stmt.Columns = append(stmt.Columns, col)
			u.Debugf("Ending column ")
			return nil
		case ql.TokenIf:
			// If guard
			m.curToken = m.l.NextToken()
			col.Guard = NewTree(m.l)
			m.curToken = m.l.NextToken()
			m.parseNode(col.Guard)
			u.Debugf("after if guard?:   %v  ", m.curToken)
		case ql.TokenCommentSingleLine:
			m.curToken = m.l.NextToken()
			col.Comment = m.curToken.V
		case ql.TokenRightParenthesis:
			// loop on my friend
		case ql.TokenComma:
			stmt.Columns = append(stmt.Columns, col)
			u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			return fmt.Errorf("expected column but got: %v", m.curToken.String())
		}
		m.curToken = m.l.NextToken()
	}
	u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

// Parse an expression tree or root Node
func (m *SqlParser) parseNode(tree *Tree) error {
	//u.Debugf("parseNode: %v", m.curToken)
	tree.SetCurrent(m.curToken)
	err := tree.buildSqlTree()
	m.curToken = tree.peek()
	u.Debugf("cur token parse: root?%#v, token=%v", tree.Root, m.curToken)
	return err
}

func (m *SqlParser) parseWhere(req *SqlRequest) error {

	// Where is Optional
	if m.curToken.T == ql.TokenEOF || m.curToken.T == ql.TokenEOS {
		return nil
	}

	if m.curToken.T != ql.TokenWhere {
		return nil
	}

	m.curToken = m.l.NextToken()
	tree := NewTree(m.l)
	m.parseNode(tree)
	req.Where = tree
	return nil
}

// Parse an expression tree or root Node
func (m *SqlParser) badparseNode(tree *Tree) {
	// for {

	// 	u.Debug(m.curToken.String())
	// 	switch m.curToken.T {
	// 	case ql.TokenRightParenthesis:
	// 		// ?  do we do anything
	// 		return nil
	// 	case ql.TokenLeftParenthesis:
	// 		// ?  do we do anything
	// 		//firstToken = false
	// 	case ql.TokenFrom, ql.TokenInto, ql.TokenAs:
	// 		// This indicates we have come to the End of the expression
	// 		// and need to return to be handled by caller
	// 		return nil
	// 	case ql.TokenUdfExpr:
	// 		// Nested expression
	// 		u.Infof("udf func? %v", m.curToken.V)
	// 		tree := lql.Expr{FunCall: &lql.FunCallDesc{FuncName: m.curToken.V}}
	// 		u.Infof("inputs?  %v", expr.FunCall.Inputs)
	// 		expr.FunCall.Inputs = append(expr.FunCall.Inputs, &expr2)
	// 		m.curToken = m.l.NextToken()
	// 		m.parseExpr(&expr2)
	// 		u.Debugf("after expr?:   %v  ", m.curToken)
	// 	case ql.TokenValue, ql.TokenInteger, ql.TokenFloat:
	// 		//expr = lql.Expr{Field: &lql.FieldRef{Right: m.curToken.V}}
	// 		u.Infof("inputs?  %v", expr.FunCall)
	// 		fieldVal := m.curToken.V
	// 		expr.FunCall.Inputs = append(expr.FunCall.Inputs, &lql.Expr{Literal: &fieldVal})
	// 	case ql.TokenIdentity:
	// 		//expr = lql.Expr{Field: &lql.FieldRef{Right: m.curToken.V}}
	// 		u.Infof("inputs?  %v", expr.FunCall)
	// 		expr.FunCall.Inputs = append(expr.FunCall.Inputs, &lql.Expr{Field: &lql.FieldRef{Right: m.curToken.V}})
	// 	case ql.TokenComma:
	// 		//we scan until end of paren
	// 	default:
	// 		return fmt.Errorf("expected column but got: %v", m.curToken.String())
	// 	}
	// 	m.curToken = m.l.NextToken()
	// }
	// return nil
}
