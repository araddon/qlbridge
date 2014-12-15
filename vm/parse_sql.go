package vm

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlbridge/lex"
)

type SqlTokenPager struct {
	token     [1]ql.Token // one-token lookahead for parser
	peekCount int
	lex       *ql.Lexer
	end       ql.TokenType
}

func NewSqlTokenPager(lex *ql.Lexer) *SqlTokenPager {
	return &SqlTokenPager{
		lex: lex,
	}
}

func (m *SqlTokenPager) SetCurrent(tok ql.Token) {
	m.peekCount = 1
	m.token[0] = tok
}

// next returns the next token.
func (m *SqlTokenPager) Next() ql.Token {
	if m.peekCount > 0 {
		m.peekCount--
	} else {
		m.token[0] = m.lex.NextToken()
	}
	return m.token[m.peekCount]
}
func (m *SqlTokenPager) Last() ql.TokenType {
	return m.end
}
func (m *SqlTokenPager) IsEnd() bool {
	tok := m.Peek()
	u.Debugf("tok:  %v", tok)
	switch tok.T {
	case ql.TokenEOF, ql.TokenEOS, ql.TokenFrom, ql.TokenComma, ql.TokenIf,
		ql.TokenAs:
		return true
	}
	return false
}

// backup backs the input stream up one token.
func (m *SqlTokenPager) Backup() {
	if m.peekCount > 0 {
		//u.Warnf("PeekCount?  %v: %v", m.peekCount, m.token)
		return
	}
	m.peekCount++
}

// peek returns but does not consume the next token.
func (m *SqlTokenPager) Peek() ql.Token {
	if m.peekCount > 0 {
		//u.Infof("peek:  %v: len=%v", m.peekCount, len(m.token))
		return m.token[m.peekCount-1]
	}
	m.peekCount = 1
	m.token[0] = m.lex.NextToken()
	//u.Infof("peek:  %v: len=%v %v", m.peekCount, len(m.token), m.token[0])
	return m.token[0]
}

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
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", m.Columns, m.From))
	if m.Where != nil {
		buf.WriteString(fmt.Sprintf(" WHERE %s ", m.Where.String()))
	}
	return buf.String()
}

// Array of Columns
type Columns []*Column

func (m *Columns) AddColumn(col *Column) {
	*m = append(*m, col)
	//u.Infof("add col: %s ct=%d", asName, len(*m))
}

func (m *Columns) String() string {

	colCt := len(*m)
	if colCt == 1 {
		return (*m)[0].String()
	} else if colCt == 0 {
		return ""
	}

	s := make([]string, len(*m))
	for i, col := range *m {
		s[i] = col.String()
	}

	return strings.Join(s, ", ")
}

// Column represents the Column as expressed in a [SELECT]
// expression
type Column struct {
	As      string
	Comment string
	Tree    *Tree
	Guard   *Tree // If
}

func (m *Column) Key() string {
	return m.As
}
func (m *Column) String() string {
	return m.As
}

// Parses ql.Tokens and returns an request.
func ParseSql(sqlQuery string) (*SqlRequest, error) {
	l := ql.NewSqlLexer(sqlQuery)
	p := Sqlbridge{l: l, pager: NewSqlTokenPager(l)}
	return p.parse()
}

// generic SQL parser evaluates should be sufficient for most
//  sql compatible languages
type Sqlbridge struct {
	l          *ql.Lexer
	pager      *SqlTokenPager
	firstToken ql.Token
	curToken   ql.Token
}

// parse the request
func (m *Sqlbridge) parse() (*SqlRequest, error) {
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
func (m *Sqlbridge) parseSqlSelect() (*SqlRequest, error) {
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

func (m *Sqlbridge) parseColumns(stmt *SqlRequest) error {

	stmt.Columns = make(Columns, 0)
	var col *Column

	for {

		u.Debug(m.curToken.String())
		switch m.curToken.T {
		case ql.TokenUdfExpr:
			// we have a udf/functional expression column
			col = &Column{As: m.curToken.V, Tree: NewTree(m.pager)}
			m.parseNode(col.Tree)

		case ql.TokenIdentity:
			//u.Warnf("TODO")
			col = &Column{As: m.curToken.V, Tree: NewTree(m.pager)}
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
			u.Infof("if guard: %v", m.curToken)
			col.Guard = NewTree(m.pager)
			//m.curToken = m.l.NextToken()
			//u.Infof("if guard 2: %v", m.curToken)
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
func (m *Sqlbridge) parseNode(tree *Tree) error {
	u.Debugf("parseNode: %v", m.curToken)
	m.pager.SetCurrent(m.curToken)
	err := tree.BuildTree()
	m.curToken = tree.Peek()
	u.Debugf("cur token parse: root?%#v, token=%v", tree.Root, m.curToken)
	return err
}

func (m *Sqlbridge) parseWhere(req *SqlRequest) error {

	// Where is Optional
	if m.curToken.T == ql.TokenEOF || m.curToken.T == ql.TokenEOS {
		return nil
	}

	if m.curToken.T != ql.TokenWhere {
		return nil
	}

	m.curToken = m.l.NextToken()
	tree := NewTree(m.pager)
	m.parseNode(tree)
	req.Where = tree
	return nil
}
