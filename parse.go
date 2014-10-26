package qlparse

import (
	"errors"
	"fmt"
	u "github.com/araddon/gou"
)

var _ = u.EMPTY

// parser evaluates tokens
type Parser struct {
	l          *Lexer
	firstToken Token
	curToken   Token
}

// parse the request
func (m *Parser) parse() (QlRequest, error) {
	m.firstToken = m.l.NextToken()
	switch m.firstToken.T {
	case TokenSelect:
		return m.parseSqlSelect()
		// case tokenTypeSqlUpdate:
		// 	return this.parseSqlUpdate()
	}
	return nil, fmt.Errorf("Unrecognized request type")
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Parser) parseSqlSelect() (QlRequest, error) {
	req := NewSqlRequest()
	m.curToken = m.l.NextToken()

	u.Debug(m.curToken)
	if m.curToken.T != TokenStar {
		if err := m.parseColumns(req.Columns); err != nil {
			u.Error(err)
			return nil, err
		}
	} else {
		// * mark as star?  TODO
		return nil, errors.New("not implemented")
	}
	// from
	u.Debugf("token:  %#v", m.curToken)
	if m.curToken.T != TokenFrom {
		return nil, errors.New("expected From")
	} else {
		// table name
		m.curToken = m.l.NextToken()
		u.Debugf("found from?  %#v  %s", m.curToken, m.curToken.T.String())
		if m.curToken.T != TokenTable {
			return nil, errors.New("expected from name")
		} else {
			req.FromTable = m.curToken.V
		}
	}

	m.curToken = m.l.NextToken()
	u.Debugf("cur token: %s", m.curToken.T.String())
	if errreq := m.parseWhere(req); errreq != nil {
		return nil, errreq
	}
	// we are good
	return req, nil
}

func (m *Parser) parseColumns(cols *Columns) error {
	nextIsColumn := true
	for {
		if nextIsColumn {
			switch m.curToken.T {
			case TokenUdfExpr:

			case TokenColumn:
			default:
				return fmt.Errorf("expected column name")
			}
			nextIsColumn = false
			cols.AddColumn(m.curToken.V)
		} else {
			if m.curToken.T != TokenComma {
				break
			}
			nextIsColumn = true
		}
		m.curToken = m.l.NextToken()
		u.Debug(m.curToken)
	}
	u.Infof("cols: %d", len(cols.Cols))
	return nil
}

func (m *Parser) parseWhere(req *SqlRequest) error {

	if m.curToken.T != TokenWhere {
		return errors.New("expected Where")
	}

	m.curToken = m.l.NextToken()
	nextIsWhereCol := true
	for {
		if nextIsWhereCol {
			switch m.curToken.T {
			case TokenUdfExpr:
			case TokenEqual, TokenLE:
				u.Info("is equal/le")
			case TokenColumn:
			default:
				return fmt.Errorf("expected where field name %v", m.curToken.T.String())
			}
			nextIsWhereCol = false
			req.AddWhere(&m.curToken)
		} else {
			if m.curToken.T != TokenComma {
				break
			}
			nextIsWhereCol = true
		}
		m.curToken = m.l.NextToken()
		u.Debug(m.curToken)
	}
	u.Infof("wheres: %d", len(req.Where))
	return nil
}

// Parse either an Expression, or value
func (m *Parser) parseExprOrValue(tok Token, nested int) error {
	nextIsColumn := true
	for {
		if nextIsColumn {
			switch tok.T {
			case TokenUdfExpr:

			case TokenColumn:
			default:
				return fmt.Errorf("expected column name")
			}
			nextIsColumn = false
		} else {
			if tok.T != TokenComma {
				break
			}
			nextIsColumn = true
		}
		tok = m.l.NextToken()
	}
	return nil
}

// Parses tokens and returns an request.
func Parse(cmd string) (QlRequest, error) {
	l := NewLexer(cmd)
	p := Parser{l: l}
	return p.parse()
}
