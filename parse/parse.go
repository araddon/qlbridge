package qlbridge

import (
	"errors"
	"fmt"
	u "github.com/araddon/gou"
	ql "github.com/araddon/qlbridge/lex"
)

var _ = u.EMPTY

// Parses tokens and returns an request.
func ParseSql(sqlQuery string) (QlRequest, error) {
	l := ql.NewSqlLexer(sqlQuery)
	p := Sqlbridge{l: l}
	return p.parse()
}

// generic SQL parser evaluates should be sufficient for most
//  sql compatible languages
type Sqlbridge struct {
	l          *ql.Lexer
	firstToken ql.Token
	curToken   ql.Token
}

// parse the request
func (m *Sqlbridge) parse() (QlRequest, error) {
	m.firstToken = m.l.NextToken()
	switch m.firstToken.T {
	case ql.TokenSelect:
		return m.parseSqlSelect()
		// case tokenTypeSqlUpdate:
		// 	return this.parseSqlUpdate()
	}
	return nil, fmt.Errorf("Unrecognized request type")
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Sqlbridge) parseSqlSelect() (QlRequest, error) {
	req := NewSqlRequest()
	m.curToken = m.l.NextToken()

	u.Debug(m.curToken)
	if m.curToken.T != ql.TokenStar {
		if err := m.parseColumns(&req.Columns); err != nil {
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

func (m *Sqlbridge) parseColumns(cols *Columns) error {
	nextIsColumn := true
	for {
		if nextIsColumn {
			switch m.curToken.T {
			case ql.TokenUdfExpr:

			case ql.TokenIdentity:
			default:
				return fmt.Errorf("expected column name")
			}
			nextIsColumn = false
			cols.AddColumn(m.curToken.V)
			u.Infof("Col ct=%v", len(*cols))
		} else {
			if m.curToken.T != ql.TokenComma {
				break
			}
			nextIsColumn = true
		}
		m.curToken = m.l.NextToken()
		u.Debug(m.curToken)
	}
	u.Infof("cols: %d", len(*cols))
	return nil
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
	nextIsWhereCol := true
	for {
		if nextIsWhereCol {
			switch m.curToken.T {
			case ql.TokenUdfExpr:
			case ql.TokenEqual, ql.TokenLE:
				u.Info("is equal/le")
			case ql.TokenIdentity:
			default:
				return fmt.Errorf("expected where field name %v", m.curToken.T.String())
			}
			nextIsWhereCol = false
			req.AddWhere(&m.curToken)
		} else {
			if m.curToken.T != ql.TokenComma {
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
func (m *Sqlbridge) parseExprOrValue(tok ql.Token, nested int) error {
	nextIsColumn := true
	for {
		if nextIsColumn {
			switch tok.T {
			case ql.TokenUdfExpr:

			case ql.TokenIdentity:
			default:
				return fmt.Errorf("expected column name")
			}
			nextIsColumn = false
		} else {
			if tok.T != ql.TokenComma {
				break
			}
			nextIsColumn = true
		}
		tok = m.l.NextToken()
	}
	return nil
}
