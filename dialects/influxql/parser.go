package influxql

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
)

/*

Parser for InfluxDB ql

*/

// Parses string query
func Parse(query string) (*Ast, error) {
	l := lex.NewLexer(query, InfluxQlDialect)
	p := Parser{l: l, qryText: query}
	return p.parse()
}

// parser evaluateslex.Tokens
type Parser struct {
	l              *lex.Lexer
	qryText        string
	initialKeyword lex.Token
	curToken       lex.Token
}

// parse the request
func (m *Parser) parse() (*Ast, error) {

	comment := m.initialComment()
	u.Debug(comment)
	// Now, find First Keyword
	switch m.curToken.T {
	case lex.TokenSelect:
		m.initialKeyword = m.curToken
		return m.parseSelect(comment)
	default:
		return nil, fmt.Errorf("Unrecognized query, expected [SELECT] influx ql")
	}
}

func (m *Parser) initialComment() string {

	m.curToken = m.l.NextToken()
	comment := ""

	for {
		// We are going to loop until we find the first Non-Commentlex.Token
		switch m.curToken.T {
		case lex.TokenComment, lex.TokenCommentML:
			comment += m.curToken.V
		case lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd, lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
		default:
			// first non-commentlex.Token
			return comment
		}
		m.curToken = m.l.NextToken()
	}
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Parser) parseSelect(comment string) (*Ast, error) {

	selStmt := SelectStmt{}
	ast := Ast{Comments: comment, Select: &selStmt}

	// we have already parsed SELECT lex.Token to get here, so this should be first col
	m.curToken = m.l.NextToken()
	if m.curToken.T != lex.TokenStar {
		if err := m.parseColumns(&selStmt); err != nil {
			return nil, err
		}
	} else {
		// * mark as star?
		return nil, fmt.Errorf("not implemented")
	}

	// FROM - required
	if m.curToken.T != lex.TokenFrom {
		return nil, fmt.Errorf("expected From")
	}

	// table/metric
	m.curToken = m.l.NextToken()
	if m.curToken.T != lex.TokenIdentity && m.curToken.T != lex.TokenValue {
		return nil, fmt.Errorf("expected from name fot %v", m.curToken)
	} else if m.curToken.T == lex.TokenRegex {
		selStmt.From = &From{Value: m.curToken.V, Regex: true}
	}

	// Where is optional
	if err := m.parseWhere(&selStmt); err != nil {
		return nil, err
	}
	// limit is optional
	// if err := m.parseLimit(&selStmt); err != nil {
	// 	return nil, err
	// }

	// we are finished, nice!
	return &ast, nil
}

func (m *Parser) parseColumns(stmt *SelectStmt) error {
	stmt.Columns = make([]*Column, 0)
	u.Infof("cols: %d", len(stmt.Columns))
	return nil
}

func (m *Parser) parseWhere(stmt *SelectStmt) error {

	// Where is Optional, if we didn't use a where statement return
	if m.curToken.T == lex.TokenEOF || m.curToken.T == lex.TokenEOS {
		return nil
	}

	if m.curToken.T != lex.TokenWhere {
		return nil
	}

	u.Infof("wheres: %#v", stmt)
	return nil
}
