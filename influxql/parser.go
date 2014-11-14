package influxql

import (
	"fmt"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser"
)

/*

Parser for InfluxDB ql

*/

// Parses string query
func Parse(query string) (*Ast, error) {
	l := ql.NewLexer(query, InfluxQlDialect)
	p := Parser{l: l, ql: query}
	return p.parse()
}

// parser evaluatesql.Tokens
type Parser struct {
	l              *ql.Lexer
	ql             string
	initialKeyword ql.Token
	curToken       ql.Token
}

// parse the request
func (m *Parser) parse() (*Ast, error) {

	comment := m.initialComment()
	u.Debug(comment)
	// Now, find First Keyword
	switch m.curToken.T {
	case ql.TokenSelect:
		m.initialKeyword = m.curToken
		return m.parseSelect(comment)
	default:
		return nil, fmt.Errorf("Unrecognized query, expected [SELECT] influx ql")
	}
	u.Warnf("Whoops, that didn't work: \n%v \n\t%v", m.curToken, m.ql)
	return nil, fmt.Errorf("Unkwown error on request")
}

func (m *Parser) initialComment() string {

	m.curToken = m.l.NextToken()
	comment := ""

	for {
		// We are going to loop until we find the first Non-Commentql.Token
		switch m.curToken.T {
		case ql.TokenComment, ql.TokenCommentML:
			comment += m.curToken.V
		case ql.TokenCommentStart, ql.TokenCommentHash, ql.TokenCommentEnd, ql.TokenCommentSingleLine, ql.TokenCommentSlashes:
			// skip, currently ignore these
		default:
			// first non-commentql.Token
			return comment
		}
		m.curToken = m.l.NextToken()
	}
	return comment
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Parser) parseSelect(comment string) (*Ast, error) {

	selast := SelectStmt{}
	ast := Ast{Comments: comment, Select: &selast}
	//u.Infof("Comment:   %v", comment)

	// we have already parsed SELECTql.Token to get here, so this should be first col
	m.curToken = m.l.NextToken()
	//u.Debug("FirstToken: ", m.curToken)
	if m.curToken.T != ql.TokenStar {
		if err := m.parseColumns(&selast); err != nil {
			u.Error(err)
			return nil, err
		}
		//u.Infof("resturned from cols: %v", len(selast.Columns))
	} else {
		// * mark as star?  TODO
		return nil, fmt.Errorf("not implemented")
	}

	// FROM - required
	//u.Debugf("token:  %s", m.curToken)
	if m.curToken.T != ql.TokenFrom {
		return nil, fmt.Errorf("expected From")
	} else {
		// table/metric
		m.curToken = m.l.NextToken()
		//u.Debugf("found from? %s", m.curToken)
		if m.curToken.T != ql.TokenIdentity && m.curToken.T != ql.TokenValue {
			//u.Warnf("No From? %v toktype:%v", m.curToken.V, m.curToken.T.String())
			return nil, fmt.Errorf("expected from name")
		} else if m.curToken.T == ql.TokenRegex {
			selast.From = &From{Value: m.curToken.V, Regex: true}
		}
	}

	// Where is optional
	if err := m.parseWhere(&selast); err != nil {
		return nil, err
	}
	// limit is optional
	// if err := m.parseLimit(&selast); err != nil {
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
	if m.curToken.T == ql.TokenEOF || m.curToken.T == ql.TokenEOS {
		return nil
	}

	if m.curToken.T != ql.TokenWhere {
		return nil
	}

	u.Infof("wheres: %#v", stmt)
	return nil
}
