package expr

import (
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
)

// Parses Tokens and returns an request.
func ParseFilterQL(filter string) (*FilterQL, error) {
	l := lex.NewFilterQLLexer(filter)
	m := FilterQLParser{l: l, FilterQLTokenPager: NewFilterQLTokenPager(l), buildVm: false}
	return m.parse()
}
func ParseFilterQLVm(filter string) (*FilterQL, error) {
	l := lex.NewFilterQLLexer(filter)
	m := FilterQLParser{l: l, FilterQLTokenPager: NewFilterQLTokenPager(l), buildVm: true}
	return m.parse()
}

type FilterQL struct {
	Pos
	Raw    string       // full original raw statement
	Filter *Filters     // A top level filter
	Limit  int          // Limit
	Offset int          // Offset
	Alias  string       // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
	With   u.JsonHelper // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
}

func NewFilterQL() *FilterQL {
	req := &FilterQL{}
	return req
}

type Filters struct {
	Pos
	Op      lex.TokenType // OR, AND
	Filters []*FilterExpr
}

type FilterExpr struct {
	Pos
	SourceField string   // field name of underlying field
	Expr        Node     // Node might be nil in which case must have filter
	Filter      *Filters // might be nil, must have expr
}

func NewFilters(tok lex.Token) *Filters {
	return &Filters{Op: tok.T, Filters: make([]*FilterExpr, 0)}
}
func NewFilterExpr() *FilterExpr {
	return &FilterExpr{}
}

// TokenPager is responsible for determining end of
// current tree (column, etc)
type FilterQLTokenPager struct {
	*LexTokenPager
	lastKw lex.TokenType
}

func NewFilterQLTokenPager(lex *lex.Lexer) *FilterQLTokenPager {
	pager := NewLexTokenPager(lex)
	return &FilterQLTokenPager{LexTokenPager: pager}
}

func (m *FilterQLTokenPager) IsEnd() bool {
	return m.lex.IsEnd()
}
func (m *FilterQLTokenPager) ClauseEnd() bool {
	tok := m.Cur()
	//u.Debugf("IsEnd()? tok:  %v", tok)
	switch tok.T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenHaving, lex.TokenComma,
		lex.TokenIf, lex.TokenAs, lex.TokenLimit, lex.TokenSelect:
		return true
	}
	return false
}

// generic Filter QL parser
type FilterQLParser struct {
	buildVm bool
	l       *lex.Lexer
	comment string
	*FilterQLTokenPager
	firstToken lex.Token
}

// parse the request
func (m *FilterQLParser) parse() (*FilterQL, error) {
	m.comment = m.initialComment()
	m.firstToken = m.Cur()
	switch m.firstToken.T {
	case lex.TokenFilter:
		return m.parseFilter()
	}
	u.Warnf("Could not parse?  %v   peek=%v", m.l.RawInput(), m.l.PeekX(40))
	return nil, fmt.Errorf("Unrecognized request type: %v", m.l.PeekWord())
}

func (m *FilterQLParser) initialComment() string {

	comment := ""

	for {
		// We are going to loop until we find the first Non-Comment Token
		switch m.Cur().T {
		case lex.TokenComment, lex.TokenCommentML:
			comment += m.Cur().V
		case lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd, lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
		default:
			// first non-comment token
			return comment
		}
		m.Next()
	}
	return comment
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *FilterQLParser) parseFilter() (*FilterQL, error) {

	req := NewFilterQL()
	req.Raw = m.l.RawInput()
	m.Next() // Consume FILTER

	// one top level filter which may be nested
	filter, err := m.parseFilters()
	if err != nil {
		u.Debug(err)
		return nil, err
	}
	req.Filter = filter

	// LIMIT
	if err := m.parseLimit(req); err != nil {
		return nil, err
	}

	// WITH
	if err := m.parseWith(req); err != nil {
		return nil, err
	}

	// ALIAS
	if err := m.parseAlias(req); err != nil {
		return nil, err
	}

	if m.Cur().T == lex.TokenEOF || m.Cur().T == lex.TokenEOS || m.Cur().T == lex.TokenRightParenthesis {

		// if err := req.Finalize(); err != nil {
		// 	u.Errorf("Could not finalize: %v", err)
		// 	return nil, err
		// }

		// we are good
		return req, nil
	}

	u.Warnf("Could not complete parsing, return error: %v %v", m.Cur(), m.l.PeekWord())
	return nil, fmt.Errorf("Did not complete parsing input: %v", m.LexTokenPager.Cur().V)
}

func (m *FilterQLParser) parseFilters() (*Filters, error) {

	switch m.Cur().T {
	case lex.TokenLogicAnd, lex.TokenAnd, lex.TokenOr, lex.TokenLogicOr:
		// fine
	default:
		return nil, fmt.Errorf("Expected ( AND | OR ) but got %V", m.Cur())
	}

	var fe *FilterExpr
	filters := NewFilters(m.Cur())
	m.Next()

	for {

		//u.Debug(m.Cur())
		switch m.Cur().T {
		case lex.TokenAnd, lex.TokenOr:
			filters, err := m.parseFilters()
			if err != nil {
				return nil, err
			}
			fe = NewFilterExpr()
			fe.Filter = filters
			filters.Filters = append(filters.Filters, fe)

		case lex.TokenLeftParenthesis:
			m.Next()
			continue
		case lex.TokenUdfExpr:
			// we have a udf/functional expression filter
			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.FilterQLTokenPager)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			fe.Expr = tree.Root
			fe.SourceField = FindIdentityField(fe.Expr)

		case lex.TokenIdentity:
			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.FilterQLTokenPager)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			fe.Expr = tree.Root

		case lex.TokenValue:
			// Value Literal
			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.FilterQLTokenPager)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			fe.Expr = tree.Root

		}
		//u.Debugf("after filter start?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			return filters, nil
		case lex.TokenCommentSingleLine, lex.TokenCommentStart, lex.TokenCommentSlashes, lex.TokenComment,
			lex.TokenCommentEnd:
			// tbd
		case lex.TokenRightParenthesis:
			// end of this filter expression
			m.Next()
			//u.Warnf("ending this clause")
			return filters, nil
		case lex.TokenComma:
			// we have already added this expression to list
		default:
			return nil, fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("filters: %d", len(filters.Filters))
	return filters, nil
}

// Parse an expression tree or root Node
func (m *FilterQLParser) parseNode(tree *Tree) error {
	//u.Debugf("cur token parse: token=%v", m.Cur())
	err := tree.BuildTree(m.buildVm)
	if err != nil {
		u.Errorf("error: %v", err)
	}
	return err
}

func (m *FilterQLParser) parseLimit(req *FilterQL) error {
	if m.Cur().T != lex.TokenLimit {
		return nil
	}
	m.Next()
	if m.Cur().T != lex.TokenInteger {
		return fmt.Errorf("Limit must be an integer %v %v", m.Cur().T, m.Cur().V)
	}
	iv, err := strconv.Atoi(m.Cur().V)
	m.Next()
	if err != nil {
		return fmt.Errorf("Could not convert limit to integer %v", m.Cur().V)
	}
	req.Limit = int(iv)
	return nil
}

func (m *FilterQLParser) parseAlias(req *FilterQL) error {
	if m.Cur().T != lex.TokenAlias {
		return nil
	}
	m.Next()
	if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
		return fmt.Errorf("Expected identity but got: %v", m.Cur().T.String())
	}
	req.Alias = strings.ToLower(m.Cur().V)
	m.Next()
	return nil
}

func (m *FilterQLParser) parseWith(req *FilterQL) error {
	if m.Cur().T != lex.TokenWith {
		return nil
	}
	m.Next()
	switch m.Cur().T {
	case lex.TokenLeftBrace: // {
		jh := make(u.JsonHelper)
		if err := parseJsonObject(m.FilterQLTokenPager, jh); err != nil {
			return err
		}
		req.With = jh
	default:
		return fmt.Errorf("Expected json { but got: %v", m.Cur().T.String())
	}
	return nil
}

func (m *FilterQLParser) isEnd() bool {
	return m.IsEnd()
}
