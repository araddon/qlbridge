package expr

import (
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
)

// Parses Tokens and returns an request.
func ParseFilterQL(filter string) (*FilterStatement, error) {
	l := lex.NewFilterQLLexer(filter)
	m := FilterQLParser{l: l, FilterTokenPager: NewFilterTokenPager(l), buildVm: false}
	return m.parse()
}
func ParseFilterQLVm(filter string) (*FilterStatement, error) {
	l := lex.NewFilterQLLexer(filter)
	m := FilterQLParser{l: l, FilterTokenPager: NewFilterTokenPager(l), buildVm: true}
	return m.parse()
}

type FilterStatement struct {
	Keyword lex.TokenType // Keyword SELECT or FILTER
	Raw     string        // full original raw statement
	Filter  *Filters      // A top level filter
	From    string        // From is optional
	Limit   int           // Limit
	Offset  int           // Offset
	Alias   string        // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
	With    u.JsonHelper  // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
}

func NewFilterStatement() *FilterStatement {
	req := &FilterStatement{}
	return req
}

type Filters struct {
	Op      lex.TokenType // OR, AND
	Filters []*FilterExpr
}

type FilterExpr struct {
	// Exactly one of these will be non-nil
	Include string   // name of foregin named alias filter to embed
	Expr    Node     // Node might be nil in which case must have filter
	Filter  *Filters // might be nil, must have expr
}

func NewFilters(tok lex.Token) *Filters {
	return &Filters{Op: tok.T, Filters: make([]*FilterExpr, 0)}
}
func NewFilterExpr() *FilterExpr {
	return &FilterExpr{}
}

// TokenPager is responsible for determining end of current clause
//   An interface used to allow Parser to be neutral to dialect
type FilterTokenPager struct {
	*LexTokenPager
	lastKw lex.TokenType
}

func NewFilterTokenPager(lex *lex.Lexer) *FilterTokenPager {
	pager := NewLexTokenPager(lex)
	return &FilterTokenPager{LexTokenPager: pager}
}

func (m *FilterTokenPager) IsEnd() bool {
	return m.lex.IsEnd()
}
func (m *FilterTokenPager) ClauseEnd() bool {
	tok := m.Cur()
	switch tok.T {
	// List of possible tokens that would indicate a end to the current clause
	case lex.TokenEOF, lex.TokenEOS, lex.TokenLimit, lex.TokenWith, lex.TokenAlias:
		return true
	}
	return false
}

// generic Filter QL parser
type FilterQLParser struct {
	buildVm bool
	l       *lex.Lexer
	comment string
	*FilterTokenPager
	firstToken lex.Token
}

// parse the request
func (m *FilterQLParser) parse() (*FilterStatement, error) {
	m.comment = m.initialComment()
	m.firstToken = m.Cur()
	switch m.firstToken.T {
	case lex.TokenFilter:
		return m.parseFilter()
	case lex.TokenSelect:
		return m.parseSelect()
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
func (m *FilterQLParser) parseSelect() (*FilterStatement, error) {

	req := NewFilterStatement()
	req.Raw = m.l.RawInput()

	m.Next() // Consume the SELECT
	if m.Cur().T != lex.TokenStar && m.Cur().T != lex.TokenMultiply {
		u.Warnf("token? %v", m.Cur())
		return nil, fmt.Errorf("Must use SELECT * currently %s", req.Raw)
	}
	m.Next() // Consume   *

	// OPTIONAL From clause
	if m.Cur().T == lex.TokenFrom {
		m.Next()
		if m.Cur().T == lex.TokenIdentity || m.Cur().T == lex.TokenTable {
			req.From = m.Cur().V
			m.Next()
		}
	}

	if m.Cur().T != lex.TokenWhere {
		return nil, fmt.Errorf("Must use SELECT * FROM [table] WHERE: %s", req.Raw)
	}
	req.Keyword = m.Cur().T
	m.Next() // Consume WHERE

	// one top level filter which may be nested
	if err := m.parseWhereExpr(req); err != nil {
		u.Debug(err)
		return nil, err
	}

	// LIMIT
	if err := m.parseLimit(req); err != nil {
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

// First keyword was FILTER, so use the FILTER parser rule-set
func (m *FilterQLParser) parseFilter() (*FilterStatement, error) {

	req := NewFilterStatement()
	req.Raw = m.l.RawInput()
	req.Keyword = m.Cur().T
	m.Next() // Consume (FILTER | WHERE )

	// OPTIONAL From clause
	if m.Cur().T == lex.TokenFrom {
		req.From = m.Cur().V
		m.Next()
	}

	// one top level filter which may be nested
	filter, err := m.parseFilters()
	if err != nil {
		u.Warnf("Could not parse filters %v", err)
		return nil, err
	}
	req.Filter = filter

	// LIMIT
	if err := m.parseLimit(req); err != nil {
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

func (m *FilterQLParser) parseWhereExpr(req *FilterStatement) error {
	tree := NewTree(m.FilterTokenPager)
	if err := m.parseNode(tree); err != nil {
		u.Errorf("could not parse: %v", err)
		return err
	}

	fe := FilterExpr{Expr: tree.Root}
	filters := Filters{Op: lex.TokenAnd, Filters: []*FilterExpr{&fe}}
	req.Filter = &filters
	return nil
}

func (m *FilterQLParser) parseFilters() (*Filters, error) {

	var fe *FilterExpr
	var filters *Filters

	switch m.Cur().T {
	case lex.TokenLogicAnd, lex.TokenAnd, lex.TokenOr, lex.TokenLogicOr:
		// fine, we have nested parent expression (AND | OR)
		filters = NewFilters(m.Cur())
		m.Next()
	default:
		//return nil, fmt.Errorf("Expected ( AND | OR ) but got %v", m.Cur())
		filters = NewFilters(lex.Token{T: lex.TokenLogicAnd})
	}

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

		case lex.TokenInclude:
			// embed/include a named filter
			m.Next()
			if m.Cur().T != lex.TokenIdentity {
				return nil, fmt.Errorf("Expected identity for Include but got %v", m.Cur())
			}
			fe = NewFilterExpr()
			fe.Include = m.Cur().V
			m.Next()
			filters.Filters = append(filters.Filters, fe)
			continue

		case lex.TokenLeftParenthesis:
			m.Next()
			continue
		case lex.TokenUdfExpr:
			// we have a udf/functional expression filter
			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.FilterTokenPager)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			fe.Expr = tree.Root

		case lex.TokenNegate, lex.TokenIdentity, lex.TokenLike, lex.TokenExists, lex.TokenBetween,
			lex.TokenIN, lex.TokenValue:
			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.FilterTokenPager)
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
			// should we save this into filter?
		case lex.TokenRightParenthesis:
			// end of this filter expression
			m.Next()
			return filters, nil
		case lex.TokenComma:
			// keep looping, looking for more expressions
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

func (m *FilterQLParser) parseLimit(req *FilterStatement) error {
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

func (m *FilterQLParser) parseAlias(req *FilterStatement) error {
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

func (m *FilterQLParser) isEnd() bool {
	return m.IsEnd()
}
