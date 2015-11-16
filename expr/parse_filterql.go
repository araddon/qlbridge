package expr

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
)

// Parses Tokens and returns an request.
func ParseFilterQL(filter string) (*FilterStatement, error) {
	l := lex.NewFilterQLLexer(filter)
	m := filterQLParser{l: l, filterTokenPager: newFilterTokenPager(l), buildVm: false}
	return m.parse()
}
func ParseFilterQLVm(filter string) (*FilterStatement, error) {
	l := lex.NewFilterQLLexer(filter)
	m := filterQLParser{l: l, filterTokenPager: newFilterTokenPager(l), buildVm: true}
	return m.parse()
}

type (
	// Filter Statement is a statement of type = Filter
	FilterStatement struct {
		Keyword lex.TokenType // Keyword SELECT or FILTER
		Raw     string        // full original raw statement
		Filter  *Filters      // A top level filter
		From    string        // From is optional
		Limit   int           // Limit
		Offset  int           // Offset
		Alias   string        // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With    u.JsonHelper  // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
	}
	// A list of Filter Expressions
	Filters struct {
		Negate  bool          // Should we negate this response?
		Op      lex.TokenType // OR, AND
		Filters []*FilterExpr
	}
	// Single Filter expression
	FilterExpr struct {
		IncludeFilter *FilterStatement // Memoized Include

		// Do we negate this entire Filter?  Default = false (ie, don't negate)
		Negate bool

		// Exactly one of these will be non-nil
		Include  string   // name of foreign named alias filter to embed
		Expr     Node     // Node might be nil in which case must have filter
		Filter   *Filters // might be nil, must have expr
		MatchAll bool     // * = match all
	}

	// TokenPager is responsible for determining end of current clause
	//   An interface used to allow Parser to be neutral to dialect
	filterTokenPager struct {
		*LexTokenPager
		lastKw lex.TokenType
	}

	// Parser, stateful representation of parser
	filterQLParser struct {
		buildVm bool
		l       *lex.Lexer
		comment string
		*filterTokenPager
		firstToken lex.Token
	}
)

func NewFilterStatement() *FilterStatement {
	req := &FilterStatement{}
	return req
}

func (m *FilterStatement) writeBuf(buf *bytes.Buffer) {

	switch m.Keyword {
	case lex.TokenSelect:
		buf.WriteString("SELECT ")
	case lex.TokenFilter:
		buf.WriteString("FILTER ")
	}

	m.Filter.writeBuf(buf)

	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Offset > 0 {
		buf.WriteString(fmt.Sprintf(" OFFSET %d", m.Offset))
	}
	if m.Alias != "" {
		buf.WriteString(fmt.Sprintf(" ALIAS %s", m.Alias))
	}
}

// String representation of FilterStatement
func (m *FilterStatement) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}

func NewFilters(tok lex.Token) *Filters {
	return &Filters{Op: tok.T, Filters: make([]*FilterExpr, 0)}
}

// String representation of Filters
func (m *Filters) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}

func (m *Filters) writeBuf(buf *bytes.Buffer) {

	if m.Negate {
		buf.WriteString("NOT ")
	}
	switch m.Op {
	case lex.TokenAnd, lex.TokenLogicAnd:
		buf.WriteString("AND")
	case lex.TokenOr, lex.TokenLogicOr:
		buf.WriteString("OR")
	}

	buf.WriteString(" ( ")

	for i, innerf := range m.Filters {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(innerf.String())
	}
	buf.WriteString(" )")
}

func NewFilterExpr() *FilterExpr {
	return &FilterExpr{}
}

// String representation of FilterExpression for diagnostic purposes.
func (fe *FilterExpr) String() string {
	prefix := ""
	if fe.Negate {
		prefix = "NOT "
	}
	switch {
	case fe.Include != "":
		return fmt.Sprintf("%sINCLUDE %s", prefix, fe.Include)
	case fe.Expr != nil:
		return fmt.Sprintf("%s%s", prefix, fe.Expr.String())
	case fe.Filter != nil:
		return fmt.Sprintf("%s%s", prefix, fe.Filter.String())
	case fe.MatchAll == true:
		return "*"
	default:
		return "<invalid expression>"
	}
}

func newFilterTokenPager(lex *lex.Lexer) *filterTokenPager {
	pager := NewLexTokenPager(lex)
	return &filterTokenPager{LexTokenPager: pager}
}

func (m *filterTokenPager) IsEnd() bool {
	return m.lex.IsEnd()
}
func (m *filterTokenPager) ClauseEnd() bool {
	tok := m.Cur()
	switch tok.T {
	// List of possible tokens that would indicate a end to the current clause
	case lex.TokenEOF, lex.TokenEOS, lex.TokenLimit, lex.TokenWith, lex.TokenAlias:
		return true
	}
	return false
}

// parse the request
func (m *filterQLParser) parse() (*FilterStatement, error) {
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

func (m *filterQLParser) initialComment() string {

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
func (m *filterQLParser) parseSelect() (*FilterStatement, error) {

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
func (m *filterQLParser) parseFilter() (*FilterStatement, error) {

	req := NewFilterStatement()
	req.Raw = m.l.RawInput()
	req.Keyword = m.Cur().T
	m.Next() // Consume (FILTER | WHERE )

	// OPTIONAL From clause
	if m.Cur().T == lex.TokenFrom {
		req.From = m.Cur().V
		m.Next()
	}

	//u.Warnf("starting filter %s", req.Raw)
	// one top level filter which may be nested
	filter, err := m.parseFilters(0)
	if err != nil {
		u.Warnf("Could not parse filters %q err=%v", req.Raw, err)
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

func (m *filterQLParser) parseWhereExpr(req *FilterStatement) error {
	tree := NewTree(m.filterTokenPager)
	if err := m.parseNode(tree); err != nil {
		u.Errorf("could not parse: %v", err)
		return err
	}

	fe := FilterExpr{Expr: tree.Root}
	filters := Filters{Op: lex.TokenAnd, Filters: []*FilterExpr{&fe}}
	req.Filter = &filters
	return nil
}

func (m *filterQLParser) parseFilters(depth int) (*Filters, error) {

	var fe *FilterExpr
	var filters *Filters

	negate := false
	if m.Cur().T == lex.TokenNegate {
		m.Next()
		negate = true
	}

	//u.Infof("%d parse %v peek=%q", depth, m.Cur(), m.l.PeekX(20))
	switch m.Cur().T {
	case lex.TokenLogicAnd, lex.TokenAnd, lex.TokenOr, lex.TokenLogicOr:
		// we have nested parent expression (AND | OR)
		filters = NewFilters(m.Cur())
		//u.Infof("starting expr %v for token %v", filters.String(), m.Cur())
		m.Next()
	default:
		// By not explicitly declaring, we assume AND and wrap children
		filters = NewFilters(lex.Token{T: lex.TokenLogicAnd})
	}
	filters.Negate = negate
	//u.Debugf("filter? p:%p  negate? %v ", filters, negate)

	for {

		negate = false
		switch m.Cur().T {
		case lex.TokenNegate:
			negate = true
			m.Next()
			//u.Debugf("setting negate %v   %q", m.Cur(), m.l.PeekX(10))
		}

		//u.Debugf("start loop %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenStar, lex.TokenMultiply:
			fe = NewFilterExpr()
			fe.MatchAll = true
			m.Next()
			filters.Filters = append(filters.Filters, fe)

		case lex.TokenAnd, lex.TokenOr, lex.TokenLogicAnd, lex.TokenLogicOr:

			innerf, err := m.parseFilters(depth + 1)
			if err != nil {
				return nil, err
			}
			innerf.Negate = negate
			//u.Infof("%d hm tok=%v %s", depth, m.Cur(), innerf.String())
			fe = NewFilterExpr()
			fe.Filter = innerf
			filters.Filters = append(filters.Filters, fe)

		case lex.TokenInclude:
			// embed/include a named filter
			m.Next()
			if m.Cur().T != lex.TokenIdentity {
				return nil, fmt.Errorf("Expected identity for Include but got %v", m.Cur())
			}
			fe = NewFilterExpr()
			fe.Negate = negate
			fe.Include = m.Cur().V
			m.Next()
			filters.Filters = append(filters.Filters, fe)
			continue

		case lex.TokenLeftParenthesis:
			m.Next()

			innerf, err := m.parseFilters(depth + 1)
			if err != nil {
				return nil, err
			}
			innerf.Negate = negate
			if len(filters.Filters) == 0 {
				//u.Infof("replacing filters %v", negate)
				if innerf.Negate || filters.Negate {
					innerf.Negate = true
				}
				innerf.Op = filters.Op
				filters = innerf
			} else {
				fe = NewFilterExpr()
				fe.Filter = innerf
				//fe.Negate = negate
				//u.Infof("what? %v", negate)
				filters.Filters = append(filters.Filters, fe)
			}
			return filters, nil
		case lex.TokenUdfExpr:
			// we have a udf/functional expression filter
			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.filterTokenPager)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			fe.Expr = tree.Root

		case lex.TokenIdentity, lex.TokenLike, lex.TokenExists, lex.TokenBetween,
			lex.TokenIN, lex.TokenValue:

			if m.Cur().T == lex.TokenIdentity {
				tv := strings.ToLower(m.Cur().V)
				if tv == "match_all" {
					fe = NewFilterExpr()
					fe.MatchAll = true
					m.Next()
					filters.Filters = append(filters.Filters, fe)
					continue
				} else if tv == "include" {
					// TODO:  this is a bug in lexer ...
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
				}
			}

			fe = NewFilterExpr()
			filters.Filters = append(filters.Filters, fe)
			tree := NewTree(m.filterTokenPager)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			fe.Expr = tree.Root

		}
		//u.Debugf("after filter start?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenLimit, lex.TokenEOS, lex.TokenEOF, lex.TokenAlias:
			//u.Infof("%d end ?? %q", depth, filters.String())
			return filters, nil
		case lex.TokenCommentSingleLine, lex.TokenCommentStart, lex.TokenCommentSlashes, lex.TokenComment,
			lex.TokenCommentEnd:
			// should we save this into filter?
		case lex.TokenRightParenthesis:
			// end of this filter expression
			//u.Infof("%d end ) %q", depth, filters.String())
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
func (m *filterQLParser) parseNode(tree *Tree) error {
	//u.Debugf("cur token parse: token=%v", m.Cur())
	err := tree.BuildTree(m.buildVm)
	if err != nil {
		u.Errorf("error: %v", err)
	}
	return err
}

func (m *filterQLParser) parseLimit(req *FilterStatement) error {
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

func (m *filterQLParser) parseAlias(req *FilterStatement) error {
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

func (m *filterQLParser) isEnd() bool {
	return m.IsEnd()
}
