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
		Keyword     lex.TokenType // Keyword SELECT or FILTER
		Description string        // initial pre-start comments
		Raw         string        // full original raw statement
		Filter      *Filters      // A top level filter
		From        string        // From is optional
		Limit       int           // Limit
		Offset      int           // Offset
		Alias       string        // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With        u.JsonHelper  // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
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
		// This should NOT be available to Expr nodes which have their own built
		// in negation/urnary
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
		buf.WriteString("SELECT")
		buf.WriteByte(' ')
	case lex.TokenFilter:
		buf.WriteString("FILTER")
		buf.WriteByte(' ')
	}

	m.Filter.writeBuf(buf)

	if m.From != "" {
		buf.WriteString(fmt.Sprintf(" FROM %s", m.From))
	}
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
	if m == nil {
		return ""
	}
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}

// Recurse this statement and find all includes
func (m *FilterStatement) Includes() []string {
	return m.Filter.Includes()
}

func NewFilters(tt lex.TokenType) *Filters {
	return &Filters{Op: tt, Filters: make([]*FilterExpr, 0)}
}

// String representation of Filters
func (m *Filters) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}

func (m *Filters) writeBuf(buf *bytes.Buffer) {

	if m.Negate {
		buf.WriteString("NOT")
		buf.WriteByte(' ')
	}

	if len(m.Filters) == 1 {
		buf.WriteString(m.Filters[0].String())
		return
	}

	switch m.Op {
	case lex.TokenAnd, lex.TokenLogicAnd:
		buf.WriteString("AND")
	case lex.TokenOr, lex.TokenLogicOr:
		buf.WriteString("OR")
	}
	if buf.Len() > 0 {
		buf.WriteByte(' ')
	}
	buf.WriteString("( ")

	for i, innerf := range m.Filters {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(innerf.String())
	}
	buf.WriteString(" )")
}

// Recurse these filters and find all includes
func (m *Filters) Includes() []string {
	inc := make([]string, 0)
	for _, f := range m.Filters {
		finc := f.Includes()
		if len(finc) > 0 {
			inc = append(inc, finc...)
		}
	}
	return inc
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

// Recurse this expression and find all includes
func (fe *FilterExpr) Includes() []string {
	if len(fe.Include) > 0 {
		return []string{fe.Include}
	}
	if fe.Filter == nil {
		return nil
	}
	return fe.Filter.Includes()
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

func (m *filterQLParser) discardNewLines() {
	for {
		// We are going to loop until we find the first Non-NewLine
		switch m.Cur().T {
		case lex.TokenNewLine:
			m.Next()
		default:
			// first non-comment token
			return
		}
	}
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *filterQLParser) parseSelect() (*FilterStatement, error) {

	req := NewFilterStatement()
	req.Raw = m.l.RawInput()
	req.Description = m.comment

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
	m.discardNewLines()

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
	req.Description = m.comment
	req.Raw = m.l.RawInput()
	req.Keyword = m.Cur().T
	m.Next() // Consume (FILTER | WHERE )

	//u.Warnf("starting filter %s", req.Raw)
	// one top level filter which may be nested
	filter, err := m.parseFirstFilters()
	if err != nil {
		u.Warnf("Could not parse filters %q err=%v", req.Raw, err)
		return nil, err
	}
	m.discardNewLines()
	req.Filter = filter

	// OPTIONAL From clause
	if m.Cur().T == lex.TokenFrom {
		m.Next()
		if m.Cur().T != lex.TokenIdentity {
			return nil, fmt.Errorf("expected identity after FROM")
		}
		if m.Cur().T == lex.TokenIdentity || m.Cur().T == lex.TokenTable {
			req.From = m.Cur().V
			m.Next()
		}
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

func (m *filterQLParser) parseFirstFilters() (*Filters, error) {

	//u.Infof("outer loop:  Cur():%v  %s", m.Cur(), m.l.RawInput())

	switch m.Cur().T {
	case lex.TokenStar, lex.TokenMultiply:

		m.Next() // Consume *
		filters := NewFilters(lex.TokenLogicAnd)
		fe := NewFilterExpr()
		fe.MatchAll = true
		filters.Filters = append(filters.Filters, fe)
		// if we have match all, nothing else allowed
		return filters, nil

	case lex.TokenIdentity:
		if strings.ToLower(m.Cur().V) == "match_all" {
			m.Next()
			filters := NewFilters(lex.TokenLogicAnd)
			fe := NewFilterExpr()
			fe.MatchAll = true
			filters.Filters = append(filters.Filters, fe)
			// if we have match all, nothing else allowed
			return filters, nil
		}
		// Fall through
	case lex.TokenNewLine:
		m.Next()
		return m.parseFirstFilters()
	}

	var op *lex.Token
	//u.Infof("cur? %#v", m.Cur())
	switch m.Cur().T {
	case lex.TokenAnd, lex.TokenOr, lex.TokenLogicAnd, lex.TokenLogicOr:
		op = &lex.Token{T: m.Cur().T, V: m.Cur().V}
		//found = true
		m.Next()
	}
	// If we don't have a shortcut
	filters, err := m.parseFilters(0, false, op)
	if err != nil {
		return nil, err
	}
	switch m.Cur().T {
	case lex.TokenRightParenthesis:
		u.Infof("consume right token")
		m.Next()
	}
	return filters, nil
}

func (m *filterQLParser) parseFilters(depth int, filtersNegate bool, filtersOp *lex.Token) (*Filters, error) {

	filters := NewFilters(lex.TokenLogicAnd) // Default outer is AND
	filters.Negate = filtersNegate
	if filtersOp != nil {
		filters.Op = filtersOp.T
		//u.Infof("%d %p setting filtersOp: %v", depth, filters, filters.String())
	}

	//u.Debugf("%d parseFilters() negate?%v filterop:%v cur:%v peek:%q", depth, filtersNegate, filtersOp, m.Cur(), m.l.PeekX(20))

	for {

		negate := false
		//found := false
		var op *lex.Token
		switch m.Cur().T {
		case lex.TokenNegate:
			negate = true
			//found = true
			m.Next()
		}

		switch m.Cur().T {
		case lex.TokenAnd, lex.TokenOr, lex.TokenLogicAnd, lex.TokenLogicOr:
			op = &lex.Token{T: m.Cur().T, V: m.Cur().V}
			//found = true
			m.Next()
		}
		//u.Debugf("%d start negate:%v  op:%v  filtersOp?%#v cur:%v", depth, negate, op, filtersOp, m.Cur())

		switch m.Cur().T {
		case lex.TokenLeftParenthesis:

			m.Next() // Consume   (

			if op == nil && filtersOp != nil && len(filters.Filters) == 0 {
				op = filtersOp
			}
			//u.Infof("%d %p consume ( op:%s for %s", depth, filters, op, filters.String())
			innerf, err := m.parseFilters(depth+1, negate, op)
			if err != nil {
				return nil, err
			}
			fe := NewFilterExpr()
			fe.Filter = innerf
			//u.Infof("%d inner ops:%s len=%d ql=%s", depth, filters.Op, len(innerf.Filters), innerf.String())
			filters.Filters = append(filters.Filters, fe)
			//u.Infof("%d %p filters: %s", depth, filters, filters.String())

		case lex.TokenUdfExpr, lex.TokenIdentity, lex.TokenLike, lex.TokenExists, lex.TokenBetween,
			lex.TokenIN, lex.TokenValue, lex.TokenInclude, lex.TokenContains:

			if op != nil {
				u.Errorf("should not have op on Clause? %v", m.Cur())
			}
			fe, err := m.parseFilterClause(depth, negate)
			if err != nil {
				return nil, err
			}
			//u.Infof("%d adding %s   new: %v", depth, filters.String(), fe.String())
			filters.Filters = append(filters.Filters, fe)

		}
		//u.Debugf("after filter start?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenLimit, lex.TokenFrom, lex.TokenAlias, lex.TokenEOS, lex.TokenEOF:
			return filters, nil
		case lex.TokenCommentSingleLine, lex.TokenCommentStart, lex.TokenCommentSlashes, lex.TokenComment,
			lex.TokenCommentEnd:
			// should we save this into filter?
			m.Next()
		case lex.TokenRightParenthesis:
			// end of this filter expression
			//u.Debugf("%d end ) %q", depth, filters.String())
			m.Next()
			return filters, nil
		case lex.TokenComma, lex.TokenNewLine:
			// keep looping, looking for more expressions
			m.Next()
		default:
			u.Warnf("cur? %v", m.Cur())
			return nil, fmt.Errorf("expected column but got: %v", m.Cur().String())
		}

		// reset any filter level stuff
		filtersNegate = false
		filtersOp = nil

	}
	//u.Debugf("filters: %d", len(filters.Filters))
	return filters, nil
}

func (m *filterQLParser) parseFilterClause(depth int, negate bool) (*FilterExpr, error) {

	fe := NewFilterExpr()
	fe.Negate = negate
	//u.Debugf("%d filterclause? negate?%v  cur=%v", depth, negate, m.Cur())

	switch m.Cur().T {
	case lex.TokenInclude:
		// embed/include a named filter

		m.Next()
		//u.Infof("type %v", m.Cur())
		if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
			return nil, fmt.Errorf("Expected identity for Include but got %v", m.Cur())
		}
		fe.Include = m.Cur().V
		m.Next()

	case lex.TokenUdfExpr:
		// we have a udf/functional expression filter
		tree := NewTree(m.filterTokenPager)
		if err := m.parseNode(tree); err != nil {
			u.Errorf("could not parse: %v", err)
			return nil, err
		}
		fe.Expr = tree.Root

	case lex.TokenIdentity, lex.TokenLike, lex.TokenExists, lex.TokenBetween,
		lex.TokenIN, lex.TokenValue, lex.TokenContains:

		if m.Cur().T == lex.TokenIdentity && strings.ToLower(m.Cur().V) == "include" {
			// TODO:  this is a bug in lexer ...
			// embed/include a named filter
			m.Next()
			if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
				return nil, fmt.Errorf("Expected identity for Include but got %v", m.Cur())
			}
			fe.Include = m.Cur().V
			m.Next()
			return fe, nil
		}

		tree := NewTree(m.filterTokenPager)
		if err := m.parseNode(tree); err != nil {
			u.Errorf("could not parse: %v", err)
			return nil, err
		}
		fe.Expr = tree.Root
	default:
		return nil, fmt.Errorf("Expected clause but got %v", m.Cur())
	}
	return fe, nil
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
