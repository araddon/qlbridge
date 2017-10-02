package rel

import (
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
)

// FilterQLParser
type FilterQLParser struct {
	// can be a FilterStatement, FilterStatements, filterSelect, filterSelects, etc.
	// Which one is determined by which Parser func you call.
	statement string
	fs        *FilterStatement
	l         *lex.Lexer
	comment   string
	*filterTokenPager
	firstToken lex.Token
	funcs      expr.FuncResolver
}

func NewFilterParser(filter string) *FilterQLParser {
	return &FilterQLParser{statement: filter}
}
func NewFilterParserfuncs(filter string, funcs expr.FuncResolver) *FilterQLParser {
	return &FilterQLParser{statement: filter, funcs: funcs}
}

// FuncResolver sets the function resolver to use during parsing.  By default we only use the Global resolver.
// But if you set a function resolver we'll use that first and then fall back to the Global resolver.
func (f *FilterQLParser) FuncResolver(funcs expr.FuncResolver) *FilterQLParser {
	f.funcs = funcs
	return f
}

func (f *FilterQLParser) setLexer(statement string) {
	l := lex.NewFilterQLLexer(statement)
	f.l = l
	f.fs = nil
	f.comment = ""
	f.filterTokenPager = newFilterTokenPager(l)
}

// ParseFilterQL Parses a FilterQL statement
func (f *FilterQLParser) ParseFilter() (*FilterSelect, error) {
	f.setLexer(f.statement)
	return f.parseSelectStart()
}

func (f *FilterQLParser) ParseFilters() (stmts []*FilterStatement, err error) {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("Could not parse %s  %v", f.statement, r)
			err = fmt.Errorf("Could not parse %v", r)
		}
	}()
	f.setLexer(f.statement)
	for {
		stmt, err := f.parseFilterStart()
		if err != nil {
			return nil, err
		}

		stmts = append(stmts, stmt)
		queryRemaining, hasMore := f.l.Remainder()
		if !hasMore {
			break
		}
		f.setLexer(queryRemaining)
	}
	return
}

func (f *FilterQLParser) ParseFilterSelects() (stmts []*FilterSelect, err error) {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("Could not parse %s  %v", f.statement, r)
			err = fmt.Errorf("Could not parse %v", r)
		}
	}()
	f.setLexer(f.statement)
	for {
		stmt, err := f.parseSelectStart()
		if err != nil {
			return nil, err
		}

		stmts = append(stmts, stmt)
		queryRemaining, hasMore := f.l.Remainder()
		if !hasMore {
			break
		}
		f.setLexer(queryRemaining)
	}
	return
}

// ParseFilters Parse a list of Filter statement's from text
func ParseFilters(statement string) (stmts []*FilterStatement, err error) {
	return NewFilterParser(statement).ParseFilters()
}
func MustParseFilters(statement string) []*FilterStatement {
	stmts, err := ParseFilters(statement)
	if err != nil {
		panic(err.Error())
	}
	return stmts
}
func MustParseFilter(statement string) *FilterStatement {
	stmts, err := ParseFilters(statement)
	if err != nil {
		panic(err.Error())
	}
	if len(stmts) != 1 {
		panic("Must have exactly 1 statement")
	}
	return stmts[0]
}

// ParseFilterQL Parses a FilterQL statement
func ParseFilterQL(filter string) (*FilterStatement, error) {
	f, err := NewFilterParser(filter).ParseFilter()
	if err != nil {
		return nil, err
	}
	return f.FilterStatement, nil
}

// ParseFilterSelect Parse a single Select-Filter statement from text
// Select-Filters are statements of following form
//    "SELECT" [COLUMNS] (FILTER | WHERE) FilterExpression
//    "FILTER" FilterExpression
func ParseFilterSelect(query string) (*FilterSelect, error) {
	return NewFilterParser(query).ParseFilter()
}

// ParseFilterSelects Parse 1-n Select-Filter statements from text
// Select-Filters are statements of following form
//    "SELECT" [COLUMNS] (FILTER | WHERE) FilterExpression
//    "FILTER" FilterExpression
func ParseFilterSelects(statement string) (stmts []*FilterSelect, err error) {
	return NewFilterParser(statement).ParseFilterSelects()
}

type (
	// TokenPager is responsible for determining end of current clause
	//   An interface used to allow Parser to be neutral to dialect
	filterTokenPager struct {
		*expr.LexTokenPager
		lastKw lex.TokenType
	}
)

func newFilterTokenPager(l *lex.Lexer) *filterTokenPager {
	pager := expr.NewLexTokenPager(l)
	return &filterTokenPager{LexTokenPager: pager}
}

func (m *filterTokenPager) IsEnd() bool {
	return m.LexTokenPager.IsEnd()
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

func (m *FilterQLParser) parseFilterStart() (*FilterStatement, error) {
	m.comment = m.initialComment()
	m.firstToken = m.Cur()
	switch m.firstToken.T {
	case lex.TokenFilter, lex.TokenWhere:
		return m.parseFilter()
	}
	return nil, fmt.Errorf("Unrecognized Filter Statement: %v peek:%s", m.firstToken, m.l.PeekWord())
}

func (m *FilterQLParser) parseSelectStart() (*FilterSelect, error) {
	m.comment = m.initialComment()
	m.firstToken = m.Cur()
	switch m.firstToken.T {
	case lex.TokenFilter:
		fs, err := m.parseFilter()
		if err != nil {
			return nil, err
		}
		return &FilterSelect{FilterStatement: fs}, nil
	case lex.TokenSelect:
		return m.parseSelect()
	}
	return nil, fmt.Errorf("Unrecognized Filter Statement: %v  %v", m.firstToken, m.l.PeekWord())
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
}

func (m *FilterQLParser) discardNewLines() {
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

func (m *FilterQLParser) discardCommentsNewLines() {
	for {
		// We are going to loop until we find the first Non-Comment Token
		switch m.Cur().T {
		case lex.TokenNewLine:
			m.Next()
		case lex.TokenComment, lex.TokenCommentML,
			lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd,
			lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
			m.Next()
		default:
			// first non-comment token
			return
		}
	}
}
func (m *FilterQLParser) discardComments() {

	for {
		// We are going to loop until we find the first Non-Comment Token
		switch m.Cur().T {
		case lex.TokenComment, lex.TokenCommentML,
			lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd,
			lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
			m.Next()
		default:
			// first non-comment token
			return
		}

	}
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *FilterQLParser) parseSelect() (*FilterSelect, error) {

	req := &FilterSelect{FilterStatement: &FilterStatement{}}
	m.fs = req.FilterStatement
	req.Raw = m.l.RawInput()
	req.Description = m.comment
	m.Next() // Consume Select

	err := parseColumns(m, m.funcs, req)
	if err != nil {
		return nil, err
	}

	// optional FROM
	if m.Cur().T == lex.TokenFrom {
		m.Next()
		if m.Cur().T == lex.TokenIdentity || m.Cur().T == lex.TokenTable {
			req.From = m.Next().V
		} else {
			return nil, fmt.Errorf("Expected FROM <identity> got %v", m.Cur())
		}
	}

	// We accept either WHERE or FILTER
	switch t := m.Next().T; t {
	case lex.TokenWhere:
		// one top level filter which may be nested
		if err = m.parseWhereExpr(req); err != nil {
			return nil, err
		}
	case lex.TokenFilter:
		// one top level filter which may be nested
		filter, err := m.parseFirstFilters()
		if err != nil {
			u.Warnf("Could not parse filters %q err=%v", req.Raw, err)
			return nil, err
		}
		req.Filter = filter
	default:
		return nil, fmt.Errorf("expected SELECT * FROM <table> { <WHERE> | <FILTER> } but got %v instead of WHERE/FILTER", t)
	}

	// LIMIT  - Optional
	m.discardCommentsNewLines()
	req.Limit, err = m.parseLimit()
	if err != nil {
		return nil, err
	}

	// WITH  - Optional
	m.discardCommentsNewLines()
	req.With, err = ParseWith(m)
	if err != nil {
		return nil, err
	}

	// ALIAS  - Optional
	m.discardCommentsNewLines()
	req.Alias, err = m.parseAlias()
	if err != nil {
		return nil, err
	}

	m.discardCommentsNewLines()
	switch m.Cur().T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenRightParenthesis:
		return req, nil
	}
	return nil, fmt.Errorf("Did not complete parsing input: %v", m.LexTokenPager.Cur())
}

// First keyword was FILTER, so use the FILTER parser rule-set
func (m *FilterQLParser) parseFilter() (*FilterStatement, error) {

	req := NewFilterStatement()
	m.fs = req
	req.Description = m.comment
	req.Raw = m.l.RawInput()
	m.Next() // Consume (FILTER | WHERE )

	// one top level filter which may be nested
	filter, err := m.parseFirstFilters()
	if err != nil {
		return nil, err
	}
	req.Filter = filter

	m.discardCommentsNewLines()
	// OPTIONAL From clause
	if m.Cur().T == lex.TokenFrom {
		m.Next()
		if m.Cur().T != lex.TokenIdentity {
			return nil, m.Cur().ErrMsg(m.l, "Expected identity after FROM")
		}
		if m.Cur().T == lex.TokenIdentity || m.Cur().T == lex.TokenTable {
			req.From = m.Cur().V
			m.Next()
		}
	}

	// LIMIT - Optional
	m.discardCommentsNewLines()
	req.Limit, err = m.parseLimit()
	if err != nil {
		return nil, err
	}

	// WITH - Optional
	m.discardCommentsNewLines()
	req.With, err = ParseWith(m)
	if err != nil {
		return nil, err
	}

	// ALIAS - Optional
	m.discardCommentsNewLines()
	req.Alias, err = m.parseAlias()
	if err != nil {
		return nil, err
	}

	m.discardCommentsNewLines()
	switch m.Cur().T {
	case lex.TokenEOF, lex.TokenEOS: //, lex.TokenRightParenthesis
		return req, nil
	case lex.TokenError:
		return nil, m.Cur().Err(m.l)
	}
	return nil, fmt.Errorf("Did not complete parsing input: %v", m.Cur())
}

func (m *FilterQLParser) parseWhereExpr(req *FilterSelect) error {
	n, err := expr.ParseExprWithFuncs(m.filterTokenPager, m.funcs)
	if err != nil {
		u.Errorf("could not parse: %v", err)
		return err
	}

	req.Where = n
	return nil
}

func (m *FilterQLParser) parseFirstFilters() (expr.Node, error) {

	// We have 2 special cases in filterQL
	// FILTER *
	// FILTER match_all
	switch m.Cur().T {
	case lex.TokenStar, lex.TokenMultiply:

		m.Next() // Consume *
		n := expr.NewIdentityNodeVal("*")
		// if we have match all, nothing else allowed
		return n, nil

	case lex.TokenIdentity:
		if strings.ToLower(m.Cur().V) == "match_all" {
			m.Next()
			n := expr.NewIdentityNodeVal("match_all")
			// if we have match all, nothing else allowed
			return n, nil
		}
	}

	m.discardCommentsNewLines()

	n, err := expr.ParseExprWithFuncs(m.filterTokenPager, m.funcs)
	if err != nil {
		u.Errorf("could not parse: %v", err)
		return nil, err
	}

	return n, nil
}

func (m *FilterQLParser) parseLimit() (int, error) {
	if m.Cur().T != lex.TokenLimit {
		return 0, nil
	}
	m.Next()
	if m.Cur().T != lex.TokenInteger {
		return 0, fmt.Errorf("Limit must be an integer %v", m.Cur())
	}
	iv, err := strconv.Atoi(m.Next().V)
	if err != nil {
		return 0, fmt.Errorf("Could not convert limit to integer %v", m.Cur().V)
	}

	return int(iv), nil
}

func (m *FilterQLParser) parseAlias() (string, error) {
	if m.Cur().T != lex.TokenAlias {
		return "", nil
	}
	m.Next() // Consume ALIAS token
	if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
		return "", fmt.Errorf("Expected identity but got: %v", m.Cur().T.String())
	}
	return strings.ToLower(m.Next().V), nil
}

func (m *FilterQLParser) isEnd() bool {
	return m.IsEnd()
}
