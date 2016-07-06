package rel

import (
	"bytes"
	"fmt"
	"hash/fnv"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
)

var (
	_ = u.EMPTY

	// Ensure each Filter statement implement's Filter interface
	_ Filter = (*FilterStatement)(nil)
	_ Filter = (*FilterSelect)(nil)
	// Statements with Columns
	_ ColumnsStatement = (*FilterSelect)(nil)
)

type (
	// Filter interface for Filter Statements (either Filter/FilterSelect)
	Filter interface {
		String() string
	}
	// FilterSelect is a Filter but also has projected columns
	FilterSelect struct {
		*FilterStatement
		Columns Columns
	}
	// FilterStatement is a statement of type = Filter
	FilterStatement struct {
		Description string       // initial pre-start comments
		Raw         string       // full original raw statement
		Filter      *Filters     // A top level filter
		From        string       // From is optional
		Limit       int          // Limit
		HasDateMath bool         // does this have date math?
		Alias       string       // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With        u.JsonHelper // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
	}
	// Filters A list of Filter Expressions
	Filters struct {
		Negate  bool          // Should we negate this response?
		Op      lex.TokenType // OR, AND
		Filters []*FilterExpr
	}
	// FilterExpr a Single Filter expression
	FilterExpr struct {
		IncludeFilter *FilterStatement // Memoized Include

		// Do we negate this entire Filter?  Default = false (ie, don't negate)
		// This should NOT be available to Expr nodes which have their own built
		// in negation/urnary
		Negate bool

		// Exactly one of these will be non-nil
		Include  string    // name of foreign named alias filter to embed
		Expr     expr.Node // Node might be nil in which case must have filter
		Filter   *Filters  // might be nil, must have expr
		MatchAll bool      // * = match all
	}
)

// NewFilterStatement Create A FilterStatement
func NewFilterStatement() *FilterStatement {
	req := &FilterStatement{}
	return req
}

func (m *FilterStatement) writeBuf(buf *bytes.Buffer) {

	buf.WriteString("FILTER ")
	m.Filter.writeBuf(buf)

	if m.From != "" {
		buf.WriteString(fmt.Sprintf(" FROM %s", m.From))
	}
	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
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

// FingerPrint create a consistent string value for statements
//  that is equivalent to a prepared-statement, multiple occurences of same
//  statement (query plan) with different Values would hash to same value.
// @rune to use to replace arguments (default = "?")
func (m *FilterStatement) FingerPrint(r rune) string {

	buf := &bytes.Buffer{}
	buf.WriteString("SELECT ")
	m.Filter.writeFingerPrint(buf, r)

	if m.From != "" {
		buf.WriteString(fmt.Sprintf(" FROM %s", m.From))
	}
	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Alias != "" {
		buf.WriteString(fmt.Sprintf(" ALIAS %s", m.Alias))
	}
	return buf.String()
}

// FingerPrint consistent hashed int value of FingerPrint above
func (m *FilterStatement) FingerPrintID() int64 {
	h := fnv.New64()
	h.Write([]byte(m.FingerPrint(rune('?'))))
	return int64(h.Sum64())
}

// Includes Recurse this statement and find all includes
func (m *FilterStatement) Includes() []string {
	return m.Filter.Includes()
}

func (m *FilterStatement) Equal(s *FilterStatement) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Description != s.Description {
		return false
	}
	if m.From != s.From {
		return false
	}
	if m.Limit != s.Limit {
		return false
	}
	if m.HasDateMath != s.HasDateMath {
		return false
	}
	if m.Alias != s.Alias {
		return false
	}
	if m.Filter != nil && !m.Filter.Equal(s.Filter) {
		return false
	}
	return true
}

func NewFilterSelect() *FilterSelect {
	req := &FilterSelect{}
	return req
}

func (m *FilterSelect) AddColumn(colArg Column) error {
	col := &colArg
	col.Index = len(m.Columns)
	m.Columns = append(m.Columns, col)

	if col.As == "" && col.Expr == nil && !col.Star {
		u.Errorf("no as or expression?  %#s", col)
	}
	return nil
}

func (m *FilterSelect) writeBuf(buf *bytes.Buffer) {

	buf.WriteString("SELECT ")
	m.Columns.writeBuf(buf)

	if m.From != "" {
		buf.WriteString(fmt.Sprintf(" FROM %s", m.From))
	}

	buf.WriteString(" FILTER ")

	m.Filter.writeBuf(buf)

	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Alias != "" {
		buf.WriteString(fmt.Sprintf(" ALIAS %s", m.Alias))
	}
}

// String representation of FilterSelect
func (m *FilterSelect) String() string {
	if m == nil {
		return ""
	}
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}

// FingerPrint create a consistent string value for statements
//  that is equivalent to a prepared-statement, multiple occurences of same
//  statement (query plan) with different Values would hash to same value.
// @rune to use to replace arguments (default = "?")
func (m *FilterSelect) FingerPrint(r rune) string {
	buf := &bytes.Buffer{}
	buf.WriteString("SELECT ")
	m.Filter.writeFingerPrint(buf, r)
	//m.Columns.writeBuf(buf)

	if m.From != "" {
		buf.WriteString(fmt.Sprintf(" FROM %s", m.From))
	}
	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Alias != "" {
		buf.WriteString(fmt.Sprintf(" ALIAS %s", m.Alias))
	}
	return buf.String()
}

// FingerPrint consistent hashed int value of FingerPrint above
func (m *FilterSelect) FingerPrintID() int64 {
	h := fnv.New64()
	h.Write([]byte(m.FingerPrint(rune('?'))))
	return int64(h.Sum64())
}

// Recurse this statement and find all includes
func (m *FilterSelect) Includes() []string {
	return m.Filter.Includes()
}

// Equal
func (m *FilterSelect) Equal(s *FilterSelect) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if !m.Columns.Equal(s.Columns) {
		return false
	}
	mfs := m.FilterStatement
	if mfs != nil {
		sfs := s.FilterStatement
		if !mfs.Equal(sfs) {
			return false
		}
	}
	return true
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
func (m *Filters) writeFingerPrint(buf *bytes.Buffer, r rune) {

	if m.Negate {
		buf.WriteString("NOT")
		buf.WriteByte(' ')
	}

	if len(m.Filters) == 1 {
		m.Filters[0].writeFingerPrint(buf, r)
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
		innerf.writeFingerPrint(buf, r)
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
func (m *Filters) Equal(s *Filters) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Negate != s.Negate {
		return false
	}
	if m.Op != s.Op {
		return false
	}
	if len(m.Filters) != len(s.Filters) {
		return false
	}
	for i, f := range m.Filters {
		if !f.Equal(s.Filters[i]) {
			return false
		}
	}
	return true
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

func (fe *FilterExpr) writeFingerPrint(buf *bytes.Buffer, r rune) {

	if fe.Negate {
		fmt.Fprint(buf, "NOT ")
	}
	switch {
	case fe.Include != "":
		fmt.Fprintf(buf, "%sINCLUDE %s", fe.Include)
	case fe.Expr != nil:
		fmt.Fprintf(buf, "%s%s", fe.Expr.FingerPrint(r))
	case fe.Filter != nil:
		fe.Filter.writeFingerPrint(buf, r)
	case fe.MatchAll == true:
		fmt.Fprint(buf, "*")
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

func (fe *FilterExpr) Equal(s *FilterExpr) bool {
	if fe == nil && s == nil {
		return true
	}
	if fe == nil && s != nil {
		return false
	}
	if fe != nil && s == nil {
		return false
	}
	if fe.Negate != s.Negate {
		return false
	}
	if fe.MatchAll != s.MatchAll {
		return false
	}
	if fe.Include != s.Include {
		return false
	}
	if fe.Expr != nil && !fe.Expr.Equal(s.Expr) {
		return false
	}
	if fe.Filter != nil && !fe.Filter.Equal(s.Filter) {
		return false
	}
	return true
}
