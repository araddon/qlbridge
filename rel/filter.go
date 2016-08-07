package rel

import (
	"fmt"
	"hash/fnv"
	"io"
	"strings"

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

func (m *FilterStatement) WriteDialect(w expr.DialectWriter) {

	if m.Description != "" {
		if !strings.Contains(m.Description, "\n") {
			io.WriteString(w, "--")
			io.WriteString(w, m.Description)
			io.WriteString(w, "\n")
		}
	}
	io.WriteString(w, "FILTER ")
	m.Filter.WriteDialect(w)

	if m.From != "" {
		io.WriteString(w, " FROM ")
		w.WriteIdentity(m.From)
	}
	if m.Limit > 0 {
		io.WriteString(w, fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if len(m.With) > 0 {
		io.WriteString(w, " WITH ")
		HelperString(w, m.With)
	}
	if m.Alias != "" {
		io.WriteString(w, " ALIAS ")
		w.WriteIdentity(m.Alias)
	}
}

// String representation of FilterStatement
func (m *FilterStatement) String() string {
	if m == nil {
		return ""
	}
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}

// FingerPrint consistent hashed int value of FingerPrint above
func (m *FilterStatement) FingerPrintID() int64 {
	h := fnv.New64()
	w := expr.NewFingerPrinter()
	m.WriteDialect(w)
	h.Write([]byte(w.String()))
	return int64(h.Sum64())
}

// Includes Recurse this statement and find all includes
func (m *FilterStatement) Includes() []string {
	return m.Filter.Includes()
}

func (m *FilterStatement) EqualLogic(s *FilterStatement) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Filter != nil && !m.Filter.Equal(s.Filter) {
		u.Warn("not equal?")
		return false
	}
	return true
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
	if len(m.With) != len(s.With) {
		return false
	}
	if len(m.With) > 0 || len(s.With) > 0 {
		if !EqualWith(m.With, s.With) {
			return false
		}
	}
	return m.EqualLogic(s)
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

func (m *FilterSelect) WriteDialect(w expr.DialectWriter) {

	io.WriteString(w, "SELECT ")
	m.Columns.WriteDialect(w)

	if m.From != "" {
		io.WriteString(w, " FROM ")
		w.WriteIdentity(m.From)
	}

	io.WriteString(w, " FILTER ")

	m.Filter.WriteDialect(w)

	if m.Limit > 0 {
		io.WriteString(w, fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if len(m.With) > 0 {
		io.WriteString(w, " WITH ")
		HelperString(w, m.With)
	}
	if m.Alias != "" {
		io.WriteString(w, " ALIAS ")
		w.WriteIdentity(m.Alias)
	}
}

// String representation of FilterSelect
func (m *FilterSelect) String() string {
	if m == nil {
		return ""
	}
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}

// FingerPrint consistent hashed int value of FingerPrint above
func (m *FilterSelect) FingerPrintID() int64 {
	h := fnv.New64()
	w := expr.NewFingerPrinter()
	m.WriteDialect(w)
	h.Write([]byte(w.String()))
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
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}

func (m *Filters) WriteDialect(w expr.DialectWriter) {

	if m.Negate {
		io.WriteString(w, "NOT ")
	}

	if len(m.Filters) == 1 {
		m.Filters[0].WriteDialect(w)
		return
	}

	switch m.Op {
	case lex.TokenAnd, lex.TokenLogicAnd:
		io.WriteString(w, "AND")
	case lex.TokenOr, lex.TokenLogicOr:
		io.WriteString(w, "OR")
	}
	if w.Len() > 0 {
		io.WriteString(w, " ")
	}
	io.WriteString(w, "( ")

	for i, innerf := range m.Filters {
		if i != 0 {
			io.WriteString(w, ", ")
		}
		io.WriteString(w, innerf.String())
	}
	io.WriteString(w, " )")
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
	if m.Op != s.Op {
		u.Warnf("not equal op")
		return false
	}

	// We are checking for logical equality so check for elided expressions
	lf, rf := m.Filters, s.Filters
	if len(lf) != len(rf) {
		if len(lf) == 1 {
			lf = m.Filters[0].Filter.Filters
		} else if len(s.Filters) == 1 {
			rf = s.Filters[0].Filter.Filters
		}
		if len(lf) != len(rf) {
			u.Warnf("not equal lens?")
			return false
		}
	}
	for i, f := range lf {
		if !f.Equal(rf[i]) {
			u.Warnf("elided not equal:  %s:%s", f, rf[i])
			return false
		}
	}
	if m.Negate != s.Negate {
		u.Warnf("negate not equal")
		return false
	}

	return true
}

func NewFilterExpr() *FilterExpr {
	return &FilterExpr{}
}

// String representation of FilterExpression for diagnostic purposes.
func (fe *FilterExpr) String() string {
	w := expr.NewDefaultWriter()
	fe.WriteDialect(w)
	return w.String()
}
func (fe *FilterExpr) WriteDialect(w expr.DialectWriter) {
	if fe.Negate {
		io.WriteString(w, "NOT ")
	}
	switch {
	case fe.Include != "":
		io.WriteString(w, "INCLUDE ")
		w.WriteIdentity(fe.Include)
	case fe.Expr != nil:
		fe.Expr.WriteDialect(w)
	case fe.Filter != nil:
		fe.Filter.WriteDialect(w)
	case fe.MatchAll == true:
		io.WriteString(w, "*")
	default:
		io.WriteString(w, "<invalid expression>")
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
	if fe.MatchAll != s.MatchAll {
		return false
	}
	if fe.Include != s.Include {
		return false
	}

	le, re := fe.Expr, s.Expr
	if le != nil || re != nil {
		// Check for elided expression
		if le == nil && fe.Filter != nil && len(fe.Filter.Filters) == 1 {
			le = fe.Filter.Filters[0].Expr
		}
		if re == nil && s.Filter != nil && len(s.Filter.Filters) == 1 {
			le = s.Filter.Filters[0].Expr
		}
		return le.Equal(re)
	}
	if fe.Negate != s.Negate {
		return false
	}
	if fe.Filter != nil && !fe.Filter.Equal(s.Filter) {
		return false
	}
	return true
}
