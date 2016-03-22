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

	// Ensure Filter statement types Filter
	_ Filter = (*FilterStatement)(nil)
	_ Filter = (*FilterSelect)(nil)
	// Statements with Columns
	_ ColumnsStatement = (*FilterSelect)(nil)
)

type (
	Filter interface {
		String() string
	}
	FilterSelect struct {
		*FilterStatement
		Columns Columns
	}
	// Filter Statement is a statement of type = Filter
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
		Include  string    // name of foreign named alias filter to embed
		Expr     expr.Node // Node might be nil in which case must have filter
		Filter   *Filters  // might be nil, must have expr
		MatchAll bool      // * = match all
	}
)

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
func (m *FilterStatement) FingerPrintID() int64 {
	h := fnv.New64()
	h.Write([]byte(m.FingerPrint(rune('?'))))
	return int64(h.Sum64())
}

// Recurse this statement and find all includes
func (m *FilterStatement) Includes() []string {
	return m.Filter.Includes()
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

// String representation of FilterSelect
func (m *FilterSelect) String() string {
	if m == nil {
		return ""
	}
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}
func (m *FilterSelect) FingerPrint(r rune) string {
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
func (m *FilterSelect) FingerPrintID() int64 {
	h := fnv.New64()
	h.Write([]byte(m.FingerPrint(rune('?'))))
	return int64(h.Sum64())
}

// Recurse this statement and find all includes
func (m *FilterSelect) Includes() []string {
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
