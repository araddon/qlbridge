package rel

import (
	"fmt"
	"hash/fnv"
	"io"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
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
		checkedIncludes bool
		includes        []string
		Description     string       // initial pre-start comments
		Raw             string       // full original raw statement
		Filter          expr.Node    // FILTER <filter_expr>
		Where           expr.Node    // WHERE <expr> [AND <expr>] syntax
		OrderBy         Columns      // order by
		From            string       // From is optional
		Limit           int          // Limit
		Alias           string       // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With            u.JsonHelper // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
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
	if !m.checkedIncludes {
		m.includes = expr.FindIncludes(m.Filter)
		m.checkedIncludes = true
	}
	return m.includes
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

// Equal Checks for deep equality
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
