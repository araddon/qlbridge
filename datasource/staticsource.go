package datasource

import (
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
)

var _ = u.EMPTY

var (
	// Enforce datasource feature interfaces
	_ schema.Source      = (*StaticSource)(nil)
	_ schema.ConnScanner = (*StaticSource)(nil)
	_ schema.Conn        = (*StaticSource)(nil)
	_ schema.ConnColumns = (*StaticSource)(nil)
)

// A static, non-thread safe, single-table data source
type StaticSource struct {
	table  string
	cols   []string
	cursor int
	vals   []schema.Message
	tbl    *schema.Table
	exit   <-chan bool
}

func NewStaticSource(name string, cols []string, msgs []schema.Message) *StaticSource {
	return &StaticSource{
		table: name,
		cols:  cols,
		vals:  msgs,
		exit:  make(<-chan bool, 1),
	}
}

func (m *StaticSource) Init()                              {}
func (m *StaticSource) Setup(*schema.SchemaSource) error   { return nil }
func (m *StaticSource) Tables() []string                   { return []string{m.table} }
func (m *StaticSource) Open(_ string) (schema.Conn, error) { return m, nil }
func (m *StaticSource) Close() error                       { return nil }
func (m *StaticSource) Columns() []string                  { return m.cols }
func (m *StaticSource) CreateIterator() schema.Iterator    { return m }
func (m *StaticSource) MesgChan() <-chan schema.Message {
	iter := m.CreateIterator()
	return SourceIterChannel(iter, m.exit)
}

func (m *StaticSource) Next() schema.Message {
	if len(m.vals) <= m.cursor {
		return nil
	}
	m.cursor++
	return m.vals[m.cursor-1]
}
func (m *StaticSource) Table(name string) (*schema.Table, error) {
	if m.tbl == nil {
		tbl := schema.NewTable(strings.ToLower(m.table))

		err := IntrospectTable(tbl, m.CreateIterator())
		if err == nil {
			return nil, err
		}
		m.tbl = tbl
	}
	return m.tbl, nil
}
