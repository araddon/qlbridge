// sqlite implements a Qlbridge Datasource interface around sqlite.
package sqlite

import (
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/araddon/qlbridge/schema"
)

const (
	// SourceType "sqlite" is the registered Source name in the qlbridge source registry
	SourceType = "sqlite"
)

func init() {
	// We need to register our DataSource provider here
	schema.RegisterSourceType(SourceType, newSourceEmtpy())
}

var (
	// Ensure our source implements Source interface
	_ schema.Source = (*Source)(nil)
)

// Source implements qlbridge DataSource to a sqlite file based source.
//
// Features
// - Support full predicate push down to SqlLite.
// - Support Thread-Safe wrapper around sqlite file.
type Source struct {
	exit   <-chan bool
	name   string
	conns  map[string]*conn
	tables []string
	mu     sync.Mutex
}

func newSourceEmtpy() schema.Source {
	return &Source{conns: make(map[string]*conn)}
}
func (m *Source) Setup(s *schema.Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, exists := m.conns[s.Name]
	if exists {
		return nil
	}

	conn = newConn(s)
	if err := conn.setup(); err != nil {
		return err
	}
	m.conns[s.Name] = conn

	m.tables = make([]string, 0)
	for _, tbl := range conn.Tables() {
		m.tables = append(m.tables, tbl)
	}

	// if err := datasource.IntrospectTable(m.tbl, m.CreateIterator()); err != nil {
	// 	u.Errorf("Could not introspect schema %v", err)
	// }

	return nil
}
func (m *Source) Init()                                     {}
func (m *Source) Open(connInfo string) (schema.Conn, error) { return m, nil }
func (m *Source) Table(table string) (*schema.Table, error) { return nil, nil }
func (m *Source) Close() error                              { return nil }
func (m *Source) Tables() []string                          { return m.tables }
