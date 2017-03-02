package files

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement Source for our file source storage
	_ schema.Source = (*storeSource)(nil)

	// Connection Interfaces
	_ schema.Conn        = (*storeSource)(nil)
	_ schema.ConnScanner = (*storeSource)(nil)
)

// storeSource DataSource for reading lists of files/names/metadata of files
// from the cloudstorage Store
//
// - readers:      s3, gcs, local-fs
type storeSource struct {
	f        *FileSource
	table    string
	tbl      *schema.Table
	exit     <-chan bool
	iter     cloudstorage.ObjectIterator
	complete bool
	err      error
	rowct    uint64
	mu       sync.Mutex
	// columns []string
	// colindex map[string]int
	// indexCol int
	// filter   expr.Node
}

// newStoreSource reader
func newStoreSource(table string, fs *FileSource) (*storeSource, error) {
	s := &storeSource{
		f:     fs,
		table: table,
		exit:  make(<-chan bool, 1),
	}
	return s, nil
}

func (m *storeSource) Init()                            {}
func (m *storeSource) Setup(*schema.SchemaSource) error { return nil }
func (m *storeSource) Tables() []string                 { return []string{m.table} }
func (m *storeSource) Columns() []string                { return m.f.fdbcols }
func (m *storeSource) CreateIterator() schema.Iterator  { return m }
func (m *storeSource) Table(tableName string) (*schema.Table, error) {

	u.Debugf("Table(%q), tbl nil?%v", tableName, m.tbl == nil)
	if m.tbl != nil {
		return m.tbl, nil
	} else {
		m.loadTable()
	}
	if m.tbl != nil {
		return m.tbl, nil
	}
	return nil, schema.ErrNotFound
}
func (m *storeSource) loadTable() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tbl != nil {
		return nil
	}
	u.Debugf("storeSource.loadTable(%q)", m.table)
	tbl := schema.NewTable(strings.ToLower(m.table))
	columns := m.Columns()
	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
		tbl.AddField(schema.NewFieldBase(columns[i], value.StringType, 64, "string"))
	}
	tbl.SetColumns(columns)
	m.tbl = tbl
	return nil
}
func (m *storeSource) Open(connInfo string) (schema.Conn, error) {
	// Make a copy of itself
	s := &storeSource{
		f:     m.f,
		table: m.table,
		tbl:   m.tbl,
		exit:  make(<-chan bool, 1),
	}
	q := cloudstorage.Query{"", m.f.path, nil}
	q.Sorted()
	s.iter = m.f.store.Objects(context.Background(), q)
	return s, nil
}

func (m *storeSource) Close() error {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("close error: %v", r)
		}
	}()
	return nil
}

func (m *storeSource) MesgChan() <-chan schema.Message {
	iter := m.CreateIterator()
	return datasource.SourceIterChannel(iter, m.exit)
}

func (m *storeSource) Next() schema.Message {

	select {
	case <-m.exit:
		return nil
	default:
		for {
			o, err := m.iter.Next()
			if err != nil {
				if err == iterator.Done {
					m.complete = true
					return nil
				} else {
					// Should we Retry?
					m.err = err
					return nil
				}
			}
			if o == nil {
				return nil
			}

			m.rowct++

			fi := m.f.fh.File(m.f.path, o)
			u.Debugf("json data: \n%#v", fi.Values())
			return datasource.NewSqlDriverMessageMap(m.rowct, fi.Values(), m.f.fdbcolidx)
		}
	}
}
