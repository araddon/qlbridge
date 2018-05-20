// sqlite implements a Qlbridge Datasource interface around sqlite.
package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	// Import Sqlite driver
	_ "github.com/mattn/go-sqlite3"

	"github.com/araddon/qlbridge/expr"
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
	// ensure our Source implements connection features
	_ schema.Conn = (*Source)(nil)
)

// Source implements qlbridge DataSource to a sqlite file based source.
//
// Features
// - Support full predicate push down to SqlLite.
// - Support Thread-Safe wrapper around sqlite file.
type Source struct {
	exit      <-chan bool
	schema    *schema.Schema
	file      string // Local file path to sqlite db
	db        *sql.DB
	mu        sync.Mutex
	source    *Source
	qryconns  map[string]*qryconn
	tables    map[string]*schema.Table
	tblmu     sync.Mutex
	tableList []string
}

func newSourceEmtpy() schema.Source {
	return &Source{
		qryconns:  make(map[string]*qryconn),
		tables:    make(map[string]*schema.Table),
		tableList: make([]string, 0),
	}
}

// Type describes this source as SourceType = "sqlite"
func (m *Source) Type() string { return SourceType }

// Setup this source with schema from parent.
func (m *Source) Setup(s *schema.Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u.Debugf("got new sqlite schema %s", s.Name)
	m.schema = s
	if m.db != nil {
		return nil
	}

	m.file = s.Conf.Settings["file"]
	if m.file == "" {
		m.file = fmt.Sprintf("/tmp/%s.sql.db", s.Name)
		u.Warnf("using tmp? %q", m.file)
	}

	// It will be created if it doesn't exist.
	//   "./source.enriched.db"
	db, err := sql.Open("sqlite3", m.file)
	if err != nil {
		u.Errorf("could not open %q err=%v", m.file, err)
		return err
	}
	err = db.Ping()
	if err != nil {
		u.Errorf("could not ping %q err=%v", m.file, err)
		return err
	}
	m.db = db

	// SELECT * FROM dbname.sqlite_master WHERE type='table';
	rows, err := db.Query("SELECT tbl_name, sql FROM sqlite_master WHERE type='table';")
	if err != nil {
		u.Errorf("could not open master err=%v", err)
		return err
	}
	var name, sql string
	for rows.Next() {
		rows.Scan(&name, &sql)
		name = strings.ToLower(name)
		t := tableFromSQL(name, sql)
		m.tables[name] = t
		m.tableList = append(m.tableList, name)
	}
	rows.Close()

	// if err := datasource.IntrospectTable(m.tbl, m.CreateIterator()); err != nil {
	// 	u.Errorf("Could not introspect schema %v", err)
	// }

	return nil
}

// Init the source
func (m *Source) Init() {}

// Open a connection, since sqlite is not threadsafe, this is locked.
func (m *Source) Open(table string) (schema.Conn, error) {
	//u.Infof("Open conn=%q", table)
	m.tblmu.Lock()
	t, ok := m.tables[table]
	m.tblmu.Unlock()
	if !ok {
		return nil, schema.ErrNotFound
	}
	m.mu.Lock()
	//u.Infof("after open lock")
	qc := newQueryConn(t, m)
	m.qryconns[table] = qc
	return qc, nil
}

// Table gets table schema for given table
func (m *Source) Table(table string) (*schema.Table, error) {
	u.Infof("source.Table(%q)", table)
	m.tblmu.Lock()
	t, ok := m.tables[table]
	m.tblmu.Unlock()
	if !ok {
		return nil, schema.ErrNotFound
	}
	return t, nil
}

// Tables gets list of tables
func (m *Source) Tables() []string { return m.tableList }

// Close this source, closing the underlying sqlite db file
func (m *Source) Close() error {
	if m.db != nil {
		err := m.db.Close()
		if err != nil {
			return err
		}
		m.db = nil
	}
	return nil
}

func tableFromSQL(name, sqls string) *schema.Table {
	t := schema.NewTable(name)
	//u.Debugf("%s  %v", name, sqls)
	cols := strings.Split(sqls, "\n")
	cols = cols[1 : len(cols)-1]
	for _, cols := range cols {
		parts := strings.Split(strings.Trim(cols, " \t,"), " ")
		if len(parts) < 2 {
			continue
		}
		colName := expr.IdentityTrim(parts[0])
		// NewFieldBase(name string, valType value.ValueType, size int, desc string)
		t.AddField(schema.NewFieldBase(colName, TypeFromString(parts[1]), 255, ""))
		// u.Debugf("%d  %v", i, parts)
		// u.Debugf("%q", expr.IdentityTrim(parts[0]))
	}
	t.SetColumnsFromFields()
	return t
}
