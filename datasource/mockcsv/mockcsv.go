// Mockcsv implements an in-memory csv data source for testing usage
// implemented by wrapping the mem-b-tree.
package mockcsv

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/schema"
)

const (
	// SchemaName is "mockcsv"
	SchemaName = "mockcsv"
)

var (
	// Ensure this Csv Data Source implements expected interfaces
	_ schema.Source       = (*Source)(nil)
	_ schema.Alter        = (*Source)(nil)
	_ schema.Conn         = (*Table)(nil)
	_ schema.ConnUpsert   = (*Table)(nil)
	_ schema.ConnDeletion = (*Table)(nil)

	// CsvGlobal mock csv in mem store
	CsvGlobal = New()
	// Schema the mock schema
	sch *schema.Schema
)

// Schema global accessor to the mockcsv schema
func Schema() *schema.Schema {
	if sch != nil {
		return sch
	}
	if err := schema.RegisterSourceAsSchema(SchemaName, CsvGlobal); err != nil {
		panic(fmt.Sprintf("Could not read schema %v", err))
	}
	sch, _ = schema.DefaultRegistry().Schema(SchemaName)
	return sch
}

// LoadTable MockCsv is used for mocking so has a global data source we can load data into
func LoadTable(schemaName, name, csvRaw string) {
	CsvGlobal.CreateTable(name, csvRaw)
	schema.DefaultRegistry().SchemaRefresh(SchemaName)
}

// Source DataSource for testing creates an in memory b-tree per "table".
// Is not thread safe.
type Source struct {
	s             *schema.Schema
	tablenamelist []string
	tables        map[string]*membtree.StaticDataSource
	raw           map[string]string
}

// Table converts the static csv-source into a schema.Conn source
type Table struct {
	*membtree.StaticDataSource
}

// New create csv mock source.
func New() *Source {
	return &Source{
		tablenamelist: make([]string, 0),
		raw:           make(map[string]string),
		tables:        make(map[string]*membtree.StaticDataSource),
	}
}

// Init no-op meets interface
func (m *Source) Init() {}

// Setup accept schema
func (m *Source) Setup(s *schema.Schema) error {
	m.s = s
	return nil
}

// DropTable Drop table schema
func (m *Source) DropTable(t string) error {
	delete(m.raw, t)
	delete(m.tables, t)
	names := make([]string, 0, len(m.tables))
	for tableName, _ := range m.raw {
		names = append(names, tableName)
	}
	m.tablenamelist = names
	return nil
}

// Open open connection to given tablename.
func (m *Source) Open(tableName string) (schema.Conn, error) {

	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		return &Table{StaticDataSource: ds}, nil
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds := m.tables[tableName]
	return &Table{StaticDataSource: ds}, nil
}

// Table get table
func (m *Source) Table(tableName string) (*schema.Table, error) {

	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		return ds.Table(tableName)
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds, ok := m.tables[tableName]
	if !ok {
		return nil, schema.ErrNotFound
	}
	return ds.Table(tableName)
}

func (m *Source) loadTable(tableName string) error {

	csvRaw, ok := m.raw[tableName]
	if !ok {
		return schema.ErrNotFound
	}
	sr := strings.NewReader(csvRaw)
	//u.Debugf("mockcsv:%p load mockcsv: %q  data:%v", m, tableName, csvRaw)
	csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
	ds := membtree.NewStaticData(tableName)
	u.Infof("loaded columns table=%q cols=%v", tableName, csvSource.Columns())
	ds.SetColumns(csvSource.Columns())
	m.tables[tableName] = ds

	// Now we are going to page through the Csv rows and Put into
	// Static Data Source, ie copy into memory btree structure
	for {
		msg := csvSource.Next()
		if msg == nil {
			break
		}
		dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
		if !ok {
			return fmt.Errorf("Expected *datasource.SqlDriverMessageMap but got %T", msg.Body())
		}

		// We don't know the Key
		ds.Put(nil, nil, dm.Values())
	}

	iter := &Table{StaticDataSource: ds}
	tbl, err := ds.Table(tableName)
	if err != nil {
		return err
	}
	return datasource.IntrospectTable(tbl, iter)
}

// Close csv source.
func (m *Source) Close() error { return nil }

// Tables list of tables.
func (m *Source) Tables() []string { return m.tablenamelist }

// CreateTable create a csv table in this source.
func (m *Source) CreateTable(tableName, csvRaw string) {
	if _, exists := m.raw[tableName]; !exists {
		m.tablenamelist = append(m.tablenamelist, tableName)
	}
	m.raw[tableName] = csvRaw
	m.loadTable(tableName)
}
