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
	MockSchemaName = "mockcsv"
)

var (
	// Enforce Features of this MockCsv Data Source
	// - the rest are implemented in the static in-memory btree
	_ schema.Source       = (*MockCsvSource)(nil)
	_ schema.Conn         = (*MockCsvTable)(nil)
	_ schema.ConnUpsert   = (*MockCsvTable)(nil)
	_ schema.ConnDeletion = (*MockCsvTable)(nil)

	// Schema  ~= global mock
	//    -> SourceSchema  = "mockcsv"
	//         -> DS = MockCsvSource
	MockCsvGlobal = NewMockSource()
	MockSchema    *schema.Schema
)

func init() {
	//u.SetupLogging("debug")
	//u.SetColorOutput()
	MockSchema = datasource.RegisterSchemaSource(MockSchemaName, MockSchemaName, MockCsvGlobal)
}

// LoadTable MockCsv is used for mocking so has a global data source we can load data into
func LoadTable(schemaName, name, csvRaw string) {
	MockCsvGlobal.CreateTable(name, csvRaw)
	MockSchema.RefreshSchema()
}

// MockCsvSource DataSource for testing
//  - creates an in memory b-tree per "table"
//  - not thread safe
type MockCsvSource struct {
	tablenamelist []string
	tables        map[string]*membtree.StaticDataSource
	raw           map[string]string
}

// MockCsvTable converts the static csv-source into a schema.Conn source
type MockCsvTable struct {
	*membtree.StaticDataSource
}

func NewMockSource() *MockCsvSource {
	return &MockCsvSource{
		tablenamelist: make([]string, 0),
		raw:           make(map[string]string),
		tables:        make(map[string]*membtree.StaticDataSource),
	}
}

func (m *MockCsvSource) Init()                            {}
func (m *MockCsvSource) Setup(*schema.SchemaSource) error { return nil }
func (m *MockCsvSource) Open(tableName string) (schema.Conn, error) {

	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		return &MockCsvTable{StaticDataSource: ds}, nil
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds := m.tables[tableName]
	return &MockCsvTable{StaticDataSource: ds}, nil
}

func (m *MockCsvSource) Table(tableName string) (*schema.Table, error) {

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

func (m *MockCsvSource) loadTable(tableName string) error {

	csvRaw, ok := m.raw[tableName]
	if !ok {
		return schema.ErrNotFound
	}
	sr := strings.NewReader(csvRaw)
	//u.Debugf("mockcsv:%p load mockcsv: %q  data:%v", m, tableName, csvRaw)
	csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
	ds := membtree.NewStaticData(tableName)
	u.Infof("loaded columns %v", csvSource.Columns())
	ds.SetColumns(csvSource.Columns())
	//u.Infof("set index col for %v: %v -- %v", tableName, 0, csvSource.Columns()[0])
	m.tables[tableName] = ds

	// Now we are going to page through the Csv rows and Put into
	//  Static Data Source, ie copy into memory btree structure
	for {
		msg := csvSource.Next()
		if msg == nil {
			//u.Infof("table:%v  len=%v", tableName, ds.Length())
			break
		}
		dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
		if !ok {
			return fmt.Errorf("Expected *datasource.SqlDriverMessageMap but got %T", msg.Body())
		}

		// We don't know the Key
		ds.Put(nil, nil, dm.Values())
	}

	iter := &MockCsvTable{StaticDataSource: ds}
	tbl, err := ds.Table(tableName)
	if err != nil {
		return err
	}
	return datasource.IntrospectTable(tbl, iter)
}

func (m *MockCsvSource) Close() error     { return nil }
func (m *MockCsvSource) Tables() []string { return m.tablenamelist }
func (m *MockCsvSource) CreateTable(tableName, csvRaw string) {
	if _, exists := m.raw[tableName]; !exists {
		m.tablenamelist = append(m.tablenamelist, tableName)
	}
	m.raw[tableName] = csvRaw
	m.loadTable(tableName)
}
