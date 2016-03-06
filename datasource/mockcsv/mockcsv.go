package mockcsv

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/schema"
)

var (
	// Enforce Features of this MockCsv Data Source
	// - the rest are implemented in the static in-memory btree
	_ schema.DataSource = (*MockCsvSource)(nil)
	_ schema.Upsert     = (*MockCsvTable)(nil)
	_ schema.Deletion   = (*MockCsvTable)(nil)

	MockCsvGlobal = NewMockSource()
)

func init() {
	//u.SetupLogging("debug")
	//u.SetColorOutput()
	datasource.Register("mockcsv", MockCsvGlobal)
}

// MockCsv is used for mocking so has a global data source we can load data into
func LoadTable(name, csvRaw string) {
	MockCsvGlobal.CreateTable(name, csvRaw)
}

// Mock Data source for testing
//  - creates an in memory b-tree per "table"
//  - not thread safe
type MockCsvSource struct {
	tablenamelist []string
	tables        map[string]*membtree.StaticDataSource
	raw           map[string]string
}

// A table
type MockCsvTable struct {
	*membtree.StaticDataSource
	//insert *rel.SqlInsert
}

func NewMockSource() *MockCsvSource {
	return &MockCsvSource{
		tablenamelist: make([]string, 0),
		raw:           make(map[string]string),
		tables:        make(map[string]*membtree.StaticDataSource),
	}
}

func (m *MockCsvSource) Open(tableName string) (schema.SourceConn, error) {

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
	u.Debugf("mockcsv:%p load mockcsv: %q  data:%v", m, tableName, csvRaw)
	csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
	tbl := membtree.NewStaticData(tableName)
	u.Infof("loaded columns %v", csvSource.Columns())
	tbl.SetColumns(csvSource.Columns())
	//u.Infof("set index col for %v: %v -- %v", tableName, 0, csvSource.Columns()[0])
	m.tables[tableName] = tbl

	// Now we are going to page through the Csv rows and Put into
	//  Static Data Source, ie copy into memory btree structure
	for {
		msg := csvSource.Next()
		if msg == nil {
			//u.Infof("table:%v  len=%v", tableName, tbl.Length())
			return nil
		}
		dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
		if !ok {
			return fmt.Errorf("Expected *datasource.SqlDriverMessageMap but got %T", msg.Body())
		}

		// We don't know the Key
		tbl.Put(nil, nil, dm.Values())
	}
	return nil
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
