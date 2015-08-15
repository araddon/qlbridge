package mockcsv

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
)

var (
	_ = u.EMPTY

	// Different Features of this MockCsv Data Source
	// - the rest are implemented in the static data source
	//   which has a Static per table
	_ datasource.DataSource = (*MockCsvSource)(nil)

	MockCsvGlobal = NewMockSource()
)

func init() {
	datasource.Register("mockcsv", MockCsvGlobal)
}
func LoadTable(name, csvRaw string) {
	MockCsvGlobal.raw[name] = csvRaw
}

type MockCsvSource struct {
	tablenamelist []string
	tables        map[string]*membtree.StaticDataSource
	raw           map[string]string
}

func NewMockSource() *MockCsvSource {
	return &MockCsvSource{
		tablenamelist: make([]string, 0),
		raw:           make(map[string]string),
		tables:        make(map[string]*membtree.StaticDataSource),
	}
}

func (m *MockCsvSource) Open(tableName string) (datasource.SourceConn, error) {

	if tbl, ok := m.tables[tableName]; ok {
		u.Debugf("found mockcsv table:%q  len=%v", tableName, tbl.Length())
		return tbl, nil
	}
	err := m.loadTable(tableName)
	if err == nil {
		return m.tables[tableName], nil
	} else {
		u.Errorf("could not load table %v", err)
		return nil, err
	}
	return nil, datasource.ErrNotFound
}

func (m *MockCsvSource) loadTable(tableName string) error {

	csvRaw, ok := m.raw[tableName]
	if !ok {
		return datasource.ErrNotFound
	}
	sr := strings.NewReader(csvRaw)
	//u.Debugf("load mockcsv: %q  data:%v", tableName, csvRaw)
	csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
	tbl := membtree.NewStaticData(tableName)
	tbl.SetColumns(csvSource.Columns())
	//u.Infof("set index col for %v: %v -- %v", tableName, 0, csvSource.Columns()[0])
	m.tables[tableName] = tbl

	// Now we are going to page through the Csv Source and Put into
	//  Static Data Source, ie copy into memory
	for {
		msg := csvSource.Next()
		if msg == nil {
			//u.Infof("table:%v  len=%v", tableName, tbl.Length())
			return nil
		}
		dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
		if !ok {
			return fmt.Errorf("Expected []driver.Value but got %T", msg.Body())
		}

		// We don't know the Key
		tbl.Put(nil, nil, dm.Values())
	}
	return nil
}

func (m *MockCsvSource) Close() error     { return nil }
func (m *MockCsvSource) Tables() []string { return m.tablenamelist }
func (m *MockCsvSource) SetTable(tableName, csvRaw string) {
	if _, exists := m.raw[tableName]; !exists {
		m.tablenamelist = append(m.tablenamelist, tableName)
	}
	// Even if it exists, replace it?  Which would not work
	//  because the raw wouldn't get converted to
	m.raw[tableName] = csvRaw
}
