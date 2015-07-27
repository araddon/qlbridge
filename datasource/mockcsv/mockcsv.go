package mockcsv

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

var (
	_ = u.EMPTY

	// Different Features of this Static Data Source
	// - the rest are implemented in the static data source
	//   which is returned per "table"
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
	tables map[string]*datasource.StaticDataSource
	raw    map[string]string
}

func NewMockSource() *MockCsvSource {
	return &MockCsvSource{
		raw:    make(map[string]string),
		tables: make(map[string]*datasource.StaticDataSource),
	}
}
func (m *MockCsvSource) Open(tableName string) (datasource.SourceConn, error) {
	//u.Debugf("MockCsv Open: %q", tableName)
	if tbl, ok := m.tables[tableName]; ok {
		return tbl, nil
	} else if csvRaw, ok := m.raw[tableName]; ok {
		sr := strings.NewReader(csvRaw)
		//u.Debugf("open mockcsv: %v  data:%v", tableName, csvRaw)
		csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
		tbl := datasource.NewStaticData(tableName)
		tbl.SetColumns(csvSource.Columns())
		m.tables[tableName] = tbl
		for {
			msg := csvSource.Next()
			if msg == nil {
				return tbl, nil
			}
			dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
			if !ok {
				return nil, fmt.Errorf("Expected []driver.Value but got %T", msg.Body())
			}
			tbl.Put(dm.Values())
		}
	}
	return nil, datasource.ErrNotFound
}
func (m *MockCsvSource) Close() error { return nil }
func (m *MockCsvSource) Tables() []string {
	tbls := make([]string, 0, len(m.tables))
	for tblName, _ := range m.tables {
		tbls = append(tbls, tblName)
	}
	return tbls
}
func (m *MockCsvSource) SetTable(name, csvRaw string) {
	m.raw[name] = csvRaw
}
