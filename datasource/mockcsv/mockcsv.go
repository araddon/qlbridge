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
	tables map[string]*membtree.StaticDataSource
	raw    map[string]string
}

func NewMockSource() *MockCsvSource {
	return &MockCsvSource{
		raw:    make(map[string]string),
		tables: make(map[string]*membtree.StaticDataSource),
	}
}
func (m *MockCsvSource) Open(tableName string) (datasource.SourceConn, error) {
	//u.Debugf("MockCsv Open: %q", tableName)
	if tbl, ok := m.tables[tableName]; ok {
		u.Debugf("found tbl: %v  %v", tableName, tbl.Length())
		return tbl, nil
	} else if csvRaw, ok := m.raw[tableName]; ok {
		sr := strings.NewReader(csvRaw)
		u.Debugf("open mockcsv: %v  data:%v", tableName, csvRaw)
		csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
		tbl := membtree.NewStaticData(tableName)
		tbl.SetColumns(csvSource.Columns())
		u.Infof("set index col for %v: %v -- %v", tableName, 0, csvSource.Columns()[0])
		m.tables[tableName] = tbl
		for {
			msg := csvSource.Next()
			if msg == nil {
				u.Infof("table:%v  len=%v", tableName, tbl.Length())
				return tbl, nil
			}
			dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
			if !ok {
				return nil, fmt.Errorf("Expected []driver.Value but got %T", msg.Body())
			}
			tbl.Put(nil, nil, dm.Values())
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
