package datasource

import (
	"database/sql/driver"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
)

func init() {
	//datasource.Register("csv", &datasource.StaticDataSource{})
}

var (
	_            = u.EMPTY
	_ DataSource = (*StaticDataSource)(nil)
	_ Scanner    = (*StaticDataSource)(nil)
)

// Static DataStoure, implements qlbridge DataSource to scan through data
//   native go static data such as arrays or maps
//
type StaticDataSource struct {
	exit   <-chan bool
	cursor int
	data   [][]driver.Value
	cols   []string
}

func NewStaticDataSource(data [][]driver.Value, cols []string) *StaticDataSource {
	m := StaticDataSource{data: data, cols: cols}
	return &m
}
func NewStaticDataValue(data interface{}, name string) *StaticDataSource {
	vals := make([]driver.Value, 1)
	vals[0] = driver.Value(data)
	m := StaticDataSource{data: [][]driver.Value{vals}, cols: []string{name}}
	return &m
}

func (m *StaticDataSource) Open(connInfo string) (DataSource, error) { return nil, nil }
func (m *StaticDataSource) Close() error                             { return nil }
func (m *StaticDataSource) CreateIterator(filter expr.Node) Iterator { return m }

func (m *StaticDataSource) Next() Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			if m.cursor >= len(m.data) {
				return nil
			}
			m.cursor++
			return &SqlDriverMessage{id: uint64(m.cursor - 1), vals: m.data[m.cursor-1]}
		}

	}

}
