package datasource

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
)

func init() {
	//datasource.Register("csv", &datasource.StaticDataSource{})
}

var (
	_            = u.EMPTY
	_ DataSource = (*StaticDataSource)(nil)
	_ SourceConn = (*StaticDataSource)(nil)
	_ Scanner    = (*StaticDataSource)(nil)
	_ Seeker     = (*StaticDataSource)(nil)
	_ Upsert     = (*StaticDataSource)(nil)
)

// Static DataSource, implements qlbridge DataSource to allow
//   in memory native go data to have a Schema and implement
//   other DataSource interfaces such as Open, Close
//
// This implementation uses
//
type StaticDataSource struct {
	name     string
	indexCol int
	exit     <-chan bool
	cursor   int
	data     [][]driver.Value
	index    map[driver.Value]int
	cols     []string
}

func NewStaticDataSource(name string, indexedCol int, data [][]driver.Value, cols []string) *StaticDataSource {
	m := StaticDataSource{name: name, indexCol: indexedCol, data: data, cols: cols}
	m.buildIndex()
	return &m
}
func NewStaticDataValue(data interface{}, name string) *StaticDataSource {
	vals := []driver.Value{data}
	return NewStaticDataSource(name, 0, [][]driver.Value{vals}, []string{name})
}

func (m *StaticDataSource) buildIndex() error {
	m.index = make(map[driver.Value]int, len(m.data))
	for i, vals := range m.data {
		indexVal := vals[m.indexCol]
		if _, exists := m.index[indexVal]; exists {
			return fmt.Errorf("Must have unique values in index column %v", indexVal)
		}
		//u.Infof("index: %v val=%v %v", i, indexVal, m.data[i])
		m.index[indexVal] = i
	}
	return nil
}

func (m *StaticDataSource) Open(connInfo string) (SourceConn, error) { return nil, nil }
func (m *StaticDataSource) Close() error                             { return nil }
func (m *StaticDataSource) CreateIterator(filter expr.Node) Iterator { return m }
func (m *StaticDataSource) Tables() []string                         { return []string{m.name} }
func (m *StaticDataSource) Columns() []string                        { return m.cols }
func (m *StaticDataSource) MesgChan(filter expr.Node) <-chan Message {
	iter := m.CreateIterator(filter)
	return SourceIterChannel(iter, filter, m.exit)
}

func (m *StaticDataSource) Next() Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			if m.cursor >= len(m.data) {
				m.cursor = 0
				return nil
			}
			m.cursor++
			return &SqlDriverMessage{Id: uint64(m.cursor - 1), Vals: m.data[m.cursor-1]}
		}
	}
}

// Implement the Upsert.Put()
func (m *StaticDataSource) Put(row interface{}) error {
	vals, ok := row.([]driver.Value)
	if !ok {
		return fmt.Errorf("Expected []driver.Value but got %T", row)
	}
	indexVal := vals[m.indexCol]
	curIndex, exists := m.index[indexVal]
	if exists {
		m.data[curIndex] = vals
	} else {
		m.index[indexVal] = len(m.data)
		m.data = append(m.data, vals)
	}
	return nil
}

// Implement datasource.Seeker()
func (m *StaticDataSource) CanSeek(sql *expr.SqlSelect) bool {
	return true
}
func (m *StaticDataSource) Get(key driver.Value) Message {
	index, exists := m.index[key]
	if exists {
		return &SqlDriverMessage{m.data[index], uint64(index)}
	}
	return nil
}
func (m *StaticDataSource) MultiGet(keys []driver.Value) []Message {
	return nil
}
