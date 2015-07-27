package datasource

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Different Features of this Static Data Source
	_ DataSource = (*StaticDataSource)(nil)
	_ SourceConn = (*StaticDataSource)(nil)
	_ Scanner    = (*StaticDataSource)(nil)
	_ Seeker     = (*StaticDataSource)(nil)
	_ Upsert     = (*StaticDataSource)(nil)
	_ Deletion   = (*StaticDataSource)(nil)
)

// Static DataSource, implements qlbridge DataSource to allow in memory native go data
//   to have a Schema and implement and be operated on by Sql Operations
//
// Features
// - only a single column may (and must) be identified as the "Indexed" column
//
// This is meant as an example of the interfaces of qlbridge DataSources
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
func NewStaticData(name string) *StaticDataSource {
	return NewStaticDataSource(name, 0, make([][]driver.Value, 0), nil)
}

func (m *StaticDataSource) buildIndex() error {
	m.index = make(map[driver.Value]int, len(m.data))
	for i, vals := range m.data {
		indexVal := vals[m.indexCol]
		if _, exists := m.index[indexVal]; exists {
			return fmt.Errorf("Must have unique values in index column %v", indexVal)
		}
		m.index[indexVal] = i
	}
	return nil
}

func (m *StaticDataSource) Open(connInfo string) (SourceConn, error) { return nil, nil }
func (m *StaticDataSource) Close() error                             { return nil }
func (m *StaticDataSource) CreateIterator(filter expr.Node) Iterator { return m }
func (m *StaticDataSource) Tables() []string                         { return []string{m.name} }
func (m *StaticDataSource) Columns() []string                        { return m.cols }
func (m *StaticDataSource) SetColumns(cols []string)                 { m.cols = cols }
func (m *StaticDataSource) AllData() [][]driver.Value                { return m.data }
func (m *StaticDataSource) Length() int                              { return len(m.data) }
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
			row := m.data[m.cursor-1]
			vals := make(map[string]driver.Value, len(row))
			for i, val := range row {
				//u.Debugf("col: %d : %v", i, row[i])
				vals[m.cols[i]] = val
			}
			return &SqlDriverMessageMap{Id: uint64(m.cursor - 1), Vals: vals, row: row}
		}
	}
}

// interface for Upsert
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
		//u.Infof("set %v at:%d to %v", indexVal, len(m.data)-1, vals)
	}
	return nil
}

// interface for Seeker
func (m *StaticDataSource) CanSeek(sql *expr.SqlSelect) bool {
	return true
}

func (m *StaticDataSource) Get(key driver.Value) (Message, error) {
	index, exists := m.index[key]
	if exists {
		return &SqlDriverMessage{m.data[index], uint64(index)}, nil
	}
	return nil, ErrNotFound // Should not found be an error?
}

func (m *StaticDataSource) MultiGet(keys []driver.Value) ([]Message, error) {
	rows := make([]Message, len(keys))
	for i, key := range keys {
		index, exists := m.index[key]
		if !exists {
			return nil, ErrNotFound
		}
		rows[i] = &SqlDriverMessage{m.data[index], uint64(index)}
	}
	return rows, nil
}

// Interface for Deletion
func (m *StaticDataSource) Delete(key driver.Value) (int, error) {
	index, exists := m.index[key]
	if !exists {
		return 0, ErrNotFound
	}
	delete(m.index, key)
	m.data = append(m.data[:index], m.data[index+1:]...)
	return 1, nil
}
func (m *StaticDataSource) DeleteExpression(expr expr.Node) (int, error) {
	return 0, fmt.Errorf("Not implemented")
}
