package inmemmap

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	// Different Features of this Static Data Source
	_ datasource.DataSource = (*StaticDataSource)(nil)
	_ datasource.SourceConn = (*StaticDataSource)(nil)
	_ datasource.Scanner    = (*StaticDataSource)(nil)
	_ datasource.Seeker     = (*StaticDataSource)(nil)
	_ datasource.Upsert     = (*StaticDataSource)(nil)
	_ datasource.Deletion   = (*StaticDataSource)(nil)
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
	exit     <-chan bool
	name     string               // Name, ie "table"
	indexCol int                  // Which column position is indexed?  ie primary key
	cursor   int                  // cursor position for paging
	data     [][]driver.Value     // the raw data store
	index    map[driver.Value]int // Index of primary key value to row-position
	cols     []string             // List of columns, expected in this order
	colidx   map[string]int       // Index of column names to position
}

func NewStaticDataSource(name string, indexedCol int, data [][]driver.Value, cols []string) *StaticDataSource {
	m := StaticDataSource{name: name, indexCol: indexedCol, data: data}
	m.SetColumns(cols)
	m.buildIndex()
	return &m
}

// StaticDataValue is used
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
func (m *StaticDataSource) SetColumns(cols []string) {
	m.cols = cols
	colidx := make(map[string]int, len(cols))
	for idx, col := range cols {
		colidx[col] = idx
	}
	m.colidx = colidx
}
func (m *StaticDataSource) Open(connInfo string) (datasource.SourceConn, error) { return nil, nil }
func (m *StaticDataSource) Close() error                                        { return nil }
func (m *StaticDataSource) CreateIterator(filter expr.Node) datasource.Iterator { return m }
func (m *StaticDataSource) Tables() []string                                    { return []string{m.name} }
func (m *StaticDataSource) Columns() []string                                   { return m.cols }
func (m *StaticDataSource) AllData() [][]driver.Value                           { return m.data }
func (m *StaticDataSource) Length() int                                         { return len(m.data) }
func (m *StaticDataSource) MesgChan(filter expr.Node) <-chan datasource.Message {
	iter := m.CreateIterator(filter)
	return datasource.SourceIterChannel(iter, filter, m.exit)
}

func (m *StaticDataSource) Next() datasource.Message {
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
			return datasource.NewSqlDriverMessageMapVals(uint64(m.cursor-1), m.data[m.cursor-1], m.cols)
		}
	}
}

// interface for Upsert.Put()
func (m *StaticDataSource) Put(ctx context.Context, key datasource.Key, row interface{}) (datasource.Key, error) {
	// TODO:
	//  - check key for existence
	var vals []driver.Value
	var curIndex int
	var indexVal driver.Value
	var exists bool
	if key != nil {
		indexVal = key.Key()
		curIndex, exists = m.index[indexVal]
		if exists {
			vals = m.data[curIndex]
		} else {
			u.Infof("had key but not found: %#v", key)
		}
	}
	switch rowVals := row.(type) {
	case []driver.Value:
		if len(rowVals) != len(m.cols) {
			return nil, fmt.Errorf("Wrong number of columns, got %v expected %v", len(rowVals), len(m.cols))
		}
		vals = rowVals
	case map[string]driver.Value:
		if !exists {
			vals = make([]driver.Value, len(m.cols))
			u.Debugf("does not exist? %#v", key)
		}
		for key, val := range rowVals {
			if keyIdx, ok := m.colidx[key]; ok {
				vals[keyIdx] = val
			} else {
				return nil, fmt.Errorf("Found column in Put that doesn't exist in cols: %v", key)
			}
		}
	default:
		return nil, fmt.Errorf("Expected []driver.Value but got %T", row)
	}
	if !exists {
		indexVal = vals[m.indexCol]
		// Double Check existence, as we could have nil key but already exists
		curIndex, exists = m.index[indexVal]
	}
	if exists {
		m.data[curIndex] = vals
		u.Infof("update set %v at:%d to %v", indexVal, len(m.data)-1, vals)
	} else {
		m.index[indexVal] = len(m.data)
		m.data = append(m.data, vals)
		u.Infof("new set %v at:%d to %v", indexVal, len(m.data)-1, vals)
	}
	return nil, nil
}
func (m *StaticDataSource) PutMulti(ctx context.Context, keys []datasource.Key, src interface{}) ([]datasource.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

// interface for Seeker
func (m *StaticDataSource) CanSeek(sql *expr.SqlSelect) bool {
	return true
}

func (m *StaticDataSource) Get(key driver.Value) (datasource.Message, error) {
	index, exists := m.index[key]
	if exists {
		return &datasource.SqlDriverMessage{m.data[index], uint64(index)}, nil
	}
	return nil, datasource.ErrNotFound // Should not found be an error?
}

func (m *StaticDataSource) MultiGet(keys []driver.Value) ([]datasource.Message, error) {
	rows := make([]datasource.Message, len(keys))
	for i, key := range keys {
		index, exists := m.index[key]
		if !exists {
			return nil, datasource.ErrNotFound
		}
		rows[i] = &datasource.SqlDriverMessage{m.data[index], uint64(index)}
	}
	return rows, nil
}

// Interface for Deletion
func (m *StaticDataSource) Delete(key driver.Value) (int, error) {
	index, exists := m.index[key]
	if !exists {
		return 0, datasource.ErrNotFound
	}
	delete(m.index, key)
	//u.Debugf("index:%v  len(m.data)%d", index, len(m.data))
	if index >= (len(m.data) + 1) {
		m.data = append(m.data[:index])
	} else {
		m.data = append(m.data[:index], m.data[index+1:]...)
	}
	for key, idx := range m.index {
		if idx > index {
			m.index[key] = idx - 1
		}
	}
	return 1, nil
}
func (m *StaticDataSource) DeleteExpression(where expr.Node) (int, error) {
	evaluator := vm.Evaluator(where)
	deletedKeys := make([]driver.Value, 0)
	for idx, row := range m.data {
		msgCtx := datasource.NewSqlDriverMessageMapVals(uint64(idx), row, m.cols)
		whereValue, ok := evaluator(msgCtx)
		if !ok {
			u.Debugf("could not evaluate where: %v   %v", idx, msgCtx.Values())
			//return deletedCt, fmt.Errorf("Could not evaluate where clause")
			continue
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				//this means do NOT delete
			} else {
				// Delete!
				indexVal := row[m.indexCol]
				deletedKeys = append(deletedKeys, indexVal)
			}
		case nil:
			// ??
		default:
			if whereVal.Nil() {
				// Doesn't match, so don't delete
			} else {
				u.Warnf("unknown type? %T", whereVal)
			}
		}
	}
	for _, deleteKey := range deletedKeys {
		//u.Debugf("calling delete: %v", deleteKey)
		if ct, err := m.Delete(deleteKey); err != nil {
			u.Errorf("Could not delete key: %v", deleteKey)
		} else if ct != 1 {
			u.Errorf("delete should have removed 1 key %v", deleteKey)
		}
	}
	return len(deletedKeys), nil
}
