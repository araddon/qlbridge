package membtree

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/google/btree"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

const (
	sourceType = "membtree"
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

type Key struct {
	Id uint64
}

func NewKey(key uint64) *Key     { return &Key{key} }
func (m *Key) Key() driver.Value { return driver.Value(m.Id) }
func (m *Key) Less(than btree.Item) bool {
	switch it := than.(type) {
	case *DriverItem:
		return m.Id < it.Id
	case *Key:
		return m.Id < it.Id
	default:
		u.Warnf("what type? %T", than)
	}
	return false
}

type DriverItem struct {
	*datasource.SqlDriverMessageMap
}

func (m *DriverItem) Less(than btree.Item) bool {
	switch it := than.(type) {
	case *DriverItem:
		return m.Id < it.Id
	case *Key:
		return m.Id < it.Id
	default:
		u.Warnf("what type? %T", than)
	}
	return false
}

func makeId(dv driver.Value) uint64 {
	switch vt := dv.(type) {
	case int:
		return uint64(vt)
	case int64:
		return uint64(vt)
	case []byte:
		return siphash.Hash(0, 1, vt)
		// iv, err := strconv.ParseUint(string(vt), 10, 64)
		// if err != nil {
		// 	u.Warnf("could not create id: %v  for %v", err, dv)
		// }
		// return iv
	case string:
		return siphash.Hash(0, 1, []byte(vt))
		// iv, err := strconv.ParseUint(vt, 10, 64)
		// if err != nil {
		// 	u.Warnf("could not create id: %v  for %v", err, dv)
		// }
		// return iv
	case *Key:
		//u.Infof("got %#v", vt)
		return vt.Id
	case datasource.KeyCol:
		//u.Infof("got %#v", vt)
		return makeId(vt.Val)
	default:
		//u.LogTracef(u.WARN, "wat")
		u.Warnf("not implemented conversion: %T", dv)
	}
	return 0
}

// Static DataSource, implements qlbridge DataSource to allow in memory native go data
//   to have a Schema and implement and be operated on by Sql Operations
//
// Features
// - only a single column may (and must) be identified as the "Indexed" column
// - NOT threadsafe
//
// This is meant as an example of the interfaces of qlbridge DataSources
//
type StaticDataSource struct {
	exit <-chan bool
	*datasource.Schema
	tbl      *datasource.Table
	indexCol int        // Which column position is indexed?  ie primary key
	cursor   btree.Item // cursor position for paging
	//data     [][]driver.Value     // the raw data store
	//index    map[driver.Value]int // Index of primary key value to row-position
	//cols   []string       // List of columns, expected in this order
	//colidx map[string]int // Index of column names to position
	bt *btree.BTree
}

func NewStaticDataSource(name string, indexedCol int, data [][]driver.Value, cols []string) *StaticDataSource {

	sourceSchema := datasource.NewSourceSchema(name, sourceType)
	tbl := datasource.NewTable(name, sourceSchema)
	sourceSchema.AddTable(tbl)
	schema := datasource.NewSchema(name)
	schema.AddSourceSchema(sourceSchema)

	m := StaticDataSource{indexCol: indexedCol}
	m.tbl = tbl
	m.bt = btree.New(32)
	m.Schema = schema
	m.tbl.SetColumns(cols)
	for _, row := range data {
		m.Put(nil, nil, row)
	}
	return &m
}

// StaticDataValue is used
func NewStaticDataValue(name string, data interface{}) *StaticDataSource {
	row := []driver.Value{data}
	ds := NewStaticDataSource(name, 0, [][]driver.Value{row}, []string{name})
	return ds
}
func NewStaticData(name string) *StaticDataSource {
	return NewStaticDataSource(name, 0, make([][]driver.Value, 0), nil)
}

func (m *StaticDataSource) Open(connInfo string) (datasource.SourceConn, error) { return nil, nil }
func (m *StaticDataSource) Close() error                                        { return nil }
func (m *StaticDataSource) CreateIterator(filter expr.Node) datasource.Iterator { return m }
func (m *StaticDataSource) Tables() []string                                    { return []string{m.Schema.Name} }
func (m *StaticDataSource) Columns() []string                                   { return m.tbl.Columns() }
func (m *StaticDataSource) Length() int                                         { return m.bt.Len() }
func (m *StaticDataSource) SetColumns(cols []string)                            { m.tbl.SetColumns(cols) }

//func (m *StaticDataSource) AllData() [][]driver.Value                           { return m.bt.}

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
			var item btree.Item

			if m.cursor == nil {
				m.bt.Ascend(func(a btree.Item) bool {
					item = a
					//u.Debugf("item: %#v", item)
					return false // stop after this
				})
			} else {
				m.bt.AscendGreaterOrEqual(m.cursor, func(a btree.Item) bool {
					if m.cursor == a {
						return true
					}
					item = a
					//u.Debugf("item: %#v", item)
					return false // stop after this
				})
			}

			if item == nil {
				m.cursor = nil
				return nil
			}
			m.cursor = item
			//u.Debugf("return? %T  %v", item, item.(*DriverItem).SqlDriverMessageMap)
			return item.(*DriverItem).SqlDriverMessageMap
			//return datasource.NewSqlDriverMessageMapVals(uint64(m.cursor-1), m.data[m.cursor-1], m.cols)
		}
	}
}

// interface for Upsert.Put()
func (m *StaticDataSource) Put(ctx context.Context, key datasource.Key, row interface{}) (datasource.Key, error) {

	switch rowVals := row.(type) {
	case []driver.Value:
		if len(rowVals) != len(m.Columns()) {
			return nil, fmt.Errorf("Wrong number of columns, got %v expected %v", len(rowVals), len(m.Columns()))
		}
		id := makeId(rowVals[m.indexCol])
		sdm := datasource.NewSqlDriverMessageMap(id, rowVals, m.tbl.FieldPositions)
		item := DriverItem{sdm}
		m.bt.ReplaceOrInsert(&item)
		//u.Debugf("PUT: %#v", rowVals)
		return NewKey(id), nil
	case map[string]driver.Value:
		// We need to convert the key:value to []driver.Value so
		// we need to look up column index for each key, and write to vals

		// TODO:   if this is a partial update, we need to look up vals
		row := make([]driver.Value, len(m.Columns()))
		if len(rowVals) < len(m.Columns()) {
			// How do we get the key?
			//m.Get(key)
		}

		for key, val := range rowVals {
			if keyIdx, ok := m.tbl.FieldPositions[key]; ok {
				row[keyIdx] = val
			} else {
				return nil, fmt.Errorf("Found column in Put that doesn't exist in cols: %v", key)
			}
		}
		id := uint64(0)
		if key == nil {
			if row[m.indexCol] == nil {
				// Since we do not have an indexed column to work off of,
				// the ideal would be to get the job builder/planner to do
				// a scan with whatever info we have and feed that in?   Instead
				// of us implementing our own scan?
				u.Warnf("wtf, nil key? %v %v", m.indexCol, row)
				return nil, fmt.Errorf("cannot update on non index column ")
			}
			id = makeId(row[m.indexCol])
		} else {
			id = makeId(key)
			sdm, _ := m.Get(key)
			//u.Debugf("sdm: %#v  err%v", sdm, err)
			if sdm != nil {
				if dmval, ok := sdm.Body().(*datasource.SqlDriverMessageMap); ok {
					for i, val := range dmval.Values() {
						if row[i] == nil {
							row[i] = val
						}
					}
				}
			}
		}
		//u.Debugf("PUT: %#v", row)
		//u.Infof("PUT: %v  key:%v  row:%v", id, key, row)
		sdm := datasource.NewSqlDriverMessageMap(id, row, m.tbl.FieldPositions)
		item := DriverItem{sdm}
		m.bt.ReplaceOrInsert(&item)
		return NewKey(id), nil
	default:
		u.Warnf("not implemented %T", row)
		return nil, fmt.Errorf("Expected []driver.Value but got %T", row)
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
	item := m.bt.Get(NewKey(makeId(key)))
	if item != nil {
		return item.(*DriverItem).SqlDriverMessageMap, nil
	}
	return nil, datasource.ErrNotFound // Should not found be an error?
}

func (m *StaticDataSource) MultiGet(keys []driver.Value) ([]datasource.Message, error) {
	rows := make([]datasource.Message, len(keys))
	for i, key := range keys {
		item := m.bt.Get(NewKey(makeId(key)))
		if item == nil {
			return nil, datasource.ErrNotFound
		}
		rows[i] = item.(*DriverItem).SqlDriverMessageMap
	}
	return rows, nil
}

// Interface for Deletion
func (m *StaticDataSource) Delete(key driver.Value) (int, error) {
	item := m.bt.Delete(NewKey(makeId(key)))
	if item == nil {
		//u.Warnf("could not delete: %v", key)
		return 0, datasource.ErrNotFound
	}
	return 1, nil
}

// Delete using a Where Expression
func (m *StaticDataSource) DeleteExpression(where expr.Node) (int, error) {
	//return 0, fmt.Errorf("not implemented")
	evaluator := vm.Evaluator(where)
	deletedKeys := make([]*Key, 0)
	m.bt.Ascend(func(a btree.Item) bool {
		di, ok := a.(*DriverItem)
		if !ok {
			u.Warnf("wat?  %T   %#v", a, a)
			return false
		}
		msgCtx := di.SqlDriverMessageMap
		whereValue, ok := evaluator(msgCtx)
		if !ok {
			u.Debugf("could not evaluate where: %v", msgCtx.Values())
			//return deletedCt, fmt.Errorf("Could not evaluate where clause")
			return true
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				//this means do NOT delete
			} else {
				// Delete!
				indexVal := msgCtx.Values()[m.indexCol]
				deletedKeys = append(deletedKeys, NewKey(makeId(indexVal)))
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
		return true
	})

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
