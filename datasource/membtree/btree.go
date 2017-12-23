// Membtree implements a Datasource in-memory implemenation
// using the google btree.
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
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

const (
	sourceType = "membtree"
)

var (
	// Different Features of this Static Data Source
	_ schema.Source       = (*StaticDataSource)(nil)
	_ schema.Conn         = (*StaticDataSource)(nil)
	_ schema.ConnColumns  = (*StaticDataSource)(nil)
	_ schema.ConnScanner  = (*StaticDataSource)(nil)
	_ schema.ConnSeeker   = (*StaticDataSource)(nil)
	_ schema.ConnUpsert   = (*StaticDataSource)(nil)
	_ schema.ConnDeletion = (*StaticDataSource)(nil)
)

// Key implements Key and Sort interfaces.
type Key struct {
	Id uint64
}

func NewKey(key uint64) *Key     { return &Key{key} }
func (m *Key) Key() driver.Value { return driver.Value(m.Id) }
func (m *Key) Less(than btree.Item) bool {
	switch it := than.(type) {
	case *DriverItem:
		return m.Id < it.IdVal
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
		//u.Infof("Less? %p:%p less?%v gt?%v  %v vs %v   thanT:%T", m, than, m.IdVal < it.IdVal, m.IdVal > it.IdVal, m.IdVal, it.IdVal, than)
		return m.IdVal < it.IdVal
	case *Key:
		return m.IdVal < it.Id
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
	case nil:
		return 0
	default:
		u.LogTracef(u.WARN, "no id conversion for type")
		u.Warnf("not implemented conversion: %T", dv)
	}
	return 0
}

// StaticDataSource implements qlbridge DataSource to allow in memory native go data
// to have a Schema and implement and be operated on by Sql Operations
//
// Features
// - only a single column may (and must) be identified as the "Indexed" column
// - NOT threadsafe
// - each StaticDataSource = a single Table
type StaticDataSource struct {
	exit     <-chan bool
	name     string
	tbl      *schema.Table
	indexCol int        // Which column position is indexed?  ie primary key
	cursor   btree.Item // cursor position for paging
	bt       *btree.BTree
	max      int
}

func NewStaticDataSource(name string, indexedCol int, data [][]driver.Value, cols []string) *StaticDataSource {

	// This source schema is a single table
	tbl := schema.NewTable(name)

	m := StaticDataSource{indexCol: indexedCol, name: name}
	m.tbl = tbl
	m.bt = btree.New(32)
	m.tbl.SetColumns(cols)
	for _, row := range data {
		m.Put(nil, nil, row)
	}
	return &m
}

// StaticDataValue is used to create a static name=value pair that matches
//   DataSource interfaces
func NewStaticDataValue(name string, data interface{}) *StaticDataSource {
	row := []driver.Value{data}
	ds := NewStaticDataSource(name, 0, [][]driver.Value{row}, []string{name})
	return ds
}
func NewStaticData(name string) *StaticDataSource {
	return NewStaticDataSource(name, 0, make([][]driver.Value, 0), nil)
}

func (m *StaticDataSource) Init()                                     {}
func (m *StaticDataSource) Setup(*schema.Schema) error                { return nil }
func (m *StaticDataSource) Open(connInfo string) (schema.Conn, error) { return m, nil }
func (m *StaticDataSource) Table(table string) (*schema.Table, error) { return m.tbl, nil }
func (m *StaticDataSource) Close() error                              { return nil }
func (m *StaticDataSource) CreateIterator() schema.Iterator           { return m }
func (m *StaticDataSource) Tables() []string                          { return []string{m.name} }
func (m *StaticDataSource) Columns() []string                         { return m.tbl.Columns() }
func (m *StaticDataSource) Length() int                               { return m.bt.Len() }
func (m *StaticDataSource) SetColumns(cols []string)                  { m.tbl.SetColumns(cols) }

func (m *StaticDataSource) Next() schema.Message {
	//u.Infof("Next()")
	select {
	case <-m.exit:
		return nil
	default:
		for {
			var item btree.Item

			if m.cursor == nil {
				//u.Infof("create new Ascend len=%d", m.Length())
				m.max = 0
				m.bt.Ascend(func(a btree.Item) bool {
					item = a
					//u.Debugf("first  item btreeP:%p itemP:%p cursorP:%p  %#v", m, item, m.cursor, item)
					return false // stop after this
				})
			} else {
				m.bt.AscendGreaterOrEqual(m.cursor, func(a btree.Item) bool {
					if m.cursor == a {
						//u.Debugf("equal, return true ie continue")
						item = nil
						return true
					}
					item = a
					//u.Debugf("found  item btreeP:%p itemP:%p cursorP:%p  %#v", m, item, m.cursor, item)
					return false // stop after this
				})
			}
			m.max++
			// if m.max > 20 {
			// 	return nil
			// }

			if item == nil {
				//u.Debugf("reset cursor to nil  %#v", item)
				m.cursor = nil
				return nil
			}
			m.cursor = item
			msg := item.(*DriverItem)
			//u.Infof("return item btreeP:%p itemP:%p cursorP:%p  %v %v", m, item, m.cursor, msg.Id(), msg.Values())
			//u.Debugf("return? %T  %v", item, item.(*DriverItem).SqlDriverMessageMap)
			return msg.SqlDriverMessageMap.Copy()
			//return datasource.NewSqlDriverMessageMapVals(uint64(m.cursor-1), m.data[m.cursor-1], m.cols)
		}
	}
}

// interface for Upsert.Put()
func (m *StaticDataSource) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {

	//u.Infof("%p Put(),  row:%#v", m, row)
	switch rowVals := row.(type) {
	case []driver.Value:
		if len(rowVals) != len(m.Columns()) {
			u.Warnf("wrong column ct")
			return nil, fmt.Errorf("Wrong number of columns, got %v expected %v", len(rowVals), len(m.Columns()))
		}
		id := makeId(rowVals[m.indexCol])
		sdm := datasource.NewSqlDriverMessageMap(id, rowVals, m.tbl.FieldPositions)
		item := DriverItem{sdm}
		itemResult := m.bt.ReplaceOrInsert(&item)
		if itemResult != nil {
			//u.Errorf("could not insert? %#v", itemResult)
		}
		//u.Debugf("%p  PUT: id:%v IdVal:%v  Id():%v vals:%#v", m, id, sdm.IdVal, sdm.Id(), rowVals)
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
}

func (m *StaticDataSource) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

// interface for Seeker
func (m *StaticDataSource) CanSeek(sql *rel.SqlSelect) bool {
	return true
}

func (m *StaticDataSource) Get(key driver.Value) (schema.Message, error) {
	item := m.bt.Get(NewKey(makeId(key)))
	if item != nil {
		return item.(*DriverItem).SqlDriverMessageMap, nil
	}
	return nil, schema.ErrNotFound // Should not found be an error?
}

func (m *StaticDataSource) MultiGet(keys []driver.Value) ([]schema.Message, error) {
	rows := make([]schema.Message, len(keys))
	for i, key := range keys {
		item := m.bt.Get(NewKey(makeId(key)))
		if item == nil {
			return nil, schema.ErrNotFound
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
		return 0, schema.ErrNotFound
	}
	return 1, nil
}

// DeleteExpression Delete using a Where Expression
func (m *StaticDataSource) DeleteExpression(p interface{}, where expr.Node) (int, error) {

	_, ok := p.(*plan.Delete)
	if !ok {
		return 0, plan.ErrNoPlan
	}

	deletedKeys := make([]*Key, 0)
	m.bt.Ascend(func(a btree.Item) bool {
		di, ok := a.(*DriverItem)
		if !ok {
			u.Warnf("wat?  %T   %#v", a, a)
			return false
		}
		msgCtx := di.SqlDriverMessageMap
		whereValue, ok := vm.Eval(msgCtx, where)
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
		if ct, err := m.Delete(deleteKey); err != nil {
			u.Errorf("Could not delete key: %v", deleteKey)
		} else if ct != 1 {
			u.Errorf("delete should have removed 1 key %v", deleteKey)
		}
	}
	return len(deletedKeys), nil
}
