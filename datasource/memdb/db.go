package memdb

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/hashicorp/go-memdb"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

const (
	sourceType = "memdb"
)

var (
	_ = u.EMPTY

	// Different Features of this Static Data Source
	_ schema.DataSource    = (*MemDb)(nil)
	_ schema.SourceConn    = (*MemDb)(nil)
	_ schema.SchemaColumns = (*MemDb)(nil)
	_ schema.Scanner       = (*MemDb)(nil)
	_ schema.Seeker        = (*MemDb)(nil)
	_ schema.Upsert        = (*MemDb)(nil)
	_ schema.Deletion      = (*MemDb)(nil)
)

func makeId(dv driver.Value) uint64 {
	switch vt := dv.(type) {
	case int:
		return uint64(vt)
	case int64:
		return uint64(vt)
	case []byte:
		return siphash.Hash(0, 1, vt)
	case string:
		return siphash.Hash(0, 1, []byte(vt))
	case datasource.KeyCol:
		return makeId(vt.Val)
	case nil:
		return 0
	default:
		u.LogTracef(u.WARN, "no id conversion for type")
		u.Warnf("not implemented conversion: %T", dv)
	}
	return 0
}

func makeMemDbSchema() *memdb.DBSchema {
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"main": &memdb.TableSchema{
				Name: "main",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "ID"},
					},
					"foo": &memdb.IndexSchema{
						Name:    "foo",
						Indexer: &memdb.StringFieldIndex{Field: "Foo"},
					},
				},
			},
		},
	}
}

// MemDB DataSource, implements qlbridge DataSource to allow in memory native go data
//   to have a Schema and implement and be operated on by Sql Operations
//
// Features
// - ues immuteable radix-tree/db mvcc under the nood
//
type MemDb struct {
	exit           <-chan bool
	*schema.Schema               // schema
	tbl            *schema.Table // schema table
	indexedColumns []string      // Which column position is indexed?  ie primary key
	db             *memdb.MemDB
	max            int
}
type dbReader struct {
	// Read only
	db     *MemDb
	txn    *memdb.Txn
	result memdb.ResultIterator
}

func NewMemDb(name string, indexedCols []string, data [][]driver.Value, cols []string) (*MemDb, error) {

	sourceSchema := schema.NewSourceSchema(name, sourceType)
	tbl := schema.NewTable(name, sourceSchema)
	sourceSchema.AddTable(tbl)
	schema := schema.NewSchema(name)
	schema.AddSourceSchema(sourceSchema)

	m := MemDb{indexedColumns: indexedCols}
	m.tbl = tbl
	db, err := memdb.NewMemDB(makeMemDbSchema())
	if err != nil {
		u.Warnf("culd not create db %v", err)
		return nil, err
	}
	m.db = db
	m.Schema = schema
	m.tbl.SetColumns(cols)
	for _, row := range data {
		m.Put(nil, nil, row)
	}
	return &m, nil
}

//func (m *MemDb) Length() int                                     { return m.db.Len() }

func (m *MemDb) Open(connInfo string) (schema.SourceConn, error) { return m, nil }
func (m *MemDb) Table(table string) (*schema.Table, error)       { return m.tbl, nil }
func (m *MemDb) Close() error                                    { return nil }
func (m *MemDb) Tables() []string                                { return []string{m.Schema.Name} }
func (m *MemDb) Columns() []string                               { return m.tbl.Columns() }
func (m *MemDb) SetColumns(cols []string)                        { m.tbl.SetColumns(cols) }

func (m *MemDb) CreateIterator(filter expr.Node) schema.Iterator {
	txn := m.db.Txn(false)
	// Attempt a row scan on the ID
	result, err := txn.Get("main", "id")
	if err != nil {
		u.Errorf("error %v", err)
	}
	return &dbReader{m, txn, result}
}
func (m *MemDb) MesgChan(filter expr.Node) <-chan schema.Message {
	iter := m.CreateIterator(filter)
	return datasource.SourceIterChannel(iter, filter, m.exit)
}

func (m *dbReader) Next() schema.Message {
	//u.Infof("Next()")
	select {
	case <-m.db.exit:
		return nil
	default:
		for {
			if m.result == nil {
				return nil
			}
			raw := m.result.Next()
			if msg, ok := raw.(schema.Message); ok {
				return msg //.SqlDriverMessageMap.Copy()
			}
			u.Warnf("error, not correct type: %#v", raw)
			return nil
		}
	}
}

// interface for Upsert.Put()
func (m *MemDb) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {

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
	return nil, nil
}

func (m *MemDb) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

// interface for Seeker
func (m *MemDb) CanSeek(sql *rel.SqlSelect) bool {
	return true
}

func (m *MemDb) Get(key driver.Value) (schema.Message, error) {
	item := m.bt.Get(NewKey(makeId(key)))
	if item != nil {
		return item.(*DriverItem).SqlDriverMessageMap, nil
	}
	return nil, schema.ErrNotFound // Should not found be an error?
}

func (m *MemDb) MultiGet(keys []driver.Value) ([]schema.Message, error) {
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
func (m *MemDb) Delete(key driver.Value) (int, error) {
	item := m.bt.Delete(NewKey(makeId(key)))
	if item == nil {
		//u.Warnf("could not delete: %v", key)
		return 0, schema.ErrNotFound
	}
	return 1, nil
}

// Delete using a Where Expression
func (m *MemDb) DeleteExpression(where expr.Node) (int, error) {
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
