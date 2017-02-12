// Memdb package implements a Qlbridge Datasource in-memory implemenation
// using the hashicorp go-memdb (immuteable radix tree's).
// Qlbridge Exec allows key-value datasources to have full SQL functionality.
package memdb

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
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
	_ schema.Source            = (*MemDb)(nil)
	_ schema.SourceTableSchema = (*MemDb)(nil)

	// Connection
	_ schema.Conn         = (*dbConn)(nil)
	_ schema.ConnColumns  = (*dbConn)(nil)
	_ schema.ConnScanner  = (*dbConn)(nil)
	_ schema.ConnUpsert   = (*dbConn)(nil)
	_ schema.ConnDeletion = (*dbConn)(nil)
	_ schema.ConnSeeker   = (*dbConn)(nil)
)

// MemDb implements qlbridge `Source` to allow in-memory native go data
// to have a Schema and implement and be operated on by Sql Statements.
type MemDb struct {
	exit           <-chan bool
	*schema.Schema                 // schema
	tbl            *schema.Table   // schema table
	indexes        []*schema.Index // index descriptions
	primaryIndex   string
	db             *memdb.MemDB
	max            int
}
type dbConn struct {
	md     *MemDb
	db     *memdb.MemDB
	txn    *memdb.Txn
	result memdb.ResultIterator
}

// NewMemDbData creates a MemDb with given indexes, columns, and values
func NewMemDbData(name string, data [][]driver.Value, cols []string) (*MemDb, error) {

	m, err := NewMemDb(name, cols)
	if err != nil {
		return nil, err
	}
	// Insert initial values
	conn := newDbConn(m)
	for _, row := range data {
		conn.Put(nil, nil, row)
	}
	conn.Close()
	return m, nil
}

// NewMemDb creates a MemDb with given indexes, columns
func NewMemDb(name string, cols []string) (*MemDb, error) {

	return NewMemDbForSchema(name, cols)
}

// NewMemDbForSchema creates a MemDb with given indexes, columns
func NewMemDbForSchema(name string, cols []string) (*MemDb, error) {

	if len(cols) < 1 {
		return nil, fmt.Errorf("must have columns provided")
	}

	m := &MemDb{}

	m.tbl = schema.NewTable(name)
	m.tbl.SetColumns(cols)

	err := m.buildDefaultIndexes()
	if err != nil {
		u.Errorf("Default indexes could not be built %v", err)
		return nil, err
	}

	mdbSchema, err := makeMemDbSchema(m)
	if err != nil {
		u.Errorf("Must have valid schema %v", err)
		return nil, err
	}
	db, err := memdb.NewMemDB(mdbSchema)
	if err != nil {
		u.Warnf("could not create db %v", err)
		return nil, err
	}
	m.db = db
	return m, nil
}

func (m *MemDb) Init() {}

// Open a Conn for this source @table name
func (m *MemDb) Open(table string) (schema.Conn, error) { return newDbConn(m), nil }

// Table by name
func (m *MemDb) Table(table string) (*schema.Table, error) { return m.tbl, nil }

// Close this source
func (m *MemDb) Close() error { return nil }

// Tables list, should be single table
func (m *MemDb) Tables() []string { return []string{m.tbl.Name} }

func (m *MemDb) buildDefaultIndexes() error {
	if len(m.indexes) == 0 {
		if len(m.tbl.Columns()) < 1 {
			return fmt.Errorf("must have columns if no index provided")
		}
		//u.Debugf("no index provided creating on %q", m.tbl.Columns()[0])
		m.indexes = []*schema.Index{
			{Name: "id", Fields: []string{m.tbl.Columns()[0]}},
		}
	}
	// First ensure we have one primary index
	hasPrimary := false
	for _, idx := range m.indexes {
		if idx.PrimaryKey {
			if idx.Name == "" {
				idx.Name = "id"
			}
			m.primaryIndex = idx.Name
			hasPrimary = true
		}
	}
	if !hasPrimary {
		m.indexes[0].PrimaryKey = true
		m.primaryIndex = m.indexes[0].Name
	}
	return nil
}

//func (m *MemDb) SetColumns(cols []string)                  { m.tbl.SetColumns(cols) }

func newDbConn(mdb *MemDb) *dbConn {
	c := &dbConn{md: mdb, db: mdb.db}
	return c
}
func (m *dbConn) Columns() []string { return m.md.tbl.Columns() }
func (m *dbConn) Close() error      { return nil }
func (m *dbConn) CreateIterator() schema.Iterator {
	m.txn = m.db.Txn(false)
	// Attempt a row scan on the primary index
	result, err := m.txn.Get(m.md.tbl.Name, m.md.primaryIndex)
	if err != nil {
		u.Errorf("error %v", err)
	}
	m.result = result
	return m
}
func (m *dbConn) MesgChan() <-chan schema.Message {
	return datasource.SourceIterChannel(m.CreateIterator(), m.md.exit)
}

func (m *dbConn) Next() schema.Message {

	if m.txn == nil {
		m.txn = m.db.Txn(false)
	}
	select {
	case <-m.md.exit:
		return nil
	default:
		for {
			if m.result == nil {
				result, err := m.txn.Get(m.md.tbl.Name, m.md.primaryIndex)
				if err != nil {
					u.Errorf("error %v", err)
					return nil
				}
				m.result = result
			}
			raw := m.result.Next()
			if raw == nil {
				return nil
			}
			if msg, ok := raw.(*datasource.SqlDriverMessage); ok {
				return msg.ToMsgMap(m.md.tbl.FieldPositions)
			}
			u.Warnf("error, not correct type: %#v", raw)
			return nil
		}
	}
}

// interface for Upsert.Put()
func (m *dbConn) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {

	//u.Infof("%p Put(),  row:%#v", m, row)
	switch rowVals := row.(type) {
	case []driver.Value:
		txn := m.db.Txn(true)
		key, err := m.putValues(txn, rowVals)
		if err != nil {
			txn.Abort()
			return nil, err
		}
		txn.Commit()
		return key, nil
		/*
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
		*/
	default:
		u.Warnf("not implemented %T", row)
		return nil, fmt.Errorf("Expected []driver.Value but got %T", row)
	}
}

func (m *dbConn) putValues(txn *memdb.Txn, row []driver.Value) (schema.Key, error) {
	if len(row) != len(m.Columns()) {
		u.Warnf("wrong column ct expected %d got %d for %v", len(m.Columns()), len(row), row)
		return nil, fmt.Errorf("Wrong number of columns, expected %v got %v", len(m.Columns()), len(row))
	}
	id := makeId(row[0])
	msg := &datasource.SqlDriverMessage{Vals: row, IdVal: id}
	txn.Insert(m.md.tbl.Name, msg)
	//u.Debugf("%p  PUT: id:%v IdVal:%v  Id():%v vals:%#v", m, id, sdm.IdVal, sdm.Id(), rowVals)
	return schema.NewKeyUint(id), nil
}

func (m *dbConn) PutMulti(ctx context.Context, keys []schema.Key, objs interface{}) ([]schema.Key, error) {
	txn := m.db.Txn(true)

	switch rows := objs.(type) {
	case [][]driver.Value:
		keys := make([]schema.Key, 0, len(rows))
		for _, row := range rows {
			key, err := m.putValues(txn, row)
			if err != nil {
				txn.Abort()
				return nil, err
			}
			keys = append(keys, key)
		}
		txn.Commit()
		return keys, nil
	}
	return nil, fmt.Errorf("unrecognized put object type: %T", objs)
}

// CanSeek is interface for Seeker, validate if we can perform this query
func (m *dbConn) CanSeek(sql *rel.SqlSelect) bool {
	return true
}

func (m *dbConn) Get(key driver.Value) (schema.Message, error) {
	txn := m.db.Txn(false)
	iter, err := txn.Get(m.md.tbl.Name, m.md.primaryIndex, fmt.Sprintf("%v", key))
	if err != nil {
		txn.Abort()
		u.Errorf("error reading %v because %v", key, err)
		return nil, err
	}
	txn.Commit() // noop

	if item := iter.Next(); item != nil {
		if msg, ok := item.(schema.Message); ok {
			return msg, nil
		}
		u.Warnf("unexpected type %T", item)
	}
	return nil, schema.ErrNotFound // Should not found be an error?
}

// MultiGet to get multiple items by keys
func (m *dbConn) MultiGet(keys []driver.Value) ([]schema.Message, error) {
	return nil, schema.ErrNotImplemented
}

// Interface for Deletion
func (m *dbConn) Delete(key driver.Value) (int, error) {
	txn := m.db.Txn(true)
	err := txn.Delete(m.md.tbl.Name, key)
	if err != nil {
		txn.Abort()
		u.Warnf("could not delete: %v  err=%v", key, err)
		return 0, err
	}
	txn.Commit()
	return 1, nil
}

// Delete using a Where Expression
func (m *dbConn) DeleteExpression(p interface{}, where expr.Node) (int, error) {

	// pd, ok := p.(*plan.Delete)
	// if !ok {
	// 	return nil, fmt.Errorf("Expected *plan.Delete but got %T", p)
	// }

	var deletedKeys []schema.Key
	txn := m.db.Txn(true)
	iter, err := txn.Get(m.md.tbl.Name, m.md.primaryIndex)
	if err != nil {
		txn.Abort()
		u.Errorf("could not get values %v", err)
		return 0, err
	}
deleteLoop:
	for {
		item := iter.Next()
		if item == nil {
			break
		}

		msg, ok := item.(datasource.SqlDriverMessage)
		if !ok {
			u.Warnf("wat?  %T   %#v", item, item)
			err = fmt.Errorf("unexpected message type %T", item)
			break
		}
		whereValue, ok := vm.Eval(msg.ToMsgMap(m.md.tbl.FieldPositions), where)
		if !ok {
			u.Debugf("could not evaluate where: %v", msg)
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				//this means do NOT delete
			} else {
				// Delete!
				if err = txn.Delete(m.md.tbl.Name, msg); err != nil {
					u.Errorf("could not delete %v", err)
					break deleteLoop
				}
				indexVal := msg.Vals[0]
				deletedKeys = append(deletedKeys, schema.NewKeyUint(makeId(indexVal)))
			}
		case nil:
			// ??
			u.Warnf("this should be fine, couldn't evaluate so don't delete %v", msg)
		default:
			if whereVal.Nil() {
				// Doesn't match, so don't delete
			} else {
				u.Warnf("unknown where eval result? %T", whereVal)
			}
		}
	}
	if err != nil {
		txn.Abort()
		return 0, err
	}
	txn.Commit()
	return len(deletedKeys), nil
}
