// sqlite implements a Qlbridge Datasource interface around sqlite.
package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	u "github.com/araddon/gou"
	"github.com/google/btree"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	// ensure our conn implements connection features
	_ schema.Conn    = (*conn)(nil)
	_ schema.ConnAll = (*tblconn)(nil)
)

// conn implements qlbridge schema.Conn to a sqlite file based source.
type conn struct {
	exit     <-chan bool
	file     string // Local file path to sqlite db
	db       *sql.DB
	mu       sync.Mutex
	s        *schema.Schema
	tblconns map[string]*tblconn
}

// tblconn
type tblconn struct {
	exit <-chan bool
	conn *conn
	tbl  *schema.Table
	rows *sql.Rows
}

func newConn(s *schema.Schema) *conn {
	m := conn{s: s}
	return &m
}
func (m *conn) setup() error {

	if m.db != nil {
		return nil
	}
	if m.s == nil {
		return fmt.Errorf("must have schema")
	}
	file := m.s.Conf.Settings.String("file")
	if file == "" {
		file = fmt.Sprintf("/tmp/%s.sql.db", m.s.Name)
	}
	m.file = file

	// It will be created if it doesn't exist.
	//   "./source.enriched.db"
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	m.db = db

	// if err := datasource.IntrospectTable(m.tbl, m.CreateIterator()); err != nil {
	// 	u.Errorf("Could not introspect schema %v", err)
	// }

	return nil
}

func (m *conn) Table(table string) (*schema.Table, error) { return m.s.Table(table) }
func (m *conn) Tables() []string                          { return m.s.Tables() }
func (m *conn) Close() error {
	if m.db != nil {
		err := m.db.Close()
		if err != nil {
			return err
		}
		m.db = nil
	}
	return nil
}

func newTableConn(conn *conn) *tblconn {
	m := tblconn{conn: conn}
	return &m
}

func (m *tblconn) Close() error {
	if m.rows != nil {
		if err := m.rows.Close(); err != nil {
			return err
		}
	}
	m.conn.mu.Lock()
	defer m.conn.mu.Unlock()
	delete(m.conn.tblconns, m.tbl.Name)
	return nil
}
func (m *tblconn) CreateIterator() schema.Iterator { return m }
func (m *tblconn) Columns() []string               { return m.tbl.Columns() }
func (m *tblconn) Length() int                     { return 0 }

//func (m *conn) SetColumns(cols []string)                  { m.tbl.SetColumns(cols) }

func (m *tblconn) Next() schema.Message {
	//u.Infof("Next()")
	select {
	case <-m.exit:
		return nil
	default:
		for {

			if item == nil {
				//u.Debugf("reset cursor to nil  %#v", item)
				return nil
			}

			//u.Infof("return item btreeP:%p itemP:%p cursorP:%p  %v %v", m, item, m.cursor, msg.Id(), msg.Values())
			//u.Debugf("return? %T  %v", item, item.(*DriverItem).SqlDriverMessageMap)
			return msg.SqlDriverMessageMap.Copy()
		}
	}
}

// interface for Upsert.Put()
func (m *tblconn) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {

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

func (m *tblconn) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

// interface for Seeker
func (m *tblconn) CanSeek(sql *rel.SqlSelect) bool {
	return true
}

func (m *tblconn) Get(key driver.Value) (schema.Message, error) {
	item := m.bt.Get(NewKey(makeId(key)))
	if item != nil {
		return item.(*DriverItem).SqlDriverMessageMap, nil
	}
	return nil, schema.ErrNotFound // Should not found be an error?
}

func (m *tblconn) MultiGet(keys []driver.Value) ([]schema.Message, error) {
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
func (m *tblconn) Delete(key driver.Value) (int, error) {
	item := m.bt.Delete(NewKey(makeId(key)))
	if item == nil {
		//u.Warnf("could not delete: %v", key)
		return 0, schema.ErrNotFound
	}
	return 1, nil
}

// DeleteExpression Delete using a Where Expression
func (m *tblconn) DeleteExpression(p interface{}, where expr.Node) (int, error) {

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
