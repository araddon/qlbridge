// sqlite implements a Qlbridge Datasource interface around sqlite.
package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/google/btree"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure our conn implements connection features
	_ schema.Conn         = (*conn)(nil)
	_ schema.ConnAll      = (*tblconn)(nil)
	_ schema.ConnMutation = (*tblconn)(nil)
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

// tblconn is a single-table connection in order to manage
// stateful, non-multi-threaded access to tables.
type tblconn struct {
	*exec.TaskBase
	stmt     rel.SqlStatement
	exit     <-chan bool
	conn     *conn
	tbl      *schema.Table
	indexCol int
	rows     *sql.Rows
	ct       uint64
	cols     []string
	colidx   map[string]int
	err      error
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

// CreateMutator part of Mutator interface to allow this connection to have access
// to the full plan context to take original sql statement and pass through to sqlite.
func (m *tblconn) CreateMutator(pc interface{}) (schema.ConnMutator, error) {
	if ctx, ok := pc.(*plan.Context); ok && ctx != nil {
		m.TaskBase = exec.NewTaskBase(ctx)
		m.stmt = ctx.Stmt
		return m, nil
	}
	return nil, fmt.Errorf("Expected *plan.Context but got %T", pc)
}

func (m *tblconn) Next() schema.Message {
	//u.Infof("Next()")
	if m.rows == nil {
		m.err = fmt.Errorf("wtf missing rows")
		return nil
	}
	select {
	case <-m.exit:
		return nil
	default:
		for {
			if !m.rows.Next() {
				return nil
			}
			vals := make([]driver.Value, len(m.cols))
			m.err = m.rows.Scan(&vals)
			if m.err != nil {
				return nil
			}

			msg := datasource.NewSqlDriverMessageMap(m.ct, vals, m.colidx)

			m.ct++

			//u.Infof("return item btreeP:%p itemP:%p cursorP:%p  %v %v", m, item, m.cursor, msg.Id(), msg.Values())
			//u.Debugf("return? %T  %v", item, item.(*DriverItem).SqlDriverMessageMap)
			return msg
		}
	}
}

// interface for Upsert.Put()
func (m *tblconn) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {

	u.Infof("%p Put(),  row:%#v", m, row)
	switch rowVals := row.(type) {
	case []driver.Value:
		if len(rowVals) != len(m.Columns()) {
			u.Warnf("wrong column ct")
			return nil, fmt.Errorf("Wrong number of columns, got %v expected %v", len(rowVals), len(m.Columns()))
		}
		id := makeId(rowVals[m.indexCol])
		//sdm := datasource.NewSqlDriverMessageMap(id, rowVals, m.tbl.FieldPositions)
		//m.conn.db.Exec(m.stmt.String(), nil)
		//u.Debugf("%p  PUT: id:%v IdVal:%v  Id():%v vals:%#v", m, id, sdm.IdVal, sdm.Id(), rowVals)
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

	row := m.conn.db.QueryRow(fmt.Sprintf("SELECT * FROM %v WHERE %s = $1", "hello", "damn"), key)
	vals := make([]driver.Value, len(m.cols))
	if err := row.Scan(&vals); err != nil {
		return nil, err
	}
	return datasource.NewSqlDriverMessageMap(0, vals, m.colidx), nil
}

func (m *tblconn) MultiGet(keys []driver.Value) ([]schema.Message, error) {
	return nil, schema.ErrNotImplemented
}

// Interface for Deletion
func (m *tblconn) Delete(key driver.Value) (int, error) {
	// item := m.bt.Delete(NewKey(makeId(key)))
	// if item == nil {
	// 	//u.Warnf("could not delete: %v", key)
	// 	return 0, schema.ErrNotFound
	// }
	// return 1, nil
	return -1, schema.ErrNotImplemented
}

// DeleteExpression Delete using a Where Expression
func (m *tblconn) DeleteExpression(p interface{}, where expr.Node) (int, error) {

	return -1, schema.ErrNotImplemented
	/*
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
	*/
}

func makeId(dv driver.Value) uint64 {
	switch vt := dv.(type) {
	case int:
		return uint64(vt)
	case int64:
		return uint64(vt)
	case []byte:
		return siphash.Hash(456729, 1111581582, vt)
	case string:
		return siphash.Hash(456729, 1111581582, []byte(vt))
		//by := append(make([]byte,0,8), byte(r), byte(r>>8), byte(r>>16), byte(r>>24), byte(r>>32), byte(r>>40), byte(r>>48), byte(r>>56))
	case datasource.KeyCol:
		return makeId(vt.Val)
	}
	return 0
}

// Key implements Key and Sort interfaces.
type Key struct {
	Id uint64
}

func NewKey(key uint64) *Key     { return &Key{key} }
func (m *Key) Key() driver.Value { return driver.Value(m.Id) }
func (m *Key) Less(than btree.Item) bool {
	switch it := than.(type) {
	case *Key:
		return m.Id < it.Id
	default:
		u.Warnf("what type? %T", than)
	}
	return false
}
