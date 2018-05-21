// Package sqlite implements a Qlbridge Datasource interface around sqlite
// that translates mysql syntax to sqlite.
package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/google/btree"
	"golang.org/x/net/context"
	// Import driver for sqlite
	_ "github.com/mattn/go-sqlite3"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure our conn implements connection features
	_ schema.ConnAll      = (*qryconn)(nil)
	_ schema.ConnMutation = (*qryconn)(nil)

	// SourcePlanner interface {
	// 	// given our request statement, turn that into a plan.Task.
	// 	WalkSourceSelect(pl Planner, s *Source) (Task, error)
	// }
	_ plan.SourcePlanner = (*qryconn)(nil)
)

type (
	// qryconn is a single-query connection in order to manage
	// stateful, non-multi-threaded access to sqlite rows object.
	qryconn struct {
		*exec.TaskBase
		stmt      rel.SqlStatement
		exit      <-chan bool
		source    *Source
		tbl       *schema.Table
		ps        *plan.Source
		indexCol  int
		rows      *sql.Rows
		ct        uint64
		cols      []string
		colidx    map[string]int
		err       error
		sqlInsert string
		sqlUpdate string
	}
)

func newQueryConn(tbl *schema.Table, source *Source) *qryconn {
	m := qryconn{
		tbl:    tbl,
		cols:   tbl.Columns(),
		source: source,
	}
	m.init()
	return &m
}

func (m *qryconn) init() {
	cols := make([]string, len(m.cols))
	vals := make([]string, len(m.cols))
	for i, col := range m.cols {
		cols[i] = expr.IdentityMaybeQuote('"', col)
		vals[i] = "?"
	}
	m.sqlInsert = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", m.tbl.Name, strings.Join(cols, ", "), strings.Join(vals, ", "))
}

// Close the qryconn.  Since sqlite is a NON-threadsafe db, this is very important
// as we actually hold a lock per-table during scans to prevent conflict.
func (m *qryconn) Close() error {
	defer m.source.mu.Unlock()
	delete(m.source.qryconns, m.tbl.Name)
	if m.rows != nil {
		if err := m.rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CreateIterator creates an interator to page through each row in this query resultset.
// This qryconn is wrapping a sql rows object, paging through until empty.
func (m *qryconn) CreateIterator() schema.Iterator { return m }

// Columns gets the columns used in this query.
func (m *qryconn) Columns() []string { return m.cols }

//func (m *qryconn) Length() int                     { return 0 }

//func (m *conn) SetColumns(cols []string)                  { m.tbl.SetColumns(cols) }

// CreateMutator part of Mutator interface to allow this connection to have access
// to the full plan context to take original sql statement and pass through to sqlite.
func (m *qryconn) CreateMutator(pc interface{}) (schema.ConnMutator, error) {
	if ctx, ok := pc.(*plan.Context); ok && ctx != nil {
		m.TaskBase = exec.NewTaskBase(ctx)
		m.stmt = ctx.Stmt
		return m, nil
	}
	return nil, fmt.Errorf("Expected *plan.Context but got %T", pc)
}

func (m *qryconn) Next() schema.Message {
	if m.rows == nil {
		m.err = fmt.Errorf("wtf missing rows")
		u.Errorf("could not find rows")
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
			//vals := make([]driver.Value, len(m.cols))
			//u.Infof("expecting %d cols", len(m.cols))
			readCols := make([]interface{}, len(m.cols))
			writeCols := make([]driver.Value, len(m.cols))
			for i := range writeCols {
				readCols[i] = &writeCols[i]
			}
			//cols, _ := m.rows.Columns()
			//u.Debugf("sqlite result cols provides %v but expecting %d", cols, len(m.cols))

			m.err = m.rows.Scan(readCols...)
			if m.err != nil {
				u.Warnf("err=%v", m.err)
				return nil
			}
			//u.Debugf("read vals: %#v", writeCols)

			// This seems pretty gross, isn't there a better way to do this?
			for i, col := range writeCols {
				//u.Debugf("%d %s  %T %v", i, m.cols[i], col, col)
				switch val := col.(type) {
				case []uint8:
					writeCols[i] = driver.Value(string(val))
				}
			}
			msg := datasource.NewSqlDriverMessageMap(m.ct, writeCols, m.colidx)

			m.ct++

			//u.Infof("return item btreeP:%p itemP:%p cursorP:%p  %v %v", m, item, m.cursor, msg.Id(), msg.Values())
			//u.Debugf("return? %T  %v", item, item.(*DriverItem).SqlDriverMessageMap)
			return msg
		}
	}
}

// Put interface for Upsert.Put() to do single row insert based on key.
func (m *qryconn) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {

	//u.Infof("%p Put(),  row:%#v", m, row)
	switch rowVals := row.(type) {
	case []driver.Value:
		if len(rowVals) != len(m.Columns()) {
			u.Warnf("wrong column ct")
			return nil, fmt.Errorf("Wrong number of columns, got %v expected %v", len(rowVals), len(m.Columns()))
		}

		id := MakeId(rowVals[m.indexCol])

		row := m.source.db.QueryRow(fmt.Sprintf("SELECT * FROM %v WHERE %s = $1", m.tbl.Name, m.cols[0]), rowVals[m.indexCol])
		vals := make([]driver.Value, len(m.cols))
		if err := row.Scan(&vals); err != nil && err != sql.ErrNoRows {
			u.Warnf("could not get current? %v", err)
			return nil, err
		} else if err == sql.ErrNoRows {
			//u.Debugf("empty, now do insert")
			//sdm := datasource.NewSqlDriverMessageMap(id, rowVals, m.tbl.FieldPositions)
			ivals := make([]interface{}, len(rowVals))
			for i, v := range rowVals {
				ivals[i] = v
			}
			_, err = m.source.db.Exec(m.sqlInsert, ivals...)
			if err != nil {
				u.Warnf("wtf %v", err)
			}
			//u.Debugf("%p  PUT: id:%v IdVal:%v  Id():%v vals:%#v", m, id, sdm.IdVal, sdm.Id(), rowVals)
		} else {
			u.Debugf("found current? %v", vals)
			sdm := datasource.NewSqlDriverMessageMap(id, rowVals, m.tbl.FieldPositions)
			_, err = m.source.db.Exec(m.stmt.String(), nil)
			if err != nil {
				u.Warnf("wtf %v", err)
			}
			u.Debugf("%p  PUT: id:%v IdVal:%v  Id():%v vals:%#v", m, id, sdm.IdVal, sdm.Id(), rowVals)
		}

		return NewKey(id), nil
	default:
		u.Warnf("not implemented %T", row)
		return nil, fmt.Errorf("Expected []driver.Value but got %T", row)
	}
}

func (m *qryconn) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

// Get a single row by key.
func (m *qryconn) Get(key driver.Value) (schema.Message, error) {

	row := m.source.db.QueryRow(fmt.Sprintf("SELECT * FROM %v WHERE %s = $1", m.tbl.Name, m.cols[0]), key)
	vals := make([]driver.Value, len(m.cols))
	if err := row.Scan(&vals); err != nil {
		return nil, err
	}
	return datasource.NewSqlDriverMessageMap(0, vals, m.colidx), nil
}

// Delete deletes a single row by key
func (m *qryconn) Delete(key driver.Value) (int, error) {
	return -1, schema.ErrNotImplemented
}

// WalkSourceSelect An interface implemented by this connection allowing the planner
// to push down as much sql logic down to sqlite.
func (m *qryconn) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {

	sqlSelect := p.Stmt.Source
	u.Infof("original %s", sqlSelect.String())
	//p.Stmt.Source = nil
	//p.Stmt.Rewrite(sqlSelect)
	//sqlSelect = p.Stmt.Source
	//u.Infof("original after From(source) rewrite %s", sqlSelect.String())
	//sqlSelect.RewriteAsRawSelect()

	m.cols = sqlSelect.Columns.UnAliasedFieldNames()
	m.colidx = sqlSelect.ColIndexes()
	sqlString, _ := newRewriter(sqlSelect).rewrite()

	u.Infof("after sqlite-rewrite %s", sqlSelect.String())
	u.Infof("pushdown sql: %s", sqlString)

	rows, err := m.source.db.Query(sqlString)
	if err != nil {
		u.Errorf("could not open master err=%v", err)
		return nil, err
	}
	m.rows = rows
	m.TaskBase = exec.NewTaskBase(p.Context())
	p.SourceExec = true
	m.ps = p
	//p.Complete = true
	return nil, nil
}

// DeleteExpression Delete using a Where Expression
func (m *qryconn) DeleteExpression(p interface{}, where expr.Node) (int, error) {

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

func MakeId(dv driver.Value) uint64 {
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
		return MakeId(vt.Val)
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
