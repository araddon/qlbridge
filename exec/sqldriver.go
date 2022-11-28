package exec

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure our driver implements appropriate database/sql interfaces
	_ driver.Conn	= (*qlbConn)(nil)
	_ driver.Driver  = (*qlbdriver)(nil)
	_ driver.Execer  = (*qlbConn)(nil)
	_ driver.Queryer = (*qlbConn)(nil)
	_ driver.Result  = (*qlbResult)(nil)
	_ driver.Rows	= (*qlbRows)(nil)
	_ driver.Stmt	= (*qlbStmt)(nil)
	//_ driver.Tx	  = (*driverConn)(nil)

	// Create an instance of our driver
	qlbd		  = &qlbdriver{}
	qlbDriverOnce sync.Once

	// Runtime Schema Config as in in-mem data structure of the
	//  datasources, tables, etc.   Sources must be registered
	//  as this is not persistent
	registry = schema.DefaultRegistry()

	_ = u.EMPTY
)

const (
	MysqlTimeFormat = "2006-01-02 15:04:05.000000000"
)

func RegisterSqlDriver() {
	qlbDriverOnce.Do(func() {
		sql.Register("qlbridge", qlbd)
	})
}

func DisableRecover() {
	schema.DisableRecover = true
}

// sql.Driver Interface implementation.
//
// Notes about Value return types:
//	 Value is a value that drivers must be able to handle.
//	 It is either nil or an instance of one of these types:
//
//	   int64
//	   float64
//	   bool
//	   []byte
//	   string   [*] everywhere except from Rows.Next.
//	   time.Time
type qlbdriver struct{}

// Open returns a new connection to the database.
//
// Open may return a cached connection (one previously closed), but doing so
// is unnecessary; the sql package maintains a pool of idle connections for
// efficient re-use.
//
// The returned connection is only used by one goroutine at a time.
//
// @connInfo = database/Schema name
// @connInfo = driver-connection-info
// @connInfo = sourceType://source
func (m *qlbdriver) Open(connInfo string) (driver.Conn, error) {
	s, ok := registry.Schema(connInfo)
	if !ok || s == nil {
		return nil, fmt.Errorf("No schema was found for %q", connInfo)
	}
	return &qlbConn{schema: s, session: datasource.NewMySqlSessionVars(), stmts: make(map[*qlbStmt]struct{})}, nil
}

// A stateful connection to database/source
//
//
// Execer is an optional interface that may be implemented by a Conn.
//		If a Conn does not implement Execer, the sql package's DB.Exec will
//		first prepare a query, execute the statement, and then close the
//		statement.
//
// Queryer is an optional interface that may be implemented by a Conn.
//		If a Conn does not implement Queryer, the sql package's DB.Query will
//		first prepare a query, execute the statement, and then close the
//		statement.
type qlbConn struct {
	parallel bool   // Do we Run In Background Mode?  Default = true
	connInfo string //
	schema   *schema.Schema
	session  expr.ContextReadWriter
	stmts	map[*qlbStmt]struct{}
}

// Exec may return ErrSkip.
//
// Execer implementation. To be used for queries that do not return any rows
// such as Create Index, Insert, Upset, Delete etc
func (m *qlbConn) Exec(query string, args []driver.Value) (driver.Result, error) {

	stmt := &qlbStmt{conn: m, query: query}
	defer stmt.Close()
	stmt.numInput = strings.Count(query, "?")
	return stmt.Exec(args)
}

// Queryer implementation
// Query may return ErrSkip
//
func (m *qlbConn) Query(query string, args []driver.Value) (driver.Rows, error) {

	stmt := &qlbStmt{conn: m, query: query}
	stmt.numInput = strings.Count(query, "?")
	return stmt.Query(args)
}

// Prepare returns a prepared statement, bound to this connection.
func (m *qlbConn) Prepare(query string) (driver.Stmt, error) {

	query = strings.TrimSpace(query)
	s := strings.Split(strings.ToLower(query), " ")
	stmt := &qlbStmt{conn: m, query: query}
	stmt.numInput = strings.Count(query, "?")
	var err error
	if s[0] == "insert" {
		stmt.job, err = createExecJob(strings.ReplaceAll(query, "?", "0"), m, nil, nil)
		if err != nil {
			return nil, err
		}
		stmt.sqlStmt = stmt.job.Ctx.Stmt
	}
	m.stmts[stmt] = struct{}{}
	return stmt, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (m *qlbConn) Close() error {

	if m.stmts != nil {
		for k, _ := range m.stmts {
			k.Close()
			delete(m.stmts, k)
		}
		m.stmts = nil
	}
	return nil
}

// Begin starts and returns a new transaction.
func (m *qlbConn) Begin() (driver.Tx, error) {
	return nil, expr.ErrNotImplemented
}

// sql.Tx Transaction Interface implementation.
type qlbTx struct{}

func (conn *qlbTx) Commit() error {
	return expr.ErrNotImplemented
}
func (conn *qlbTx) Rollback() error { return expr.ErrNotImplemented }

// driver.Stmt Interface implementation.
//
// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
//
type qlbStmt struct {
	job			  *JobExecutor
	query			string
	numInput		 int
	conn			 *qlbConn
	sqlStmt		  rel.SqlStatement
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (m *qlbStmt) Close() error {

	if m.job != nil {
		m.job.Close()
	}
	if m.conn.stmts != nil {
		delete (m.conn.stmts, m)
	}
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (m *qlbStmt) NumInput() int { 
	return m.numInput
}

// Exec executes a query that doesn't return rows, such
// as an INSERT, UPDATE, DELETE
func (m *qlbStmt) Exec(args []driver.Value) (driver.Result, error) {

	if m.query == "" {
		return nil, fmt.Errorf("No query in stmt.Exec() %#p", m)
	}
	var err error
	prepared := false
	if m.conn.stmts != nil {
		 _, prepared = m.conn.stmts[m]  // in list of prepared
		if prepared {
			prepared = m.sqlStmt != nil // has parsed sql
		}
	}
	if !prepared {
		m.job, err = createExecJob(m.query, m.conn, args, nil)
		if err != nil {
			return nil, err
		}
	} else {  // Previously prepared
		m.job, err = createExecJob(m.query, m.conn, args, m.sqlStmt)
		if err != nil {
			return nil, err
		}
		rows := make([][]*rel.ValueColumn, 0)
		rows = append(rows, argsToValueColumns(args))
		switch p := m.job.Ctx.Stmt.(type) {
		case *rel.SqlInsert:
			p.Rows = rows
		default:
			return nil, fmt.Errorf("sqldriver Exec prepared stmt type %T not implemented.", p)
		}
	}

	resultWriter := NewResultExecWriter(m.job.Ctx)
	m.job.RootTask.Add(resultWriter)
	m.job.Setup()

	//u.Infof("in qlbdriver.Exec about to run")
	err = m.job.Run()
	//u.Debugf("After qlb driver.Run() in Exec()")
	if err != nil {
		u.Errorf("error on Query.Run(): %v", err)
		//resultWriter.ErrChan() <- err
		//job.Close()
	}
	return resultWriter.Result(), err
}

// Query executes a query that may return rows, such as a SELECT
func (m *qlbStmt) Query(args []driver.Value) (driver.Rows, error) {

	var err error
	qry := m.query
	if len(args) > 0 {
		qry, err = queryArgsConvert(qry, args)
		if err != nil {
			return nil, err
		}
	}
	u.Debugf("stmt.query: %v", qry)

	// Create a Job, which is Dag of Tasks that Run()
	ctx := plan.NewContext(qry)
	ctx.Schema = m.conn.schema
	ctx.Session = m.conn.session
	job, err := BuildSqlJob(ctx)
	if err != nil {
		u.Errorf("return error? %v", err)
		return nil, err
	}
	m.job = job

	// The only type of stmt that makes sense for Query is SELECT
	//  and we need list of columns that requires casing
	//sqlSelect, ok := job.Ctx.Stmt.(*rel.SqlSelect)
	_, ok := job.Ctx.Stmt.(*rel.SqlSelect)
	if !ok {
		u.Warnf("ctx? %v", job.Ctx)
		return nil, fmt.Errorf("We could not recognize that as a select query: %T", job.Ctx.Stmt)
	}

	// Prepare a result writer, we manually append this task to end
	// of job?
	//resultWriter := NewResultRows(ctx, sqlSelect.Columns.AliasedFieldNames())

	projCols := job.Ctx.Projection.Proj.Columns
	cols := make([]string, len(projCols))
	for i, col := range projCols {
		cols[i] = col.As
	}
	resultWriter := NewResultRows(ctx, cols)
	job.RootTask.Add(resultWriter)
	job.Setup()

	// TODO:   this can't run in parallel-buffered mode?
	// how to open in go-routine and still be able to send error to rows?
	go func() {
		//u.Debugf("Start Job.Run")
		err = job.Run()
		//u.Debugf("After job.Run()")
		if err != nil {
			u.Errorf("error on Query.Run(): %v", err)
			//resultWriter.ErrChan() <- err
			//job.Close()
		}
		job.Close()
		//u.Debugf("exiting Background Query")
	}()

	return resultWriter, nil
}

// driver.ColumnConverter Interface implementation.
//
// ColumnConverter may be optionally implemented by driver.Stmt if the
// statement is aware of its own columns' types and can convert from
// any type to a driver Value.
//
// ColumnConverter returns a ValueConverter for the provided
// column index.  If the type of a specific column isn't known
// or shouldn't be handled specially, DefaultValueConverter
// can be returned.
func (conn *qlbStmt) ColumnConverter(idx int) driver.ValueConverter { 
	return driver.DefaultParameterConverter
}

// driver.Rows Interface implementation.
//
// Rows is an iterator over an executed query's results.
//
type qlbRows struct{}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice.  If a particular column name isn't known, an empty
// string should be returned for that entry.
func (conn *qlbRows) Columns() []string { return nil }

// Close closes the rows iterator.
func (conn *qlbRows) Close() error { return expr.ErrNotImplemented }

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// The dest slice may be populated only with
// a driver Value type, but excluding string.
// All string values must be converted to []byte.
//
// Next should return io.EOF when there are no more rows.
func (conn *qlbRows) Next(dest []driver.Value) error { return expr.ErrNotImplemented }

// driver.Result Interface implementation.
//
// Result is the result of a query execution that doesn't return rows
//
type qlbResult struct {
	lastId   int64
	affected int64
	err	  error
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *qlbResult) LastInsertId() (int64, error) { return r.lastId, r.err }

// RowsAffected returns the number of rows affected by the
// query.
func (r *qlbResult) RowsAffected() (int64, error) { return r.affected, r.err }

func join(a []string) string {
	n := 0
	for _, s := range a {
		n += len(s)
	}
	b := make([]byte, n)
	n = 0
	for _, s := range a {
		n += copy(b[n:], s)
	}
	return string(b)
}

func queryArgsConvert(query string, args []driver.Value) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	// a tiny, tiny, tiny bit of string sanitization
/*
	if strings.ContainsAny(query, `'"`) {
		return "", nil
	}
*/
	q := make([]string, 2*len(args)+1)
	n := 0
	for _, a := range args {
		i := strings.IndexRune(query, '?')
		if i == -1 {
			return "", fmt.Errorf("number of parameters doesn't match number of placeholders for query %s", query)
		}
		var s string
		switch v := a.(type) {
		case nil:
			s = "NULL"
		case string:
			s = "'" + escapeString(v) + "'"
		case []byte:
			s = "'" + escapeString(string(v)) + "'"
		case int64:
			s = strconv.FormatInt(v, 10)
		case time.Time:
			s = "'" + v.Format(MysqlTimeFormat) + "'"
		case bool:
			if v {
				s = "1"
			} else {
				s = "0"
			}
		case float64:
			s = strconv.FormatFloat(v, 'e', 12, 64)
		default:
			panic(fmt.Sprintf("%v (%T) can't be handled by godrv", v, v))
		}
		q[n] = query[:i]
		q[n+1] = s
		query = query[i+1:]
		n += 2
	}
	q[n] = query
	return join(q), nil
}

func escapeString(txt string) string {
	var (
		esc string
		buf bytes.Buffer
	)
	last := 0
	for ii, bb := range txt {
		switch bb {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		io.WriteString(&buf, txt[last:ii])
		io.WriteString(&buf, esc)
		last = ii + 1
	}
	io.WriteString(&buf, txt[last:])
	return buf.String()
}

func escapeQuotes(txt string) string {
	var buf bytes.Buffer
	last := 0
	for ii, bb := range txt {
		if bb == '\'' {
			io.WriteString(&buf, txt[last:ii])
			io.WriteString(&buf, `''`)
			last = ii + 1
		}
	}
	io.WriteString(&buf, txt[last:])
	return buf.String()
}

func createExecJob(query string, conn *qlbConn, args []driver.Value, 
		stmt rel.SqlStatement) (*JobExecutor, error) {

	if query == "" {
		return nil, fmt.Errorf("createExecJob no sql provided")
	}
	var err error
	if args != nil && len(args) > 0 {
		query, err = queryArgsConvert(query, args)
		if err != nil {
			return nil, err
		}
	}

	// Create a Job, which is Dag of Tasks that Run()
	ctx := plan.NewContext(query)
	ctx.Schema = conn.schema
	ctx.Session = conn.session
	ctx.Stmt = stmt
	job, err := BuildSqlJob(ctx)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func argsToValueColumns(vals []driver.Value) []*rel.ValueColumn {

	row := make([]*rel.ValueColumn, len(vals))
	for i, x := range vals {
		switch v := x.(type) {
			case nil:
				row[i] = &rel.ValueColumn{Value: value.NewNilValue()}
			case float64:
				row[i] = &rel.ValueColumn{Value: value.NewNumberValue(x.(float64))}
			case string:
				row[i] = &rel.ValueColumn{Value: value.NewStringValue(x.(string))}
			case []byte:
				row[i] = &rel.ValueColumn{Value: value.NewStringValue(string(x.([]byte)))}
			case int64:
				row[i] = &rel.ValueColumn{Value: value.NewIntValue(x.(int64))}
			case time.Time:
				row[i] = &rel.ValueColumn{Value: value.NewStringValue(x.(time.Time).String())}
			case bool:
				row[i] = &rel.ValueColumn{Value: value.NewBoolValue(x.(bool))}
			default:
				panic(fmt.Sprintf("%v (%T) argument can't be handled by prepared insert", v, v))
		}
	}
	return row
}
