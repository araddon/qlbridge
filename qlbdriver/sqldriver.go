package qlbdriver

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("qlbridgedrv", &qlbdriver{})
}

//////////////////////////////////////////////////////////////////////////
// sql.Driver Interface implementation.
//
//
// Notes about Value return types:
//     Value is a value that drivers must be able to handle.
//     It is either nil or an instance of one of these types:
//
//       int64
//       float64
//       bool
//       []byte
//       string   [*] everywhere except from Rows.Next.
//       time.Time
//////////////////////////////////////////////////////////////////////////
type qlbdriver struct{}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (*qlbdriver) Open(name string) (sql.Conn, error) {
	return nil, ErrNotImplemented
}

//////////////////////////////////////////////////////////////////////////
// sql.Conn Interface implementation.
//
//
// plus:
// Execer is an optional interface that may be implemented by a Conn.
//        If a Conn does not implement Execer, the sql package's DB.Exec will
//        first prepare a query, execute the statement, and then close the
//        statement.
//
// Queryer is an optional interface that may be implemented by a Conn.
//        If a Conn does not implement Queryer, the sql package's DB.Query will
//        first prepare a query, execute the statement, and then close the
//        statement.
//////////////////////////////////////////////////////////////////////////
type qlbConn struct{}

// Exec may return ErrSkip.
//
// Execer implementation. To be used for queries that do not return any rows
// such as Create Index, Insert, Upset, Delete etc
func (conn *qlbConn) Exec(query string, args []driver.Value) (driver.Result, error) {

}

// Queryer implementation
// Query may return ErrSkip.
func (conn *qlbConn) Query(query string, args []driver.Value) (driver.Rows, error) {

}

// Prepare returns a prepared statement, bound to this connection.
func (conn *qlbConn) Prepare(query string) (driver.Stmt, error) {

}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (conn *qlbConn) Close() error {

}

// Begin starts and returns a new transaction.
func (conn *qlbConn) Begin() (driver.Tx, error) {

}

//////////////////////////////////////////////////////////////////////////
// sql.Tx Interface implementation.
//
// Tx is a transaction.
//////////////////////////////////////////////////////////////////////////
type qlbTx struct{}

func (conn *qlbTx) Commit() error   {}
func (conn *qlbTx) Rollback() error {}

//////////////////////////////////////////////////////////////////////////
// driver.Stmt Interface implementation.
//
// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
//
//////////////////////////////////////////////////////////////////////////

type qlbStmt struct{}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (conn *qlbStmt) Close() error {}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (conn *qlbStmt) NumInput() int {}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (conn *qlbStmt) Exec(args []driver.Value) (driver.Result, error) {}

// Query executes a query that may return rows, such as a
// SELECT.
func (conn *qlbStmt) Query(args []driver.Value) (driver.Rows, error) {}

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
func (conn *qlbStmt) ColumnConverter(idx int) driver.ValueConverter {}

//////////////////////////////////////////////////////////////////////////
// driver.Rows Interface implementation.
//
// Rows is an iterator over an executed query's results.
//
//////////////////////////////////////////////////////////////////////////
type qlbRows struct{}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice.  If a particular column name isn't known, an empty
// string should be returned for that entry.
func (conn *qlbRows) Columns() []string {

}

// Close closes the rows iterator.
func (conn *qlbRows) Close() error {

}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// The dest slice may be populated only with
// a driver Value type, but excluding string.
// All string values must be converted to []byte.
//
// Next should return io.EOF when there are no more rows.
func (conn *qlbRows) Next(dest []driver.Value) error {

}

//////////////////////////////////////////////////////////////////////////
// driver.Result Interface implementation.
//
// Result is the result of a query execution.
//
//////////////////////////////////////////////////////////////////////////
type qlbResult struct{}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (conn *qlbResult) LastInsertId() (int64, error) {}

// RowsAffected returns the number of rows affected by the
// query.
func (conn *qlbResult) RowsAffected() (int64, error) {}
