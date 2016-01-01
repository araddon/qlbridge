package datasource

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY

	// Some common errors
	ErrNotFound = fmt.Errorf("Not Found")
)

// A datasource is most likely a database, file, api, in-mem data etc
// something that provides data rows.  If the source is a sql database
// it can do its own planning/implementation.
//
// However sources do not have to implement all features of a database
// scan/seek/sort/filter/group/aggregate, in which case we will use our own
// execution engine to "Polyfill" the features
//
// Minimum Features:
//  - Scanning:   iterate through messages/rows, use expr to evaluate
//                this is the minium we need to implement sql select
//  - Schema Tables:  at a minium tables available, the column level data
//                    can be introspected so is optional
//
// Planning:
//  - CreateMutator(ctx *plan.Context) :  execute a mutation task
//
//
// Non Select based Sql DML Operations:
//  - Deletion:    (sql delete)
//      Delete()
//      DeleteExpression()
//  - Upsert Interface   (sql Update, Upsert, Insert)
//      Put()
//      PutMulti()
//
// DDL/Schema Operations
//  - schema discovery
//  - create
//  - index

// A scanner, most basic of data sources, just iterate through
//  rows without any optimizations
type Scanner interface {
	schema.SchemaColumns
	schema.SourceConn
	// create a new iterator to scan through row by row
	CreateIterator(filter expr.Node) Iterator
	MesgChan(filter expr.Node) <-chan Message
}

// Interface for Seeking row values instead of scanning (ie, Indexed)
type Seeker interface {
	schema.DataSource
	// Just because we have Get, Multi-Get, doesn't mean we can seek all
	// expressions, find out with CanSeek for given expression
	CanSeek(*expr.SqlSelect) bool
	Get(key driver.Value) (Message, error)
	MultiGet(keys []driver.Value) ([]Message, error)
}

// SourceMutation, is a statefull connection similar to Open() connection for select
//  - accepts the stmt used in this upsert/insert/update
//
type SourceMutation interface {
	CreateMutator(ctx *plan.Context) (Mutator, error)
}

type Mutator interface {
	Upsert
	Deletion
}

// Mutation interface for Put
//  - assumes datasource understands key(s?)
type Upsert interface {
	Put(ctx context.Context, key Key, value interface{}) (Key, error)
	PutMulti(ctx context.Context, keys []Key, src interface{}) ([]Key, error)
}

// Patch Where, pass through where expression to underlying datasource
//  Used for update statements WHERE x = y
type PatchWhere interface {
	PatchWhere(ctx context.Context, where expr.Node, patch interface{}) (int64, error)
}

type Deletion interface {
	// Delete using this key
	Delete(driver.Value) (int, error)
	// Delete with given expression
	DeleteExpression(expr.Node) (int, error)
}
