package schema

import (
	"database/sql/driver"
	"fmt"

	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
)

var (
	// Some common errors
	ErrNotFound       = fmt.Errorf("Not Found")
	ErrNotImplemented = fmt.Errorf("Not Implemented")
)

// A datasource is most likely a database, file, api, in-mem data etc that we
// are modeling as a database to provide a subset of Get, Scan, Put, Delete operations.
//
// The source may be read only (Get, Scan) such as csv files, json api's
//
// Sources do not have to implement all features of a database
// scan/seek/sort/filter/group/aggregate, in which case we will use our own
// execution engine to Polyfill the features where possible.  This allows us to take
// a scan-only source such as csv, json api and implement full SQL queries on it.
//
// Minimum Read Features:
//  - Scanning:   iterate through messages/rows
//  - Schema Tables:  at a minium list of tables available, the column level data
//                    can be introspected so is optional
//
// Planning:
//  - CreateMutator(ctx *plan.Context) :  execute a mutation task insert, delete, update
//
//
// Non Select based Sql DML Operations for Mutator:
//  - Deletion:    (sql delete)
//      Delete()
//      DeleteExpression()
//  - Upsert Interface   (sql Update, Upsert, Insert)
//      Put()
//      PutMulti()
//
// DDL/Schema Operations
//  - schema discovery, tables, columns etc
//  - create
//  - index

// A datasource is most likely a database, file, api, in-mem data etc
// - the base implementation for any of the other datasource features
type DataSource interface {
	Tables() []string
	Open(source string) (SourceConn, error)
	Close() error
}

// needs the stateful schema
type SourceSetup interface {
	Setup(*SourceSchema) error
}

// A data source provider that also provides schema
type SchemaProvider interface {
	DataSource
	Table(table string) (*Table, error)
}

// DataSource Connection that is Stateful, only one guaranteed feature, although
//  should implement many more (scan, seek, etc)
type SourceConn interface {
	Close() error
}

// Interface for a data source connection exposing column positions for []driver.Value iteration
type SchemaColumns interface {
	Columns() []string
}

// DataSource that is partitionable into ranges for splitting
//  reads, writes onto different nodes.
type SourcePartitionable interface {
	// Create Partitions
	CreatePartitions(ct int) error
	// Many databases's already have internal Partitions, allow those to
	// be using in calculations of partitions
	Partitions() []*Partition
	PartitionSource(p *Partition) (SourceConn, error)
}

// simple iterator interface for paging through a datastore Messages/rows
// - used for scanning
// - for datasources that implement exec.Visitor() (ie, select) this
//    represents the alreader filtered, calculated rows
type Iterator interface {
	Next() Message
}

// A scanner is the most basic of data sources, just iterate through
//  rows without any optimizations.  Key-Value store like csv, redis, cassandra.
type Scanner interface {
	SchemaColumns
	SourceConn
	// create a new iterator to scan through row by row
	CreateIterator(filter expr.Node) Iterator
	MesgChan(filter expr.Node) <-chan Message
}

// A seeker is a datsource that is Key-Value store, allows relational
//  implementation to be faster for Seeking row values instead of scanning
type Seeker interface {
	DataSource
	// Just because we have Get, Multi-Get, doesn't mean we can seek all
	// expressions, find out with CanSeek for given expression
	CanSeek(*rel.SqlSelect) bool
	Get(key driver.Value) (Message, error)
	MultiGet(keys []driver.Value) ([]Message, error)
}

// SourceMutation, creates a Mutator connection similar to Open() connection for select
//  - accepts the plan context used in this upsert/insert/update
//  - returns a connection which must be closed
type SourceMutation interface {
	CreateMutator(pc interface{} /*plan.Context*/) (Mutator, error)
}

// Mutator Connection
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

// Delete interface for data sources
type Deletion interface {
	// Delete using this key
	Delete(driver.Value) (int, error)
	// Delete with given expression
	DeleteExpression(expr.Node) (int, error)
}
