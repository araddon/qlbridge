package schema

import (
	"database/sql/driver"
	"fmt"

	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	ErrNotFound       = fmt.Errorf("Not Found")
	ErrNotImplemented = fmt.Errorf("Not Implemented")
)

type (
	// Source is factory registered to create connections to a backing dastasource.
	// Datasources are most likely a database, file, api, in-mem data etc.
	// It is thread-safe, singleton, responsible for creating connections and
	// exposing schema and performing ddl operations. It also exposes partition information
	// optionally if a distributed source, or able to be distributed queries.
	//
	// DDL/Schema Operations
	// - schema discovery, tables, columns etc
	// - create
	// - index
	//
	// Lifecycle:
	// - Init()
	// - Setup()
	// - running ....
	// - Close()
	Source interface {
		// Init provides opportunity for those sources that require
		// no configuration and sniff schema from their environment time
		// to load pre-schema discovery
		Init()
		// Setup A Datasource optional interface for getting the SourceSchema injected
		// during creation/starup.  Since the Source is a singleton, stateful manager
		// it has a startup/shutdown process.
		Setup(*SchemaSource) error
		// Close
		Close() error
		// Open create a connection (not thread safe) to this source
		Open(source string) (Conn, error)
		// List of tables provided by this source
		Tables() []string
		// provides table schema info
		Table(table string) (*Table, error)
		// Create/Alter TODO
	}
	// SourcePartitionable DataSource that is partitionable into ranges for splitting
	//  reads, writes onto different nodes.
	SourcePartitionable interface {
		// Many databases's already have internal Partitions, allow those to
		// be exposed for use in our partitioning
		Partitions() []*Partition
		PartitionSource(p *Partition) (Conn, error)
	}
	// SourceTableColumn is a partial source that just provides access to
	// Column schema info, used in Generators.
	SourceTableColumn interface {
		// Underlying data type of column
		Column(col string) (value.ValueType, bool)
	}
)

type (
	// Conn A Connection/Session to a file, api, backend database.  Provides DML operations.
	//
	// Minimum Read Features to provide Sql Select:
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
	// DataSource Connection Session that is Stateful.  this is
	// really a generic interface, will actually implement features
	// below:  SchemaColumns, Scanner, Seeker, Mutator
	Conn interface {
		Close() error
	}
	// ConnAll interface
	ConnAll interface {
		Close() error
		ConnColumns
		Iterator
		ConnSeeker
		ConnUpsert
		ConnDeletion
	}
	// ConnColumns Interface for a data source connection exposing column positions for []driver.Value iteration
	ConnColumns interface {
		//Conn
		Columns() []string
	}
	// ConnScanner is the most basic of data sources, just iterate through
	//  rows without any optimizations.  Key-Value store like csv, redis, cassandra.
	ConnScanner interface {
		Conn
		Iterator
	}
	// ConnScannerIterator Another advanced iterator, probably deprecate?
	ConnScannerIterator interface {
		//Conn
		// create a new iterator to scan through row by row
		CreateIterator() Iterator
		MesgChan() <-chan Message
	}
	// ConnSeeker is a datsource that is Key-Value store, allows relational
	//  implementation to be faster for Seeking row values instead of scanning
	ConnSeeker interface {
		//Conn
		// Just because we have Get, Multi-Get, doesn't mean we can seek all
		// expressions, find out with CanSeek for given expression
		CanSeek(*rel.SqlSelect) bool
		Get(key driver.Value) (Message, error)
		MultiGet(keys []driver.Value) ([]Message, error)
	}
	// ConnMutation creates a Mutator connection similar to Open() connection for select
	//  - accepts the plan context used in this upsert/insert/update
	//  - returns a connection which must be closed
	ConnMutation interface {
		CreateMutator(pc interface{} /*plan.Context*/) (ConnMutator, error)
	}
	// ConnMutator Mutator Connection
	ConnMutator interface {
		ConnUpsert
		ConnDeletion
	}
	// ConnUpsert Mutation interface for Put
	//  - assumes datasource understands key(s?)
	ConnUpsert interface {
		Put(ctx context.Context, key Key, value interface{}) (Key, error)
		PutMulti(ctx context.Context, keys []Key, src interface{}) ([]Key, error)
	}
	// ConnPatchWhere pass through where expression to underlying datasource
	//  Used for update statements WHERE x = y
	ConnPatchWhere interface {
		PatchWhere(ctx context.Context, where expr.Node, patch interface{}) (int64, error)
	}
	// ConnDeletion deletion interface for data sources
	ConnDeletion interface {
		// Delete using this key
		Delete(driver.Value) (int, error)
		// Delete with given expression
		DeleteExpression(p interface{} /* plan.Delete */, n expr.Node) (int, error)
	}
)
