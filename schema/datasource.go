package schema

import (
	"database/sql/driver"
	"fmt"

	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var (
	// ErrNotFound is error expressing sought item was not found.
	ErrNotFound = fmt.Errorf("Not Found")
	// ErrNotImplemented this feature is not implemented for this source.
	ErrNotImplemented = fmt.Errorf("Not Implemented")
)

type (
	// Source is an interface describing a datasource such as a database, file, api,
	// in-mem data etc. It is thread-safe, singleton, responsible for creating connections and
	// exposing schema. It also exposes partition information optionally if a distributed source.
	//
	// Sources are registered in a registry, to be dynamically created as schema demands.
	//
	// Lifecycle:
	//
	//   Init()
	//   Setup()
	//   // running ....  Open() , Table(name)  etc .....
	//   Close()
	//
	Source interface {
		// Init provides opportunity for those sources that require/ no configuration and
		// introspect schema from their environment time to load pre-schema discovery
		Init()
		// Setup optional interface for getting the Schema injected during creation/starup.
		// Since the Source is a singleton, stateful manager,  it has a startup/shutdown process.
		Setup(*Schema) error
		// Close this source, ensure connections, underlying resources are closed.
		Close() error
		// Open create a connection (not thread safe) to this source.
		Open(source string) (Conn, error)
		// Tables is a list of table names provided by this source.
		Tables() []string
		// Table get table schema for given table name.
		Table(table string) (*Table, error)
	}
	// SourceTableSchema Partial interface from Source to define just Table()
	SourceTableSchema interface {
		Table(table string) (*Table, error)
	}
	// SourcePartitionable is an optional interface a source may implement that announces it (source)
	// as partitionable into ranges for splitting reads, writes onto different nodes of a cluster.
	//
	// Many databases's already have internal Partition schemas this allow's those to
	// be exposed for use in our partitioning, so the query-planner can distributed work across nodes.
	SourcePartitionable interface {
		// Partitions list of partitions.
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
	// Conn A Connection/Session to a file, api, backend database.  Depending on the features
	// of the backing source, it may optionally implement different portions of this interface.
	//
	// Minimum Read Features to provide Sql Select
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
	// ConnAll interface describes the FULL set of features a connection can implement.
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
		Columns() []string
	}
	// ConnScanner is the primary basis for reading data sources.  It exposes
	// an interface to scan through rows.  If the Source supports Predicate
	// Push Down (ie, push the where/sql down to underlying store) this is
	// just the resulting rows.  Otherwise, Qlbridge engine must polyfill.
	ConnScanner interface {
		Conn
		Iterator
	}
	// Iterator is simple iterator for paging through a datastore Message(rows)
	// to be used for scanning.  Building block for Tasks that process part of
	// a DAG of tasks to process data.
	Iterator interface {
		// Next returns the next message.  If none remain, returns nil.
		Next() Message
	}
	// ConnSeeker is a conn that is Key-Value store, allows relational
	// implementation to be faster for Seeking row values instead of scanning
	ConnSeeker interface {
		Get(key driver.Value) (Message, error)
	}
	// ConnMutation creates a Mutator connection similar to Open() connection for select
	// - accepts the plan context used in this upsert/insert/update
	// - returns a connection which must be closed
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
	// Used for update statements WHERE x = y
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
