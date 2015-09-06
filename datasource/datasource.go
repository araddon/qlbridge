package datasource

import (
	"database/sql/driver"
	"fmt"
	"sync"

	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// the data sources registry mutex
	sourceMu sync.Mutex
	// registry for data sources
	sources = newDataSources()

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
//  - ??  Accept() or VisitSelect()  not yet implemented
//
// Optional Select Features:
//  - Seek          ie, key-value lookup, or indexed rows
//  - Projection    ie, selecting specific fields
//  - Where         filtering response
//  - GroupBy
//  - Aggregations  ie, count(*), avg()   etc
//  - Sort          sort response, very important for fast joins
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
type DataSource interface {
	Tables() []string
	Open(connInfo string) (SourceConn, error)
	Close() error
}

// A backend data source provider that also provides schema
type SchemaProvider interface {
	DataSource
	Table(table string) (*Table, error)
}

// DataSource Connection, only one guaranteed feature, although
//  should implement many more (scan, seek, etc)
type SourceConn interface {
	Close() error
}

// Some sources can do their own planning
type SourceSelectPlanner interface {
	// Accept a sql statement, to plan the execution ideally, this would be done
	// by planner but, we need source specific planners, as each backend has different features
	//Accept(expr.Visitor) (Scanner, error)
	VisitSelect(stmt *expr.SqlSelect) (interface{}, error)
}

// Some sources can do their own planning for sub-select statements
type SourcePlanner interface {
	// Accept a sql statement, to plan the execution ideally, this would be done
	// by planner but, we need source specific planners, as each backend has different features
	Accept(expr.SubVisitor) (Scanner, error)
}

// A scanner, most basic of data sources, just iterate through
//  rows without any optimizations
type Scanner interface {
	ScannerColumns
	// create a new iterator for underlying datasource
	CreateIterator(filter expr.Node) Iterator
	MesgChan(filter expr.Node) <-chan Message
}

// Interface for a data source exposing column positions for []driver.Value iteration
type ScannerColumns interface {
	Columns() []string
}

// simple iterator interface for paging through a datastore Messages/rows
// - used for scanning
// - for datasources that implement exec.Visitor() (ie, select) this
//    represents the alreader filtered, calculated rows
type Iterator interface {
	Next() Message
}

// Interface for Seeking row values instead of scanning (ie, Indexed)
type Seeker interface {
	DataSource
	// Just because we have Get, Multi-Get, doesn't mean we can seek all
	// expressions, find out with CanSeek for given expression
	CanSeek(*expr.SqlSelect) bool
	Get(key driver.Value) (Message, error)
	MultiGet(keys []driver.Value) ([]Message, error)
}

type WhereFilter interface {
	DataSource
	Filter(expr.SqlStatement) error
}

type GroupBy interface {
	DataSource
	GroupBy(expr.SqlStatement) error
}

type Sort interface {
	DataSource
	Sort(expr.SqlStatement) error
}

type Aggregations interface {
	DataSource
	Aggregate(expr.SqlStatement) error
}

// Some data sources that implement more features, can provide
//  their own projection.
type Projection interface {
	// Describe the Columns etc
	Projection() (*expr.Projection, error)
}

// SourceMutation, is a statefull connetion similar to Open() connection for select
//  - accepts the tble used in this upsert/insert/update
//
type SourceMutation interface {
	Create(tbl *Table, stmt expr.SqlStatement) (Mutator, error)
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

// We do type introspection in advance to speed up runtime
// feature detection for datasources
type Features struct {
	SourcePlanner  bool
	Scanner        bool
	Seeker         bool
	WhereFilter    bool
	GroupBy        bool
	Sort           bool
	Aggregations   bool
	Projection     bool
	SourceMutation bool
	Upsert         bool
	PatchWhere     bool
	Deletion       bool
}
type DataSourceFeatures struct {
	Features *Features
	DataSource
}

func NewFeaturedSource(src DataSource) *DataSourceFeatures {
	return &DataSourceFeatures{NewFeatures(src), src}
}
func NewFeatures(src DataSource) *Features {
	f := Features{}
	if _, ok := src.(Scanner); ok {
		f.Scanner = true
	}
	if _, ok := src.(Seeker); ok {
		f.Seeker = true
	}
	if _, ok := src.(WhereFilter); ok {
		f.WhereFilter = true
	}
	if _, ok := src.(GroupBy); ok {
		f.GroupBy = true
	}
	if _, ok := src.(Sort); ok {
		f.Sort = true
	}
	if _, ok := src.(Aggregations); ok {
		f.Aggregations = true
	}
	if _, ok := src.(Projection); ok {
		f.Projection = true
	}
	if _, ok := src.(SourceMutation); ok {
		f.SourceMutation = true
	}
	if _, ok := src.(Upsert); ok {
		f.Upsert = true
	}
	if _, ok := src.(PatchWhere); ok {
		f.PatchWhere = true
	}
	if _, ok := src.(Deletion); ok {
		f.Deletion = true
	}
	return &f
}

func SourceIterChannel(iter Iterator, filter expr.Node, sigCh <-chan bool) <-chan Message {

	out := make(chan Message, 100)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				u.Errorf("recover panic: %v", r)
			}
			// Can we safely close this?
			close(out)
		}()
		for item := iter.Next(); item != nil; item = iter.Next() {

			//u.Infof("In source Scanner iter %#v", item)
			select {
			case <-sigCh:
				u.Warnf("got signal quit")

				return
			case out <- item:
				// continue
			}
		}
	}()
	return out
}
