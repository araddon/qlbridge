package datasource

import (
	"database/sql/driver"
	"fmt"
	"strings"
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

/*

DataSource:  Datasource config/definition, will open connections
                  if Conn's are not session/specific you may
                  return DataSource itself

SourceConn:  A connection to datasource, session/conn specific

*/

// Key interface is the Unique Key identifying a row
type Key interface {
	Key() driver.Value
}
type KeyInt struct {
	Id int
}

func (m *KeyInt) Key() driver.Value {
	return driver.Value(m.Id)
}

type KeyCol struct {
	Name string
	val  driver.Value
}

func NewKeyCol(name string, val driver.Value) KeyCol {
	return KeyCol{name, val}
}

func (m KeyCol) Key() driver.Value {
	return m.val
}

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

// Connection, only one guaranteed feature, although
// should implement many more (scan, seek, etc)
type SourceConn interface {
	Close() error
}

// Some sources can do their own planning
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

// Mutation interface for Put
//  - assumes datasource understands key(s?)
type Upsert interface {
	Put(ctx context.Context, key Key, value interface{}) (Key, error)
	PutMulti(ctx context.Context, keys []Key, src interface{}) ([]Key, error)
}

// Patch Where, pass through where expression to underlying datasource
type PatchWhere interface {
	PatchWhere(ctx context.Context, where expr.Node, patch interface{}) (int, error)
}

// Delete with given expression
type Deletion interface {
	Delete(driver.Value) (int, error)
	DeleteExpression(expr.Node) (int, error)
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type DataSources struct {
	sources      map[string]DataSource
	tableSources map[string]DataSource
}

func newDataSources() *DataSources {
	return &DataSources{
		sources:      make(map[string]DataSource),
		tableSources: make(map[string]DataSource),
	}
}

// We do type introspection in advance to speed up runtime
// feature detection for datasources
type Features struct {
	SourcePlanner bool
	Scanner       bool
	Seeker        bool
	WhereFilter   bool
	GroupBy       bool
	Sort          bool
	Aggregations  bool
	Projection    bool
	Upsert        bool
	PatchWhere    bool
	Deletion      bool
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

func (m *DataSources) Get(sourceType string) *DataSourceFeatures {
	if source, ok := m.sources[strings.ToLower(sourceType)]; ok {
		//u.Debugf("found source: %v", sourceType)
		return NewFeaturedSource(source)
	}
	if len(m.sources) == 1 {
		for _, src := range m.sources {
			//u.Debugf("only one source?")
			return NewFeaturedSource(src)
		}
	}
	if sourceType == "" {
		u.LogTracef(u.WARN, "No Source Type?")
	} else {
		u.Debugf("datasource.Get('%v')", sourceType)
	}

	if len(m.tableSources) == 0 {
		for _, src := range m.sources {
			tbls := src.Tables()
			for _, tbl := range tbls {
				if _, ok := m.tableSources[tbl]; ok {
					u.Warnf("table names must be unique across sources %v", tbl)
				} else {
					u.Debugf("creating tbl/source: %v  %T", tbl, src)
					m.tableSources[tbl] = src
				}
			}
		}
	}
	if src, ok := m.tableSources[sourceType]; ok {
		//u.Debugf("found src with %v", sourceType)
		return NewFeaturedSource(src)
	} else {
		for src, _ := range m.sources {
			u.Debugf("source: %v", src)
		}
		u.LogTracef(u.WARN, "No table?  len(sources)=%d len(tables)=%v", len(m.sources), len(m.tableSources))
		u.Warnf("could not find table: %v  tables:%v", sourceType, m.tableSources)
	}
	return nil
}

func (m *DataSources) String() string {
	sourceNames := make([]string, 0, len(m.sources))
	for source, _ := range m.sources {
		sourceNames = append(sourceNames, source)
	}
	return fmt.Sprintf("{Sources: [%s] }", strings.Join(sourceNames, ", "))
}

// get registry of all datasource types
func DataSourcesRegistry() *DataSources {
	return sources
}

// Register makes a datasource available by the provided name.
// If Register is called twice with the same name or if source is nil,
// it panics.
func Register(name string, source DataSource) {
	if source == nil {
		panic("qlbridge/datasource: Register driver is nil")
	}
	name = strings.ToLower(name)
	u.Warnf("register datasource: %v %T", name, source)
	//u.LogTracef(u.WARN, "adding source %T to registry", source)
	sourceMu.Lock()
	defer sourceMu.Unlock()
	if _, dup := sources.sources[name]; dup {
		panic("qlbridge/datasource: Register called twice for datasource " + name)
	}
	sources.sources[name] = source
}

// Open a datasource
//  sourcename = "csv", "elasticsearch"
func OpenConn(sourceName, sourceConfig string) (SourceConn, error) {
	sourcei, ok := sources.sources[sourceName]
	if !ok {
		return nil, fmt.Errorf("datasource: unknown source %q (forgotten import?)", sourceName)
	}
	source, err := sourcei.Open(sourceConfig)
	if err != nil {
		return nil, err
	}
	return source, nil
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
