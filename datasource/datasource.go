package datasource

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// the data sources mutex
	sourceMu sync.Mutex
	// registry for data sources
	sources = newDataSources()

	// ensure our DataSourceFeatures is also DataSource
	_ DataSource = (*DataSourceFeatures)(nil)
)

/*

DataSource:   = Datasource config/defintion, will open connections
                  if Conn's are not session/specific you may
                  return DataSource itself

SourceConn:   = A connection to datasource, session/conn specific




*/

// We do type introspection in advance to speed up runtime
// feature detection for datasources
type Features struct {
	Scan         bool
	Seek         bool
	Where        bool
	GroupBy      bool
	Sort         bool
	Aggregations bool
}

// A datasource is most likely a database, file, api, in-mem data etc
// something that provides input which can be evaluated and at a minimum provide:
// - Scanning:   iterate through messages/rows
//
// Optionally:
//  - Seek          ie, key-value lookup, or indexed rows
//  - Projection    ie, selecting specific fields
//  - Where         filtering response
//  - GroupBy
//  - Aggregations  ie, count(*), avg()   etc
//  - Sort          sort response, very important for fast joins
//
//  - Delete
//  - Update
//  - Upsert
//  - Insert
//
// Dml/Schema
//  - schema discovery
//  - create
//  - index
type DataSource interface {
	Tables() []string
	Open(connInfo string) (SourceConn, error)
	Close() error
}

// Connection
type SourceConn interface {
	Close() error
}

type DataSourceFeatures struct {
	Features Features
	DataSource
}

// A scanner, most basic of data sources, just iterate through
//  rows without any optimizations
type Scanner interface {
	// create a new iterator for underlying datasource
	CreateIterator(filter expr.Node) Iterator
	MesgChan(filter expr.Node) <-chan Message
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
	// Just because we have Get, Multi-Get, doesn't mean we can seek all
	// expressions, find out.
	CanSeek(*expr.SqlSelect)
	Get(key string) Message
	MultiGet(keys []string) []Message
	// any seeker must also be a Scanner?
	//Scanner
}

type WhereFilter interface {
	Filter(expr.SqlStatement) error
}

type GroupBy interface {
	GroupBy(expr.SqlStatement) error
}

type Sort interface {
	Sort(expr.SqlStatement) error
}

type Aggregations interface {
	Aggregate(expr.SqlStatement) error
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

func (m *DataSources) Get(sourceType string) DataSource {
	if source, ok := m.sources[strings.ToLower(sourceType)]; ok {
		return source
	}
	if len(m.sources) == 1 {
		for _, src := range m.sources {
			return src
		}
	}
	u.Warnf("what are we getting? %v", sourceType)
	if len(m.tableSources) == 0 {
		for _, src := range m.sources {
			tbls := src.Tables()
			for _, tbl := range tbls {
				if _, ok := m.tableSources[tbl]; ok {
					u.Warnf("table names must be unique across sources %v", tbl)
				} else {
					m.tableSources[tbl] = src
				}
			}
		}
	}
	if src, ok := m.tableSources[sourceType]; ok {
		return src
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
