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
)

// A datasource is most likely a database, csv file, etc
// something that provides input which can be evaluated and at a minimum provide:
// - Scanning:   iterate through messages/rows
//
// Optionally:
//  - Seeking (ie, key-value lookup, or indexed rows)
//  - Projection    (ie, selecting specific fields)
//  - Where
//  - GroupBy
//  - Aggregations  ie, count(*), avg()   etc
//  - Sort
//  - Delete
//  - Update
//  - Upsert
//  - Insert
// Dml/Schema
//  - schema discovery
type DataSource interface {
	Open(connInfo string) (DataSource, error)
	Close() error
}

type Scanner interface {
	// create a new iterator for underlying datasource
	CreateIterator(filter expr.Node) Iterator
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
	CanSeek(*expr.SqlSelect)
	Get(key string) Message
	MultiGet(keys []string) []Message
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
	Sort(expr.SqlStatement) error
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type DataSources struct {
	sources map[string]DataSource
}

func newDataSources() *DataSources {
	return &DataSources{
		sources: make(map[string]DataSource),
	}
}

func (m *DataSources) Get(sourceType string) DataSource {
	return m.sources[strings.ToLower(sourceType)]
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
func Open(sourceName, sourceConfig string) (DataSource, error) {
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
