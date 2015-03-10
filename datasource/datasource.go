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

// A datasource is most likely a database, csv file, set of channels, etc
// something that provides input which can be evaluated and at a minimum provide:
// - Scanning:   iterate through messages/rows
//
// Optionally:
//  - Seeking (ie, key-value lookup)
type DataSource interface {
	// create a new iterator for underlying datasource
	CreateIterator(filter expr.Node) Iterator

	Open(connInfo string) (DataSource, error)

	Close() error
}

// simple iterator interface for paging through a datastore Messages/rows
// - used for scanning
// - for datasources that implement exec.Visitor() (ie, select) this
//    represents the alreader filtered, calculated rows
type Iterator interface {
	Next() Message
}

// Super simple seek interface for finding specific key(s)
type Seeker interface {
	Get(key string) Message
	MultiGet(keys []string) []Message
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
