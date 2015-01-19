package datasource

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	//"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// the data sources mutex
	sourceMu sync.Mutex
	// registry for data sources
	sources = newDataSources()
)

// Super simple iterator interface
type Iterator interface {
	Next() Message
}

// A datasource is most likely a database, csv file, set of channels, etc
// something that provides input which can be evaluated
type DataSource interface {
	// create a new iterator for underlying data
	CreateIterator(filter expr.Node) Iterator

	Open(connInfo string) (DataSource, error)

	Close() error
}

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

// get registry
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
//  sourcename = "csv"
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
