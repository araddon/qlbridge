package datasource

import (
	"fmt"
	"net/url"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/ast"
	//"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// the func mutext
	sourceMu sync.Mutex
	//var drivers = make(map[string]driver.Driver)
	dataSources = make(map[string]DataSource)
)

// represents a message routable by the topology. The Key() method
// is used to route the message in certain topologies. Body() is used
// to express something user specific.
// see  "https://github.com/mdmarek/topo" AND http://github.com/lytics/grid
type Message interface {
	Key() uint64
	Body() interface{}
}

type UrlValuesMsg struct {
	body url.Values
	id   uint64
}

func (m *UrlValuesMsg) Key() uint64       { return m.id }
func (m *UrlValuesMsg) Body() interface{} { return m.body }

// Super simple iterator interface
type Iterator interface {
	Next() Message
}

// A datasource is most likely a database, csv file, set of channels, etc
// something that provides input which can be evaluated
type DataSource interface {
	// Meta-data about this data source, or Schema() *Schema  or something?
	//MetaData(id uint32, keys []string) []string

	//Field(name, field string) (fieldID uint8, valType value.ValueType)

	// create a new iterator for underlying data
	CreateIterator(filter *ast.Tree) Iterator

	Open(connInfo string) (DataSource, error)
	//Clone() DataSource
}

// Register makes a datasource available by the provided name.
// If Register is called twice with the same name or if source is nil,
// it panics.
func Register(name string, source DataSource) {
	if source == nil {
		panic("qlbridge/datasource: Register driver is nil")
	}
	sourceMu.Lock()
	defer sourceMu.Unlock()
	if _, dup := dataSources[name]; dup {
		panic("qlbridge/datasource: Register called twice for datasource " + name)
	}
	dataSources[name] = source
}

func Open(sourceName, sourceConfig string) (DataSource, error) {
	sourcei, ok := dataSources[sourceName]
	if !ok {
		return nil, fmt.Errorf("datasource: unknown source %q (forgotten import?)", sourceName)
	}
	source, err := sourcei.Open(sourceConfig)
	if err != nil {
		return nil, err
	}
	return source, nil
}
