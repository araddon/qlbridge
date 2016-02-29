package datasource

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
)

var (
	// the global data sources registry mutex
	registryMu sync.Mutex
	// registry for data sources
	registry = newRegistry()
)

// Register makes a datasource available by the provided @sourceName
// If Register is called twice with the same name or if source is nil, it panics.
//
//  Sources are specific schemas of type csv, elasticsearch, etc containing
//    multiple tables
func Register(sourceName string, source schema.DataSource) {
	if source == nil {
		panic("qlbridge/datasource: Register DataSource is nil")
	}
	sourceName = strings.ToLower(sourceName)
	u.Debugf("global source register datasource: %v %T", sourceName, source)
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dupe := registry.sources[sourceName]; dupe {
		panic("qlbridge/datasource: Register called twice for datasource " + sourceName)
	}
	registry.sources[sourceName] = source
}

// get registry of all datasource types
func DataSourcesRegistry() *Registry {
	return registry
}

// Open a datasource, Globalopen connection function using
//  default schema registry
//
func OpenConn(sourceName, sourceConfig string) (schema.SourceConn, error) {
	sourcei, ok := registry.sources[sourceName]
	if !ok {
		return nil, fmt.Errorf("datasource: unknown source %q (forgotten import?)", sourceName)
	}
	source, err := sourcei.Open(sourceConfig)
	if err != nil {
		return nil, err
	}
	return source, nil
}

// The RuntimeSchema provides info on available datasources
//  given connection info, get datasource, ie a stateful "schema"
//
type RuntimeSchema struct {
	*Registry             // All registered DataSources
	connInfo       string // connection for sing
	DisableRecover bool   // If disableRecover=true, we will not capture/suppress panics
}

func NewRuntimeSchema() *RuntimeSchema {
	c := &RuntimeSchema{
		Registry: registry,
	}
	return c
}

// Get connection for given Database
//
//  @db      database name
//
func (m *RuntimeSchema) Conn(db string) schema.SourceConn {

	if m.connInfo == "" {
		//u.Debugf("RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
		if source := m.Get(strings.ToLower(db)); source != nil {
			//u.Debugf("found source: db=%s   %T", db, source)
			conn, err := source.Open(db)
			if err != nil {
				u.Errorf("could not open data source: %v  %v", db, err)
				return nil
			}
			//u.Infof("source: %T  %#v", conn, conn)
			return conn
		} else {
			u.Errorf("DataSource(%s) was not found", db)
		}
	} else {
		//u.Debugf("No Conn? RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
		// We have connection info, likely sq/driver
		source := m.DataSource(m.connInfo)
		//u.Infof("source=%v    about to call Conn() db='%v'", source, db)
		conn, err := source.Open(db)

		if err != nil {
			u.Errorf("could not open data source: %v  %v", db, err)
			return nil
		}
		return conn
	}
	return nil
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type Registry struct {
	// Map of source name, each source name is name of db in a specific source
	//   such as elasticsearch, mongo, csv etc
	sources map[string]schema.DataSource
	schemas map[string]*schema.Schema
	// We need to be able to flatten all tables across all sources into single keyspace
	tableSources map[string]schema.DataSource
	tables       []string
}

func newRegistry() *Registry {
	return &Registry{
		sources:      make(map[string]schema.DataSource),
		schemas:      make(map[string]*schema.Schema),
		tableSources: make(map[string]schema.DataSource),
		tables:       make([]string, 0),
	}
}

// Create a source schema from given named source
//  we will find DataSource for that name and introspect
func createSchema(sourceName string) (*schema.Schema, bool) {

	sourceName = strings.ToLower(sourceName)
	ss := schema.NewSourceSchema(sourceName, sourceName)

	u.Debugf("createSchema(%q)", sourceName)
	ds := registry.Get(sourceName)
	if ds == nil {
		u.Warnf("not able to find schema %q", sourceName)
		return nil, false
	}

	//u.Infof("reg p:%p ds %#v tables:%v", registry, ds, ds.Tables())
	ss.DS = ds
	schema := schema.NewSchema(sourceName)
	ss.Schema = schema
	for _, tableName := range ds.Tables() {
		ss.AddTableName(tableName)
	}

	schema.AddSourceSchema(ss)

	SystemSchemaCreate(schema)

	return schema, true
}

// Get schema for given source
//
//  @source      source/database name
//
func (m *Registry) Schema(source string) (*schema.Schema, bool) {

	u.Debugf("Schema(%q)", source)
	ss, ok := m.schemas[source]
	if ok && ss != nil {
		return ss, ok
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	ss, ok = createSchema(source)
	if ok {
		u.Debugf("datasource register schema %q %p", source, ss)
		m.schemas[source] = ss
	}
	return ss, ok
}

// Add a new Schema
func (m *Registry) SchemaAdd(s *schema.Schema) {
	registryMu.Lock()
	defer registryMu.Unlock()
	_, ok := m.schemas[s.Name]
	if ok {
		return
	}
	m.schemas[s.Name] = s
}

// Get table schema for given @tableName
//
func (m *Registry) Table(tableName string) (*schema.Table, error) {

	tableName = strings.ToLower(tableName)
	u.Debugf("Registry.Table(%q)", tableName)
	if source := m.Get(tableName); source != nil {
		if schemaSource, ok := source.(schema.SchemaProvider); ok {
			//u.Debugf("found source: db=%s   %T", db, source)
			tbl, err := schemaSource.Table(tableName)
			if err != nil {
				u.Errorf("could not get table for %q  err=%v", tableName, err)
				return nil, err
			}
			//u.Infof("table: %T  %#v", tbl, tbl)
			return tbl, nil
		} else {
			u.Warnf("%T didnt implement SchemaProvider", source)
		}
	} else {
		u.Warnf("Table(%q) was not found", tableName)
	}

	return nil, schema.ErrNotFound
}

// Get all tables from this registry
//
func (m *Registry) Tables() []string {
	if len(m.tables) == 0 {
		tbls := make([]string, 0)
		for _, src := range m.sources {
			for _, tbl := range src.Tables() {
				tbls = append(tbls, tbl)
			}
		}
		m.tables = tbls
	}
	return m.tables
}

// given connection info, get datasource
//  @connInfo =    csv:///dev/stdin
//                 mockcsv
func (m *Registry) DataSource(connInfo string) schema.DataSource {
	// if  mysql.tablename allow that convention
	u.Debugf("get datasource: conn=%q ", connInfo)
	//parts := strings.SplitN(from, ".", 2)
	// TODO:  move this to a csv, or other source not in global registry
	sourceType := ""
	if len(connInfo) > 0 {
		switch {
		// case strings.HasPrefix(name, "file://"):
		// 	name = name[len("file://"):]
		case strings.HasPrefix(connInfo, "csv://"):
			sourceType = "csv"
			//m.db = connInfo[len("csv://"):]
		case strings.Contains(connInfo, "://"):
			strIdx := strings.Index(connInfo, "://")
			sourceType = connInfo[0:strIdx]
			//m.db = connInfo[strIdx+3:]
		default:
			sourceType = connInfo
		}
	}

	sourceType = strings.ToLower(sourceType)
	//u.Debugf("source: %v", sourceType)
	if source := m.Get(sourceType); source != nil {
		//u.Debugf("source: %T", source)
		return source
	} else {
		u.Errorf("DataSource(conn) was not found: '%v'", sourceType)
	}

	return nil
}

// Get a Data Source, similar to DataSource(@connInfo)
// - tries first by sourcename
// - then tries by table name
func (m *Registry) Get(sourceName string) schema.DataSource {

	u.Debugf("Registry.Get(%q)", sourceName)
	if source, ok := m.sources[strings.ToLower(sourceName)]; ok {
		return source
	}
	if len(m.sources) == 1 {
		for _, src := range m.sources {
			return src
		}
	}
	if sourceName == "" {
		u.LogTracef(u.WARN, "No Source Name?")
		return nil
	}
	//u.Debugf("datasource.Get('%v')", sourceName)

	if len(m.tableSources) == 0 {
		for _, src := range m.sources {
			tbls := src.Tables()
			for _, tbl := range tbls {
				if _, ok := m.tableSources[tbl]; ok {
					u.Warnf("table names must be unique across sources %v", tbl)
				} else {
					//u.Debugf("creating tbl/source: %v  %T", tbl, src)
					m.tableSources[tbl] = src
				}
			}
		}
	}
	if src, ok := m.tableSources[sourceName]; ok {
		//u.Debugf("found src with %v", sourceName)
		return src
	} else {
		for src, _ := range m.sources {
			u.Debugf("source: %v", src)
		}
		u.LogTracef(u.WARN, "No table?  len(sources)=%d len(tables)=%v", len(m.sources), len(m.tableSources))
		u.Warnf("could not find table: %v  tables:%v", sourceName, m.tableSources)
	}
	return nil
}

func (m *Registry) String() string {
	sourceNames := make([]string, 0, len(m.sources))
	for source, _ := range m.sources {
		sourceNames = append(sourceNames, source)
	}
	return fmt.Sprintf("{Sources: [%s] }", strings.Join(sourceNames, ", "))
}
