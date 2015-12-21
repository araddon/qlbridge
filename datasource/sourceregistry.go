package datasource

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
)

var (
	// the global data sources registry mutex
	sourceMu sync.Mutex
	// registry for data sources
	sources = newDataSources()
)

// The RuntimeSchema provides info on available datasources
//  given connection info, get datasource
//
type RuntimeSchema struct {
	schemas        map[string]*Schema
	Sources        *DataSources // All registered DataSources
	connInfo       string       // db.driver only allows one connection, this is default
	db             string       // db.driver only allows one db, this is default
	DisableRecover bool         // If disableRecover=true, we will not capture/suppress panics
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type DataSources struct {
	// Map of source name, each source name is name of db in a specific source
	//   such as elasticsearch, mongo, csv etc
	sources map[string]DataSource
	// We need to be able to flatten all tables across all sources into single keyspace
	tableSources map[string]DataSource
	tables       []string
}

// Open a datasource, Globalopen connection function using
//  default schema registry
//
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

// Create a source schema from datasource
func SchemaFromSource(sourceName string) (*Schema, bool) {

	sourceName = strings.ToLower(sourceName)
	ss := NewSourceSchema(sourceName, sourceName)

	ds := sources.Get(sourceName)
	//u.Infof("ds %#v tables:%v", ds, ds.Tables())
	ss.DS = ds.DataSource
	ss.DSFeatures = ds
	for _, tableName := range ds.Tables() {
		//u.Debugf("table load: %q", tableName)
		//ss.AddTable(tableName)
		ss.AddTableName(tableName)
	}

	schema := NewSchema(sourceName)
	ss.Schema = schema
	schema.AddSourceSchema(ss)

	return schema, true
}

func NewRuntimeSchema() *RuntimeSchema {
	c := &RuntimeSchema{
		Sources: DataSourcesRegistry(),
	}
	return c
}

// Our RunTime configuration possibly only supports a single schema/connection
// info.  for example, the sql/driver interface, so will be set here.
//
//  @connInfo =    csv:///dev/stdin
//
func (m *RuntimeSchema) SetConnInfo(connInfo string) {
	m.connInfo = connInfo
}

// Get connection for given Database
//
//  @db      database name
//
func (m *RuntimeSchema) Conn(db string) SourceConn {

	if m.connInfo == "" {
		//u.Debugf("RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
		if source := m.Sources.Get(strings.ToLower(db)); source != nil {
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

// Get table schema for given @tableName
//
func (m *RuntimeSchema) Table(tableName string) (*Table, error) {

	tableName = strings.ToLower(tableName)
	//u.Debugf("RuntimeSchema.Table(%q)  //  connInfo='%v'", Table, m.connInfo)
	if source := m.Sources.Get(tableName); source != nil {
		if schemaSource, ok := source.DataSource.(SchemaProvider); ok {
			//u.Debugf("found source: db=%s   %T", db, source)
			tbl, err := schemaSource.Table(tableName)
			if err != nil {
				u.Errorf("could not get table for %q  err=%v", tableName, err)
				return nil, err
			}
			//u.Infof("table: %T  %#v", tbl, tbl)
			return tbl, nil
		} else {
			u.Warnf("%T didnt implement SchemaProvider", source.DataSource)
		}
	} else {
		u.Warnf("Table(%q) was not found", tableName)
	}

	return nil, ErrNotFound
}

// Get all tables
//
func (m *RuntimeSchema) Tables() []string {
	if len(m.Sources.tables) == 0 {
		tbls := make([]string, 0)
		for _, src := range m.Sources.sources {
			for _, tbl := range src.Tables() {
				tbls = append(tbls, tbl)
			}
		}
		m.Sources.tables = tbls
	}
	return m.Sources.tables
}

// given connection info, get datasource
//  @connInfo =    csv:///dev/stdin
//                 mockcsv
func (m *RuntimeSchema) DataSource(connInfo string) DataSource {
	// if  mysql.tablename allow that convention
	//u.Debugf("get datasource: conn=%v ", connInfo)
	//parts := strings.SplitN(from, ".", 2)
	sourceType := ""
	if len(connInfo) > 0 {
		switch {
		// case strings.HasPrefix(name, "file://"):
		// 	name = name[len("file://"):]
		case strings.HasPrefix(connInfo, "csv://"):
			sourceType = "csv"
			m.db = connInfo[len("csv://"):]
		case strings.Contains(connInfo, "://"):
			strIdx := strings.Index(connInfo, "://")
			sourceType = connInfo[0:strIdx]
			m.db = connInfo[strIdx+3:]
		default:
			sourceType = connInfo
		}
	}

	sourceType = strings.ToLower(sourceType)
	//u.Debugf("source: %v", sourceType)
	if source := m.Sources.Get(sourceType); source != nil {
		//u.Debugf("source: %T", source)
		return source
	} else {
		u.Errorf("DataSource(conn) was not found: '%v'", sourceType)
	}

	return nil
}

func newDataSources() *DataSources {
	return &DataSources{
		sources:      make(map[string]DataSource),
		tableSources: make(map[string]DataSource),
		tables:       make([]string, 0),
	}
}

func (m *DataSources) Get(sourceName string) *DataSourceFeatures {
	if source, ok := m.sources[strings.ToLower(sourceName)]; ok {
		u.Debugf("found source: %v", sourceName)
		return NewFeaturedSource(source)
	}
	if len(m.sources) == 1 {
		for _, src := range m.sources {
			//u.Debugf("only one source?")
			return NewFeaturedSource(src)
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
		return NewFeaturedSource(src)
	} else {
		for src, _ := range m.sources {
			u.Debugf("source: %v", src)
		}
		u.LogTracef(u.WARN, "No table?  len(sources)=%d len(tables)=%v", len(m.sources), len(m.tableSources))
		u.Warnf("could not find table: %v  tables:%v", sourceName, m.tableSources)
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

// Register makes a datasource available by the provided @sourceName
// If Register is called twice with the same name or if source is nil, it panics.
//
//  Sources are specific schemas of type csv, elasticsearch, etc containing
//    multiple tables
func Register(sourceName string, source DataSource) {
	if source == nil {
		panic("qlbridge/datasource: Register driver is nil")
	}
	sourceName = strings.ToLower(sourceName)
	u.Debugf("global source register datasource: %v %T", sourceName, source)
	//u.LogTracef(u.WARN, "adding source %T to registry", source)
	sourceMu.Lock()
	defer sourceMu.Unlock()
	if _, dup := sources.sources[sourceName]; dup {
		panic("qlbridge/datasource: Register called twice for datasource " + sourceName)
	}
	sources.sources[sourceName] = source
}
