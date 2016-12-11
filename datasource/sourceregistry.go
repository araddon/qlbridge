// Datasource package contains database/source type related.  A few datasources
// are implemented here (test, csv).  This package also includes
// schema base services (datasource registry).
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
	registryMu sync.RWMutex
	// registry for data sources
	registry = newRegistry()

	// If disableRecover=true, we will not capture/suppress panics
	// Test only feature hopefully
	DisableRecover bool
)

// Register makes a datasource available by the provided @sourceName
// If Register is called twice with the same name or if source is nil, it panics.
//
//  Sources are specific schemas of type csv, elasticsearch, etc containing
//    multiple tables
func Register(sourceName string, source schema.Source) {
	registryMu.Lock()
	defer registryMu.Unlock()
	sourceName = strings.ToLower(sourceName)
	registerNeedsLock(sourceName, source)
}

func registerNeedsLock(sourceName string, source schema.Source) {
	if source == nil {
		panic("qlbridge/datasource: Register Source is nil")
	}

	if _, dupe := registry.sources[sourceName]; dupe {
		panic("qlbridge/datasource: Register called twice for source " + sourceName)
	}
	registry.sources[sourceName] = source
}

// Register makes a datasource available by the provided @sourceName
// If Register is called twice with the same name or if source is nil, it panics.
//
//  Sources are specific schemas of type csv, elasticsearch, etc containing
//    multiple tables
func RegisterSchemaSource(schema, sourceName string, source schema.Source) *schema.Schema {
	sourceName = strings.ToLower(sourceName)
	registryMu.Lock()
	defer registryMu.Unlock()
	registerNeedsLock(sourceName, source)
	s, _ := createSchema(sourceName)
	registry.schemas[sourceName] = s
	return s
}

// DataSourcesRegistry get access to the shared/global
// registry of all datasource implementations
func DataSourcesRegistry() *Registry {
	return registry
}

// Open a datasource, Global open connection function using
//  default schema registry
//
func OpenConn(sourceName, sourceConfig string) (schema.Conn, error) {
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

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type Registry struct {
	// Map of source name, each source name is name of db in a specific source
	//   such as elasticsearch, mongo, csv etc
	sources map[string]schema.Source
	schemas map[string]*schema.Schema
	// We need to be able to flatten all tables across all sources into single keyspace
	//tableSources map[string]schema.DataSource
	tables []string
}

func newRegistry() *Registry {
	return &Registry{
		sources: make(map[string]schema.Source),
		schemas: make(map[string]*schema.Schema),
		//tableSources: make(map[string]schema.DataSource),
		tables: make([]string, 0),
	}
}

// Init pre-schema load call any sources that need pre-schema init
func (m *Registry) Init() {
	registryMu.RLock()
	defer registryMu.RUnlock()

	for _, src := range m.sources {
		src.Init()
	}
}

// Get connection for given Database
//
//  @db      database name
//
func (m *Registry) Conn(db string) schema.Conn {

	//u.Debugf("Registry.Conn(db='%v') ", db)
	source := m.Get(strings.ToLower(db))
	if source != nil {
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
	return nil
}

// Get schema for given source
//
//  @schemaName =  virtual database name made up of multiple backend-sources
//
func (m *Registry) Schema(schemaName string) (*schema.Schema, bool) {

	registryMu.RLock()
	s, ok := m.schemas[schemaName]
	registryMu.RUnlock()
	if ok && s != nil {
		return s, ok
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	s, ok = createSchema(schemaName)
	if ok {
		u.Debugf("s:%p datasource register schema %q", s, schemaName)
		m.schemas[schemaName] = s
	}
	return s, ok
}

// SchemaAdd Add a new Schema
func (m *Registry) SchemaAdd(s *schema.Schema) {
	registryMu.Lock()
	defer registryMu.Unlock()
	_, ok := m.schemas[s.Name]
	if ok {
		return
	}
	m.schemas[s.Name] = s
}

// Add a new SourceSchema to a schema which will be created if it doesn't exist
func (m *Registry) SourceSchemaAdd(schemaName string, ss *schema.SchemaSource) error {

	registryMu.RLock()
	s, ok := m.schemas[schemaName]
	registryMu.RUnlock()
	if !ok {
		u.Warnf("must have schema %#v", ss)
		return fmt.Errorf("Must have schema when adding source schema %v", ss.Name)
	}
	s.AddSourceSchema(ss)
	return loadSchema(ss)
}

// Tables - Get all tables from this registry
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
func (m *Registry) DataSource(connInfo string) schema.Source {
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

// Get a Data Source, similar to Source(@connInfo)
func (m *Registry) Get(sourceName string) schema.Source {
	return m.getDepth(0, sourceName)
}
func (m *Registry) getDepth(depth int, sourceName string) schema.Source {
	source, ok := m.sources[strings.ToLower(sourceName)]
	if ok {
		return source
	}
	if depth > 0 {
		return nil
	}
	parts := strings.SplitN(sourceName, "://", 2)
	if len(parts) == 2 {
		source = m.getDepth(1, parts[0])
		if source != nil {
			return source
		}
		u.Warnf("not able to find schema %q", sourceName)
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

// Create a source schema from given named source
//  we will find Source for that name and introspect
func createSchema(sourceName string) (*schema.Schema, bool) {

	sourceName = strings.ToLower(sourceName)

	ss := schema.NewSchemaSource(sourceName, sourceName)
	//u.Debugf("ss:%p createSchema %v", ss, sourceName)

	ds := registry.Get(sourceName)
	if ds == nil {
		u.Warnf("not able to find schema %q", sourceName)
		return nil, false
	}

	ss.DS = ds
	s := schema.NewSchema(sourceName)
	s.AddSourceSchema(ss)
	loadSchema(ss)

	return s, true
}

func loadSchema(ss *schema.SchemaSource) error {

	if ss.DS == nil {
		u.Warnf("missing DataSource for %s", ss.Name)
		panic(fmt.Sprintf("Missing datasource for %q", ss.Name))
	}

	if dsConfig, getsConfig := ss.DS.(schema.SourceSetup); getsConfig {
		if err := dsConfig.Setup(ss); err != nil {
			u.Errorf("Error setuping up %v  %v", ss.Name, err)
			return err
		}
	}

	s := ss.Schema()
	infoSchema := s.InfoSchema
	var infoSchemaSource *schema.SchemaSource
	var err error

	if infoSchema == nil {

		infoSchema = schema.NewSchema("schema")
		infoSchemaSource = schema.NewSchemaSource("schema", "schema")

		schemaDb := NewSchemaDb(s)
		infoSchemaSource.DS = schemaDb
		schemaDb.is = infoSchema
		//u.Debugf("schema:%p ss:%p loadSystemSchema: NEW infoschema:%p  s:%s ss:%s", s, ss, infoSchema, s.Name, ss.Name)

		infoSchemaSource.AddTableName("tables")
		infoSchema.InfoSchema = infoSchema
		infoSchema.AddSourceSchema(infoSchemaSource)
	} else {
		infoSchemaSource, err = infoSchema.Source("schema")
	}

	if err != nil {
		u.Errorf("could not find schema")
		return err
	}

	// For each table in source schema
	for _, tableName := range ss.Tables() {
		//u.Debugf("adding table: %q to infoSchema %p", tableName, infoSchema)
		_, err := ss.Table(tableName)
		if err != nil {
			//u.Warnf("Missing table?? %q", tableName)
			continue
		}
		infoSchemaSource.AddTableName(tableName)
	}

	s.InfoSchema = infoSchema

	s.RefreshSchema()

	//u.Debugf("s:%p ss:%p infoschema:%p  name:%s", s, ss, infoSchema, s.Name)

	return nil
}
