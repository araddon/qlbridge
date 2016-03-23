// Datasource are individual database/source types, a few of which are
// implemented here (test, csv) and base services (datasource registry).
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

	// If disableRecover=true, we will not capture/suppress panics
	// Test only feature hopefully
	DisableRecover bool
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
	u.Debugf("global source register datasource: %v %T source:%p", sourceName, source, source)
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

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type Registry struct {
	// Map of source name, each source name is name of db in a specific source
	//   such as elasticsearch, mongo, csv etc
	sources map[string]schema.DataSource
	schemas map[string]*schema.Schema
	// We need to be able to flatten all tables across all sources into single keyspace
	//tableSources map[string]schema.DataSource
	tables []string
}

func newRegistry() *Registry {
	return &Registry{
		sources: make(map[string]schema.DataSource),
		schemas: make(map[string]*schema.Schema),
		//tableSources: make(map[string]schema.DataSource),
		tables: make([]string, 0),
	}
}

// Get connection for given Database
//
//  @db      database name
//
func (m *Registry) Conn(db string) schema.SourceConn {

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

	//u.Debugf("Registry.Schema(%q)", schemaName)
	//u.WarnT(5)
	s, ok := m.schemas[schemaName]
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

// Add a new SourceSchema to a schema which will be created if it doesn't exist
func (m *Registry) SourceSchemaAdd(ss *schema.SourceSchema) error {

	if ss.Schema == nil {
		u.Warnf("must have schema %#v", ss)
		return fmt.Errorf("Must have schema when adding source schema %v", ss.Name)
	}
	return loadSchema(ss)
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
func (m *Registry) Get(sourceName string) schema.DataSource {
	if source, ok := m.sources[strings.ToLower(sourceName)]; ok {
		return source
	}
	//u.Warnf("not found %q", sourceName)
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
//  we will find DataSource for that name and introspect
func createSchema(sourceName string) (*schema.Schema, bool) {

	sourceName = strings.ToLower(sourceName)
	ss := schema.NewSourceSchema(sourceName, sourceName)

	ds := registry.Get(sourceName)
	if ds == nil {
		parts := strings.SplitN(sourceName, "://", 2)
		//u.Infof("parts: %d   %v", len(parts), parts)
		if len(parts) == 2 {
			ds = registry.Get(parts[0])
			if ds == nil {
				//return &qlbConn{schema: s, connInfo: parts[1]}, nil
				u.Warnf("not able to find schema %q", sourceName)
				return nil, false
			}
		} else {
			//u.WarnT(7)
			u.Warnf("not able to find schema %q", sourceName)
			return nil, false
		}
	}

	u.Infof("reg p:%p source=%q  ds %#v tables:%v", registry, sourceName, ds, ds.Tables())
	ss.DS = ds
	schema := schema.NewSchema(sourceName)
	ss.Schema = schema
	u.Debugf("schema:%p ss:%p createSchema(%q) NEW ", schema, ss, sourceName)

	loadSchema(ss)

	return schema, true
}

func loadSchema(ss *schema.SourceSchema) error {

	//u.WarnT(6)
	if ss.DS == nil {
		u.Warnf("missing DataSource for %s", ss.Name)
		return fmt.Errorf("Missing datasource for %q", ss.Name)
	}
	if dsConfig, getsConfig := ss.DS.(schema.SourceSetup); getsConfig {
		if err := dsConfig.Setup(ss); err != nil {
			u.Errorf("Error setuping up %v  %v", ss.Name, err)
			return err
		}
	}

	for _, tableName := range ss.DS.Tables() {
		ss.AddTableName(tableName)
		u.Debugf("table %q", tableName)
	}

	ss.Schema.AddSourceSchema(ss)

	// Intercept and create info schema
	loadSystemSchema(ss)
	return nil
}

// We are going to Create an 'information_schema' for given schema
func loadSystemSchema(ss *schema.SourceSchema) error {

	s := ss.Schema
	if s == nil {
		return fmt.Errorf("Must have schema but was nil")
	}

	//u.WarnT(6)
	//u.Debugf("loadSystemSchema")

	infoSchema := s.InfoSchema
	var infoSourceSchema *schema.SourceSchema
	if infoSchema == nil {
		infoSourceSchema = schema.NewSourceSchema("schema", "schema")

		//u.Infof("reg p:%p ds %#v tables:%v", registry, ds, ds.Tables())
		schemaDb := NewSchemaDb(s)
		infoSourceSchema.DS = schemaDb
		infoSchema = schema.NewSchema("schema")
		schemaDb.is = infoSchema
		//u.Debugf("schema:%p ss:%p loadSystemSchema: NEW infoschema:%p  s:%s ss:%s", s, ss, infoSchema, s.Name, ss.Name)

		infoSourceSchema.Schema = infoSchema
		infoSourceSchema.AddTableName("tables")
		infoSchema.SourceSchemas["schema"] = infoSourceSchema
	} else {
		infoSourceSchema = infoSchema.SourceSchemas["schema"]
	}

	// For each table in source schema
	for _, tableName := range ss.Tables() {
		//u.Debugf("adding table: %q to infoSchema %p", tableName, infoSchema)
		_, err := ss.Table(tableName)
		if err != nil {
			u.Warnf("Missing table?? %q", tableName)
			continue
		}
		infoSourceSchema.AddTableName(tableName)
	}

	s.InfoSchema = infoSchema

	s.RefreshSchema()

	//u.Debugf("s:%p ss:%p infoschema:%p  name:%s", s, ss, infoSchema, s.Name)

	return nil
}
