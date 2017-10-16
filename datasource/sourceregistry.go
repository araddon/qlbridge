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
// Sources are specific schemas of type csv, elasticsearch, etc containing
// multiple tables.
func Register(sourceName string, source schema.Source) {
	registryMu.Lock()
	defer registryMu.Unlock()
	sourceName = strings.ToLower(sourceName)
	registerNeedsLock(sourceName, source)
}

func registerNeedsLock(sourceName string, source schema.Source) {
	if source == nil {
		panic("Register Source is nil")
	}

	if _, dupe := registry.sources[sourceName]; dupe {
		panic(fmt.Sprintf("Register called twice for source %q for %T", sourceName, source))
	}
	registry.sources[sourceName] = source
}

// RegisterSchemaSource makes a datasource available by the provided @sourceName
// If Register is called twice with the same name or if source is nil, it panics.
//
// Sources are specific schemas of type csv, elasticsearch, etc containing
// multiple tables
func RegisterSchemaSource(schema, sourceName string, source schema.Source) *schema.Schema {
	sourceName = strings.ToLower(sourceName)
	registryMu.Lock()
	defer registryMu.Unlock()
	registerNeedsLock(sourceName, source)
	s, _ := createSchema(source, sourceName)
	registry.schemas[schema] = s
	return s
}

// DataSourcesRegistry get access to the shared/global
// registry of all datasource implementations
func DataSourcesRegistry() *Registry {
	return registry
}

// Open a datasource, Global open connection function using
// default schema registry
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
	// Map of source name, each source name is name of db-TYPE
	// such as elasticsearch, mongo, csv etc
	sources map[string]schema.Source
	schemas map[string]*schema.Schema
	mu      sync.RWMutex
}

func newRegistry() *Registry {
	return &Registry{
		sources: make(map[string]schema.Source),
		schemas: make(map[string]*schema.Schema),
	}
}

// Init pre-schema load call any sources that need pre-schema init
func (m *Registry) Init() {
	//registryMu.RLock()
	//defer registryMu.RUnlock()
	// TODO:  this is a race, we need a lock on sources
	for _, src := range m.sources {
		src.Init()
	}
}

// Schema Get schema for given name
// @schemaName =  virtual database name made up of multiple backend-sources
func (m *Registry) Schema(schemaName string) (*schema.Schema, bool) {

	registryMu.RLock()
	s, ok := m.schemas[schemaName]
	registryMu.RUnlock()
	if ok && s != nil {
		return s, ok
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	ds := m.getDepth(0, schemaName)
	if ds == nil {
		u.Warnf("Could not get %q source", schemaName)
		return nil, false
	}
	s, ok = createSchema(ds, schemaName)
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

// SourceSchemaAdd Add a new SourceSchema to a schema which will be created if it doesn't exist
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

// Schemas:  returns a list of schemas
func (m *Registry) Schemas() []string {

	registryMu.RLock()
	defer registryMu.RUnlock()
	schemas := make([]string, 0, len(m.schemas))
	for _, s := range m.schemas {
		schemas = append(schemas, s.Name)
	}
	return schemas
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
	for source := range m.sources {
		sourceNames = append(sourceNames, source)
	}
	return fmt.Sprintf("{Sources: [%s] }", strings.Join(sourceNames, ", "))
}

// Create a source schema from given named source
// we will find Source for that name and introspect
func createSchema(ds schema.Source, sourceName string) (*schema.Schema, bool) {

	sourceName = strings.ToLower(sourceName)

	ss := schema.NewSchemaSource(sourceName, sourceName)
	u.Debugf("ss:%p createSchema %v", ss, sourceName)
	ss.DS = ds
	s := schema.NewSchema(sourceName)
	s.AddSourceSchema(ss)
	if err := loadSchema(ss); err != nil {
		u.Errorf("Could not load schema %v", err)
		return nil, false
	}

	return s, true
}

func loadSchema(ss *schema.SchemaSource) error {

	if ss.DS == nil {
		u.Warnf("missing DataSource for %s", ss.Name)
		panic(fmt.Sprintf("Missing datasource for %q", ss.Name))
	}

	if err := ss.DS.Setup(ss); err != nil {
		u.Errorf("Error setuping up %v  %v", ss.Name, err)
		return err
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
		u.Debugf("adding table: %q to infoSchema %p", tableName, infoSchema)
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
