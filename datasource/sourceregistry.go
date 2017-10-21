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

// OpenConn a schema-source Connection, Global open connection function using
// default schema registry.
func OpenConn(schemaName, table string) (schema.Conn, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	schema, ok := registry.schemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("unknown schema %q", schemaName)
	}
	return schema.OpenConn(table)
}

// RegisterSourceType makes a datasource type available by the provided @sourceType
// If Register is called twice with the same name or if source is nil, it panics.
//
// Sources are specific schemas of type csv, elasticsearch, etc containing
// multiple tables.
func RegisterSourceType(sourceType string, source schema.Source) {
	registryMu.Lock()
	registry.addSourceType(sourceType, source)
	registryMu.Unlock()
}

// RegisterSourceAsSchema means you have a datasource, that is going to act
// as a named schema.  ie, this will not be a nested schema with sub-schemas
// and the source will not be re-useable as a source-type.
func RegisterSourceAsSchema(name string, source schema.Source) error {
	registryMu.Lock()
	defer registryMu.Unlock()
	s := schema.NewSchema(name)
	s.DS = source
	if err := registry.SchemaAdd(s); err != nil {
		return err
	}
	return discoverSchemaFromSource(s)
}

// RegisterSchema makes a named schema available by the provided @name
// If Register is called twice with the same name or if source is nil, it panics.
//
// Sources are specific schemas of type csv, elasticsearch, etc containing
// multiple tables.
func RegisterSchema(schema *schema.Schema) {
	registryMu.Lock()
	registry.SchemaAdd(schema)
	registryMu.Unlock()
}

// DataSourcesRegistry get access to the shared/global
// registry of all datasource implementations
func DataSourcesRegistry() *Registry {
	return registry
}

// Registry  is a global or namespace registry of datasources and schema
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

func (m *Registry) addSourceType(sourceType string, source schema.Source) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sourceType = strings.ToLower(sourceType)
	if source == nil {
		panic("Register Source is nil")
	}

	if _, dupe := registry.sources[sourceType]; dupe {
		panic(fmt.Sprintf("Register called twice for source %q for %T", sourceType, source))
	}
	registry.sources[sourceType] = source
}

// Init pre-schema load call any sources that need pre-schema init
func (m *Registry) Init() {
	// TODO:  this is a race, we need a lock on sources
	for _, src := range m.sources {
		src.Init()
	}
}

// Schema Get schema for given name.
func (m *Registry) Schema(schemaName string) (*schema.Schema, bool) {
	m.mu.RLock()
	s, ok := m.schemas[schemaName]
	m.mu.RUnlock()
	return s, ok

}

// SchemaAdd Add a new Schema
func (m *Registry) SchemaAdd(s *schema.Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.schemas[s.Name]
	if ok {
		return fmt.Errorf("Cannot add existing schema %q", s.Name)
	}
	if s.InfoSchema == nil {
		s.InfoSchema = schema.NewSchema("schema")
		schemaDb := NewSchemaDb(s)
		schemaDb.is = s.InfoSchema
		s.InfoSchema.DS = schemaDb
	}
	m.schemas[s.Name] = s
	return nil
}

// Schemas returns a list of schema names
func (m *Registry) Schemas() []string {
	m.mu.RLock()
	defer registryMu.RUnlock()
	schemas := make([]string, 0, len(m.schemas))
	for _, s := range m.schemas {
		schemas = append(schemas, s.Name)
	}
	return schemas
}

// GetSource Find a DataSource by SourceType
func (m *Registry) GetSource(sourceType string) (schema.Source, error) {
	return m.getDepth(0, sourceType)
}
func (m *Registry) getDepth(depth int, sourceType string) (schema.Source, error) {
	source, ok := m.sources[strings.ToLower(sourceType)]
	if ok {
		return source, nil
	}
	if depth > 0 {
		return nil, schema.ErrNotFound
	}
	parts := strings.SplitN(sourceType, "://", 2)
	if len(parts) == 2 {
		return m.getDepth(1, parts[0])
	}
	return nil, schema.ErrNotFound
}

func (m *Registry) String() string {
	sourceNames := make([]string, 0, len(m.sources))
	for source := range m.sources {
		sourceNames = append(sourceNames, source)
	}
	schemas := make([]string, 0, len(m.schemas))
	for _, sch := range m.schemas {
		schemas = append(schemas, sch.Name)
	}
	return fmt.Sprintf("{Sources: [%s] , Schemas: [%s]}", strings.Join(sourceNames, ", "), strings.Join(schemas, ", "))
}

// Create a schema from given named source
// we will find Source for that name and introspect
func discoverSchemaFromSource(s *schema.Schema) error {

	if s.DS == nil {
		return fmt.Errorf("Missing datasource for schema %q", s.Name)
	}
	if s.InfoSchema == nil {
		return fmt.Errorf("Missing InfoSchema for schema %q", s.Name)
	}
	u.Debugf("discoverSchemaFromSource(%q) SourceType: %T", s.Name, s.DS)

	if err := s.DS.Setup(s); err != nil {
		u.Errorf("Error setting up %v  %v", s.Name, err)
		return err
	}

	u.Debugf("discoverSchemaFromSource %q  tables: %v", s.Name, s.Tables())
	// For each table in source schema
	for _, tableName := range s.Tables() {
		u.Debugf("adding table: %q to infoSchema %p", tableName, s.InfoSchema)
		_, err := s.Table(tableName)
		if err != nil {
			u.Warnf("Missing table?? %q", tableName)
			continue
		}
		s.InfoSchema.AddSchemaForTable(tableName, s)
	}

	s.RefreshSchema()
	return nil
}
