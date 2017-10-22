package schema

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
)

var (
	// the global data sources registry mutex
	registryMu sync.RWMutex
	// default registry for schema, datasources
	registry *Registry

	// DisableRecover If true, we will not capture/suppress panics.
	// Test only feature hopefully
	DisableRecover bool

	// DefaultSchemaStoreProvider The default schema store provider
	//DefaultSchemaStoreProvider SchemaStoreProvider
)

type (
	// Registry  is a global or namespace registry of datasources and schema
	Registry struct {
		applyer Applyer
		//schemaStoreProvider SchemaStoreProvider
		// Map of source name, each source name is name of db-TYPE
		// such as elasticsearch, mongo, csv etc
		sources     map[string]Source
		schemas     map[string]*Schema
		schemaNames []string
		mu          sync.RWMutex
	}
)

// CreateDefaultRegistry create the default registry.
func CreateDefaultRegistry(applyer Applyer) {
	registry = NewRegistry(applyer)
}

// OpenConn a schema-source Connection, Global open connection function using
// default schema registry.
func OpenConn(schemaName, table string) (Conn, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	schema, ok := registry.schemas[strings.ToLower(schemaName)]
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
func RegisterSourceType(sourceType string, source Source) {
	registryMu.Lock()
	registry.addSourceType(sourceType, source)
	registryMu.Unlock()
}

// RegisterSourceAsSchema means you have a datasource, that is going to act
// as a named schema.  ie, this will not be a nested schema with sub-schemas
// and the source will not be re-useable as a source-type.
func RegisterSourceAsSchema(name string, source Source) error {

	// Since registry is a global, lets first lock that.
	registryMu.Lock()
	defer registryMu.Unlock()

	s := NewSchema(name)
	s.DS = source
	source.Init()
	source.Setup(s)
	if err := registry.SchemaAdd(s); err != nil {
		return err
	}
	return discoverSchemaFromSource(s, registry.applyer)
}

// RegisterSchema makes a named schema available by the provided @name
// If Register is called twice with the same name or if source is nil, it panics.
//
// Sources are specific schemas of type csv, elasticsearch, etc containing
// multiple tables.
func RegisterSchema(schema *Schema) {
	registryMu.Lock()
	registry.SchemaAdd(schema)
	registryMu.Unlock()
}

// DefaultRegistry get access to the shared/global
// registry of all datasource implementations
func DefaultRegistry() *Registry {
	return registry
}

// NewRegistry create schema registry.
func NewRegistry(applyer Applyer) *Registry {
	return &Registry{
		applyer:     applyer,
		sources:     make(map[string]Source),
		schemas:     make(map[string]*Schema),
		schemaNames: make([]string, 0),
	}
}

func (m *Registry) addSourceType(sourceType string, source Source) {
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

// RefreshSchema means reload the schema from underlying store.  Possibly
// requires introspection.
func (m *Registry) RefreshSchema(name string) error {
	m.mu.RLock()
	s, ok := m.schemas[name]
	m.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}

	//s.refreshSchemaUnlocked()

	return m.applyer.AddOrUpdateOnSchema(s, s)
}

// Init pre-schema load call any sources that need pre-schema init
func (m *Registry) Init() {
	// TODO:  this is a race, we need a lock on sources
	for _, src := range m.sources {
		src.Init()
	}
}

// Schema Get schema for given name.
func (m *Registry) Schema(schemaName string) (*Schema, bool) {
	m.mu.RLock()
	s, ok := m.schemas[schemaName]
	m.mu.RUnlock()
	return s, ok
}

// SchemaAdd Add a new Schema
func (m *Registry) SchemaAdd(s *Schema) error {
	s.Name = strings.ToLower(s.Name)

	m.mu.Lock()
	_, ok := m.schemas[s.Name]

	if ok {
		m.mu.Unlock()
		return fmt.Errorf("Cannot add duplicate schema %q", s.Name)
	}
	m.schemas[s.Name] = s
	m.schemaNames = append(m.schemaNames, s.Name)
	m.mu.Unlock()

	if s.InfoSchema == nil {
		s.InfoSchema = NewSchema("schema")
		m.applyer.AddOrUpdateOnSchema(s, s)
	}
	return nil
}

// SchemaAddChild Add a new Child Schema
func (m *Registry) SchemaAddChild(name string, child *Schema) error {
	name = strings.ToLower(name)
	m.mu.RLock()
	parent, ok := m.schemas[name]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("Cannot find schema %q to add child", name)
	}
	m.applyer.AddOrUpdateOnSchema(parent, child)
	return nil
}

// Schemas returns a list of schema names
func (m *Registry) Schemas() []string {
	return m.schemaNames
}

// GetSource Find a DataSource by SourceType
func (m *Registry) GetSource(sourceType string) (Source, error) {
	return m.getDepth(0, sourceType)
}
func (m *Registry) getDepth(depth int, sourceType string) (Source, error) {
	source, ok := m.sources[strings.ToLower(sourceType)]
	if ok {
		return source, nil
	}
	if depth > 0 {
		return nil, ErrNotFound
	}
	parts := strings.SplitN(sourceType, "://", 2)
	if len(parts) == 2 {
		return m.getDepth(1, parts[0])
	}
	return nil, ErrNotFound
}

// String describe contents of registry.
func (m *Registry) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
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
func discoverSchemaFromSource(s *Schema, applyer Applyer) error {

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
		tbl, err := s.Table(tableName)
		if err != nil || tbl == nil {
			u.Warnf("Missing table?? %q", tableName)
			continue
		}
		applyer.AddOrUpdateOnSchema(s, tbl)
	}

	return nil
}
