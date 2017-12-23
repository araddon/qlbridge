package schema

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
)

var (
	// the global data sources registry mutex
	registryMu sync.RWMutex
	// default registry for schema, datasources
	registry *Registry

	// DisableRecover If true, we will not capture/suppress panics.
	// Test only feature hopefully
	DisableRecover bool
)

type (
	// Registry  is a global or namespace registry of datasources and schema.
	// Datasources have a "sourcetype" and define somewhat the driver.
	// Schemas are made up of one or more underlying source-types and have normal
	// schema info about tables etc.
	Registry struct {
		applyer Applyer
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
	applyer.Init(registry)
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
	defer registryMu.Unlock()
	registry.addSourceType(sourceType, source)
}

// RegisterSourceAsSchema means you have a datasource, that is going to act
// as a named schema.  ie, this will not be a nested schema with sub-schemas
// and the source will not be re-useable as a source-type.
func RegisterSourceAsSchema(name string, source Source) error {

	// Since registry is a global, lets first lock that.
	registryMu.Lock()
	defer registryMu.Unlock()

	s := NewSchemaSource(name, source)
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
func RegisterSchema(schema *Schema) error {
	registryMu.Lock()
	defer registryMu.Unlock()
	return registry.SchemaAdd(schema)
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

// SchemaDrop removes a schema
func (m *Registry) SchemaDrop(schema, name string, objectType lex.TokenType) error {
	name = strings.ToLower(name)
	switch objectType {
	case lex.TokenSchema, lex.TokenSource:
		m.mu.RLock()
		s, ok := m.schemas[name]
		m.mu.RUnlock()
		if !ok {
			return ErrNotFound
		}
		return m.applyer.Drop(s, s)
	case lex.TokenTable:
		m.mu.RLock()
		s, ok := m.schemas[schema]
		m.mu.RUnlock()
		if !ok {
			return ErrNotFound
		}
		t, _ := s.Table(name)
		if t == nil {
			return ErrNotFound
		}
		return m.applyer.Drop(s, t)
	}
	return fmt.Errorf("Object type %s not recognized to DROP", objectType)
}

// SchemaRefresh means reload the schema from underlying store.  Possibly
// requires introspection.
func (m *Registry) SchemaRefresh(name string) error {
	m.mu.RLock()
	s, ok := m.schemas[name]
	m.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}
	return m.applyer.AddOrUpdateOnSchema(s, s)
}

// Init pre-schema load call any sources that need pre-schema init
func (m *Registry) Init() {
	// TODO:  this is a race, we need a lock on sources
	for _, src := range m.sources {
		src.Init()
	}
}

// SchemaAddFromConfig means you have a Schema-Source you want to add
func (m *Registry) SchemaAddFromConfig(conf *ConfigSource) error {

	source, err := m.GetSource(conf.SourceType)
	if err != nil {
		u.Warnf("could not find source type %q  \nregistry: %s", conf.SourceType, m.String())
		return err
	}

	s := NewSchema(conf.Name)
	s.Conf = conf
	s.DS = source
	if err := s.DS.Setup(s); err != nil {
		u.Errorf("Error setuping up %+v  err=%v", conf, err)
		return err
	}

	// If we specify a parent schema to add this child schema to
	if conf.Schema != "" && conf.Schema != s.Name {
		_, ok := m.Schema(conf.Schema)
		if !ok {
			ps := NewSchema(conf.Schema)
			err := m.SchemaAdd(ps)
			if err != nil {
				u.Warnf("could not create parent schema %v", err)
				return err
			}
		}
		if err = m.SchemaAddChild(conf.Schema, s); err != nil {
			return nil
		}
		return nil
	}

	return m.SchemaAdd(s)
}

// Schema Get schema for given name.
func (m *Registry) Schema(schemaName string) (*Schema, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.schemas[schemaName]
	return s, ok
}

// SchemaAdd Add a new Schema
func (m *Registry) SchemaAdd(s *Schema) error {

	s.Name = strings.ToLower(s.Name)
	m.mu.RLock()
	_, ok := m.schemas[s.Name]
	m.mu.RUnlock()
	if ok {
		u.Warnf("Can't add duplicate schema %q", s.Name)
		return fmt.Errorf("Cannot add duplicate schema %q", s.Name)
	}

	if s.InfoSchema == nil {
		s.InfoSchema = NewInfoSchema("schema", s)
	}
	m.applyer.AddOrUpdateOnSchema(s, s)
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

	if err := s.DS.Setup(s); err != nil {
		u.Errorf("Error setting up %v  %v", s.Name, err)
		return err
	}

	// For each table in source schema
	for _, tableName := range s.Tables() {
		tbl, err := s.Table(tableName)
		if err != nil || tbl == nil {
			u.Warnf("Missing table?? %q", tableName)
			continue
		}
		applyer.AddOrUpdateOnSchema(s, tbl)
	}

	return nil
}
