package schema

import (
	"fmt"

	u "github.com/araddon/gou"
)

type (
	// Applyer takes schema writes and applies them.  This is used both as a database
	// is being loaded, and schema is loaded by store as well as responsible for applying
	// schema changes such as Alters.  In distributed db's this is very, very huge part
	// of work so is a very important interface that is under flux.
	Applyer interface {
		// Init initialize the applyer with registry.
		Init(r *Registry)
		// AddOrUpdateOnSchema Add or Update object (Table, Index)
		AddOrUpdateOnSchema(s *Schema, obj interface{}) error
		// Drop an object from schema
		Drop(s *Schema, obj interface{}) error
	}

	// SchemaSourceProvider is factory for creating schema storage
	SchemaSourceProvider func(s *Schema) Source

	// InMemApplyer applies schema changes in memory.  As changes to
	// schema come in (such as ALTER statements, new tables, new databases)
	// we need to apply them to the underlying schema.
	InMemApplyer struct {
		reg          *Registry
		schemaSource SchemaSourceProvider
	}
)

// NewApplyer new in memory applyer.  For distributed db's we would need
// a different applyer (Raft).
func NewApplyer(sp SchemaSourceProvider) Applyer {
	return &InMemApplyer{
		schemaSource: sp,
	}
}

// Init store the registry as part of in-mem applyer which needs it.
func (m *InMemApplyer) Init(r *Registry) {
	m.reg = r
}

// AddOrUpdateOnSchema we have a schema change to apply.  A schema change is
// a new table, index, or whole new schema being registered.  We provide the first
// argument which is which schema it is being applied to (ie, add table x to schema y).
func (m *InMemApplyer) AddOrUpdateOnSchema(s *Schema, v interface{}) error {

	// All Schemas must also have an info-schema
	if s.InfoSchema == nil {
		s.InfoSchema = NewInfoSchema("schema", s)
	}

	// The info-schema if new will need an actual store, the provider
	// will add it to the schema.
	if s.InfoSchema.DS == nil {
		m.schemaSource(s)
	}

	// Find the type of operation being updated.
	switch v := v.(type) {
	case *Table:
		u.Debugf("%p:%s InfoSchema P:%p  adding table %q", s, s.Name, s.InfoSchema, v.Name)
		s.InfoSchema.DS.Init() // Wipe out cache, it is invalid
		s.mu.Lock()
		s.addTable(v)
		s.mu.Unlock()
		s.InfoSchema.refreshSchemaUnlocked()
	case *Schema:

		u.Debugf("%p:%s InfoSchema P:%p  adding schema %q s==v?%v", s, s.Name, s.InfoSchema, v.Name, s == v)
		if s == v {
			// s==v means schema has been updated
			m.reg.mu.Lock()
			_, exists := m.reg.schemas[s.Name]
			if !exists {
				m.reg.schemas[s.Name] = s
				m.reg.schemaNames = append(m.reg.schemaNames, s.Name)
			}
			m.reg.mu.Unlock()

			s.mu.Lock()
			s.refreshSchemaUnlocked()
			s.mu.Unlock()
		} else {
			// since s != v then this is a child schema
			s.addChildSchema(v)
			s.mu.Lock()
			s.refreshSchemaUnlocked()
			s.mu.Unlock()
		}
		if s.Name != "schema" {
			s.InfoSchema.refreshSchemaUnlocked()
		}
	default:
		u.Errorf("invalid type %T", v)
		return fmt.Errorf("Could not find %T", v)
	}

	return nil
}

// Drop we have a schema change to apply.
func (m *InMemApplyer) Drop(s *Schema, v interface{}) error {

	// Find the type of operation being updated.
	switch v := v.(type) {
	case *Table:
		u.Debugf("%p:%s InfoSchema P:%p  dropping table %q from %v", s, s.Name, s.InfoSchema, v.Name, s.Tables())
		// s==v means schema is being dropped
		m.reg.mu.Lock()
		s.mu.Lock()
		s.dropTable(v)
		m.reg.schemas[s.Name] = s
		s.refreshSchemaUnlocked()
		s.mu.Unlock()
		m.reg.mu.Unlock()
	case *Schema:

		u.Debugf("%p:%s InfoSchema P:%p  dropping schema %q s==v?%v", s, s.Name, s.InfoSchema, v.Name, s == v)
		// s==v means schema is being dropped
		m.reg.mu.Lock()
		s.mu.Lock()

		delete(m.reg.schemas, s.Name)
		names := make([]string, 0, len(m.reg.schemaNames))
		for _, n := range m.reg.schemaNames {
			if s.Name != n {
				names = append(names, n)
			}
		}
		m.reg.schemaNames = names

		s.refreshSchemaUnlocked()
		s.mu.Unlock()
		m.reg.mu.Unlock()

	default:
		u.Errorf("invalid type %T", v)
		return fmt.Errorf("Could not find %T", v)
	}

	return nil
}
