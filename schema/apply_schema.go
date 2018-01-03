package schema

import (
	"fmt"
	"time"

	u "github.com/araddon/gou"
	"github.com/gogo/protobuf/proto"
)

type (
	// Applyer takes schema writes and applies them.  This is used both as a database
	// is being loaded, and schema is loaded by store as well as responsible for applying
	// schema changes such as Alters.  In distributed db's this is very, very huge part
	// of work so is a very important interface that is under flux.
	Applyer interface {
		// Init initialize the applyer with registry.
		Init(r *Registry, repl Replicator)
		// Apply a schema change (drop, new, alter)
		Apply(op Command_Operation, to *Schema, delta interface{}) error
	}

	// Replicator will take schema changes and replicate them across servers.
	Replicator func(*Command) error

	// SchemaSourceProvider is factory for creating schema storage
	SchemaSourceProvider func(s *Schema) Source

	// InMemApplyer applies schema changes in memory.  As changes to
	// schema come in (such as ALTER statements, new tables, new databases)
	// we need to apply them to the underlying schema.
	InMemApplyer struct {
		id           string
		reg          *Registry
		repl         Replicator
		schemaSource SchemaSourceProvider
	}
)

// NewApplyer new in memory applyer.  For distributed db's we would need
// a different applyer (Raft).
func NewApplyer(sp SchemaSourceProvider) Applyer {
	m := &InMemApplyer{
		schemaSource: sp,
	}
	m.id = fmt.Sprintf("%p", m)
	return m
}

// Init store the registry as part of in-mem applyer which needs it.
func (m *InMemApplyer) Init(r *Registry, repl Replicator) {
	m.reg = r
	m.repl = repl
}

func (m *InMemApplyer) schemaSetup(s *Schema) {
	// All Schemas must also have an info-schema
	if s.InfoSchema == nil {
		s.InfoSchema = NewInfoSchema("schema", s)
	}

	// The info-schema if new will need an actual store, the provider
	// will add it to the schema.
	if s.InfoSchema.DS == nil {
		m.schemaSource(s)
	}
}

// Apply we have a schema change to apply.  A schema change is
// a new table, index, or whole new schema being registered.  We provide the first
// argument which is which schema it is being applied to (ie, add table x to schema y).
func (m *InMemApplyer) Apply(op Command_Operation, s *Schema, delta interface{}) error {

	if m.repl == nil {
		//u.Debugf("replicator nil so applying in mem")
		return m.applyObject(op, s, delta)
	}

	cmd := &Command{}
	cmd.Op = op
	cmd.Origin = m.id
	cmd.Schema = s.Name
	cmd.Ts = time.Now().UnixNano()

	// Find the type of operation being updated.
	switch v := delta.(type) {
	case *Table:
		u.Debugf("%p:%s InfoSchema P:%p  adding table %q", s, s.Name, s.InfoSchema, v.Name)
		by, err := proto.Marshal(v)
		if err != nil {
			u.Errorf("%v", err)
			return err
		}
		cmd.Msg = by
		cmd.Type = "table"
	case *Schema:
		u.Debugf("%p:%s InfoSchema P:%p  adding schema %q s==v?%v tables=%v", s, s.Name, s.InfoSchema, v.Name, s == v, s.Tables())
		by, err := proto.Marshal(v)
		if err != nil {
			u.Errorf("%v", err)
			return err
		}
		cmd.Msg = by
		cmd.Type = "schema"
	default:
		//u.Errorf("invalid type %T", v)
		return fmt.Errorf("Could not find %T", v)
	}

	// Send command to replicator
	return m.repl(cmd)
}

func (m *InMemApplyer) applyObject(op Command_Operation, s *Schema, delta interface{}) error {
	switch op {
	case Command_AddUpdate:
		return m.addOrUpdate(s, delta)
	case Command_Drop:
		return m.drop(s, delta)
	}
	return fmt.Errorf("unhandled command %v", op)
}

func (m *InMemApplyer) ApplyCommand(cmd *Command) error {

	var s *Schema
	var delta interface{}

	u.Debugf("ApplyCommand(%q)", cmd.Type)
	switch cmd.Type {
	case "table":
		tbl := &Table{}
		if err := proto.Unmarshal(cmd.Msg, tbl); err != nil {
			u.Errorf("Could not read schema %+v, err=%v", cmd, err)
			return err
		}
		delta = tbl

		sch, ok := m.reg.Schema(cmd.Schema)
		if !ok {
			u.Warnf("could not find %q in reg %#v", cmd.Schema, m.reg)
			return ErrNotFound
		}
		s = sch

	case "schema":
		sch := &Schema{}
		if err := proto.Unmarshal(cmd.Msg, sch); err != nil {
			u.Errorf("Could not read schema %+v, err=%v", cmd, err)
			return err
		}
		delta = sch
		if sch.Name == cmd.Schema {
			s = sch
			u.Debugf("found same schema we are working on  %q  tables=%v", sch.Name, s.Tables())
		} else {
			s, ok := m.reg.Schema(cmd.Schema)
			if !ok {
				u.Warnf("could not find %q in reg %#v", cmd.Schema, m.reg)
				return ErrNotFound
			}
			sch = s
		}
	}

	return m.applyObject(cmd.Op, s, delta)
}

func (m *InMemApplyer) addOrUpdate(s *Schema, v interface{}) error {
	m.schemaSetup(s)
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

		u.Infof("%p:%s InfoSchema P:%p  adding schema %q s==v?%v", s, s.Name, s.InfoSchema, v.Name, s == v)
		if s == v {
			// s==v means schema has been updated
			m.reg.mu.Lock()
			_, exists := m.reg.schemas[s.Name]
			if !exists {
				m.reg.schemas[s.Name] = s
				m.reg.schemaNames = append(m.reg.schemaNames, s.Name)
			}
			m.reg.mu.Unlock()

			//u.WarnT(20)
			//s.Discovery()
			u.Infof("add schema1 %v", s.Tables())
		} else {
			// since s != v then this is a child schema
			s.addChildSchema(v)
			//u.WarnT(20)
			//s.Discovery()
			u.Infof("add schema2 %v", s.Tables())
		}
		if s.Name != "schema" {
			s.InfoSchema.refreshSchemaUnlocked()
		}
	default:
		//u.Errorf("invalid type %T", v)
		return fmt.Errorf("Could not find %T", v)
	}

	return nil
}

// Drop we have a schema change to apply.
func (m *InMemApplyer) drop(s *Schema, v interface{}) error {

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
		//u.Errorf("invalid type %T", v)
		return fmt.Errorf("Could not find %T", v)
	}

	return nil
}
