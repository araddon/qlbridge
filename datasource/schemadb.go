package datasource

import (
	"database/sql/driver"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

const (
	SchemaDbSourceType = "schemadb"
)

var (
	_ = u.EMPTY

	// Different Features of this Static Data Source
	_ schema.DataSource    = (*SchemaDb)(nil)
	_ schema.SourceConn    = (*schemaConn)(nil)
	_ schema.SchemaColumns = (*schemaConn)(nil)
	_ schema.Scanner       = (*schemaConn)(nil)

	// normal tables
	defaultSchemaTables = []string{"tables"}
	defaultTableColumns = []string{"Table"}
	tableColumnMap      = map[string]int{"Table": 0}
)

type (
	// Static Schema Source, implements qlbridge DataSource to allow in memory native go data
	//   to have a Schema and implement and be operated on by Sql Operations
	SchemaDb struct {
		exit <-chan bool
		s    *schema.Schema
		is   *schema.Schema
		tbls []string
	}
	schemaConn struct {
		db     *SchemaDb
		tbl    *schema.Table
		cursor int
	}
)

func NewSchemaDb(s *schema.Schema) *SchemaDb {
	m := SchemaDb{s: s, tbls: defaultSchemaTables}
	return &m
}
func (m *SchemaDb) Close() error     { return nil }
func (m *SchemaDb) Tables() []string { return m.tbls }
func (m *SchemaDb) Table(table string) (*schema.Table, error) {
	//u.Infof("ask for table %q", table)
	switch table {
	case "tables":
		return tableForSchema(m.s, m.is)
	}
	return nil, schema.ErrNotFound
}

// Create a schemaConn specific to schema object (table, database)
func (m *SchemaDb) Open(schemaObjectName string) (schema.SourceConn, error) {
	//u.Warnf("SchemaDb.Open(%q)", schemaObjectName)
	tbl, err := m.Table(schemaObjectName)
	if err == nil && tbl != nil {
		return &schemaConn{db: m, tbl: tbl}, nil
	}
	return nil, schema.ErrNotFound
}

func (m *schemaConn) Close() error { return nil }

func (m *schemaConn) Columns() []string                               { return m.tbl.Columns() }
func (m *schemaConn) CreateIterator(filter expr.Node) schema.Iterator { return m }
func (m *schemaConn) MesgChan(filter expr.Node) <-chan schema.Message {
	iter := m.CreateIterator(filter)
	return SourceIterChannel(iter, filter, m.db.exit)
}
func (m *schemaConn) Next() schema.Message {
	if m.cursor >= len(m.db.s.Tables()) {
		return nil
	}
	u.Infof("%d Next():  %v", m.cursor, m.db.s.Tables())
	select {
	case <-m.db.exit:
		return nil
	default:
		tableName := m.db.s.Tables()[m.cursor]
		tbl, err := m.db.s.Table(tableName)
		m.cursor++
		if err != nil {
			u.Warnf("wat?  %q", tableName)
			return nil
		}
		vals := make([]driver.Value, 1)
		vals[0] = tbl.Name
		msg := NewSqlDriverMessageMap(uint64(m.cursor-1), vals, tableColumnMap)
		u.Infof("msg: %#v", msg)
		return msg
	}
}

func (m *schemaConn) Get(key driver.Value) (schema.Message, error) {
	return nil, schema.ErrNotFound
}

func tableForSchema(s, is *schema.Schema) (*schema.Table, error) {
	ss := is.SourceSchemas["schema"]
	t := schema.NewTable("tables", ss)
	t.AddField(schema.NewFieldBase("Table", value.StringType, 64, "string"))
	t.SetColumns(defaultTableColumns)
	ss.AddTable(t)
	return t, nil
}

func SystemSchemaCreate(s *schema.Schema) error {

	if s.InfoSchema != nil {
		return nil
	}

	//sourceName := strings.ToLower(s.Name) + "_schema"
	sourceName := "schema"
	ss := schema.NewSourceSchema("schema", "schema")
	u.Debugf("createSchema(%q)", s.Name)

	//u.Infof("reg p:%p ds %#v tables:%v", registry, ds, ds.Tables())
	schemaDb := NewSchemaDb(s)
	ss.DS = schemaDb
	infoSchema := schema.NewSchema(sourceName)
	ss.Schema = infoSchema
	ss.AddTableName("tables")

	infoSchema.AddSourceSchema(ss)

	s.InfoSchema = infoSchema
	schemaDb.is = infoSchema

	u.Warnf("%p created InfoSchema: %p", s, infoSchema)

	return nil
}
