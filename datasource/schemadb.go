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
	defaultSchemaTables = []string{"tables", "databases", "columns"}
	tableColumns        = []string{"Table", "Table_Type"}
	databasesColumns    = []string{"Database"}
	columnColumns       = []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	tableColumnMap      = map[string]int{"Table": 0}
	columnsColumnMap    = map[string]int{"Field": 0, "Type": 1, "Null": 2, "Key": 3, "Default": 4, "Extra": 5}
	/*
		mysql> show databases;
		+--------------------+
		| Database           |
		+--------------------+
		| information_schema |
		| mysql              |
		| performance_schema |
		+--------------------+
		3 rows in set (0.00 sec)

	*/
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
		rows   [][]driver.Value
	}
)

func NewSchemaDb(s *schema.Schema) *SchemaDb {
	m := SchemaDb{s: s, tbls: defaultSchemaTables}
	return &m
}
func (m *SchemaDb) Close() error     { return nil }
func (m *SchemaDb) Tables() []string { return m.tbls }
func (m *SchemaDb) Table(table string) (*schema.Table, error) {

	switch table {
	case "tables":
		return tableForSchema(m.s, m.is)
	case "databases":
		return databasesForSchema(m.s, m.is)
	default:
		u.Warnf("unhandled schema table %q", table)
	}
	return nil, schema.ErrNotFound
}

// Create a schemaConn specific to schema object (table, database)
func (m *SchemaDb) Open(schemaObjectName string) (schema.SourceConn, error) {
	u.Warnf("SchemaDb.Open(%q)", schemaObjectName)
	tbl, err := m.Table(schemaObjectName)
	if err == nil && tbl != nil {
		return &schemaConn{db: m, tbl: tbl, rows: tbl.AsRows()}, nil
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
	if m.cursor >= len(m.rows) {
		return nil
	}

	select {
	case <-m.db.exit:
		return nil
	default:
		msg := NewSqlDriverMessageMap(uint64(m.cursor-1), m.rows[m.cursor], m.tbl.FieldNamesPositions())
		u.Infof("msg: %#v", msg)
		m.cursor++
		return msg
	}

}

func (m *schemaConn) Get(key driver.Value) (schema.Message, error) {
	return nil, schema.ErrNotFound
}

func tableForSchema(s, is *schema.Schema) (*schema.Table, error) {
	// This table doesn't belong in schema
	ss := is.SourceSchemas["schema"]
	t := schema.NewTable("tables", ss)
	t.AddField(schema.NewFieldBase("Table", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Table_type", value.StringType, 64, "string"))
	t.SetColumns(tableColumns)
	ss.AddTable(t)
	rows := make([][]driver.Value, len(s.Tables()))
	for i, tableName := range s.Tables() {
		rows[i] = []driver.Value{tableName, "BASE TABLE"}
	}
	t.SetRows(rows)
	return t, nil
}

func databasesForSchema(s, is *schema.Schema) (*schema.Table, error) {
	ss := is.SourceSchemas["schema"]

	t := schema.NewTable("databases", ss)
	t.AddField(schema.NewFieldBase("Database", value.StringType, 64, "string"))
	t.SetColumns(databasesColumns)
	rows := make([][]driver.Value, 0, len(registry.schemas))
	for db, _ := range registry.schemas {
		rows = append(rows, []driver.Value{db})
	}
	t.SetRows(rows)
	ss.AddTable(t)
	return t, nil
}

// We are going to Create an 'information_schema' for given schema
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
	ss.AddTableName("table")

	infoSchema.AddSourceSchema(ss)

	s.InfoSchema = infoSchema
	schemaDb.is = infoSchema

	u.Warnf("%p created InfoSchema: %p", s, infoSchema)

	return nil
}
