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
		exit     <-chan bool
		s        *schema.Schema
		is       *schema.Schema
		tbls     []string
		tableMap map[string]*schema.Table
	}
	schemaConn struct {
		db     *SchemaDb
		tbl    *schema.Table
		cursor int
		rows   [][]driver.Value
	}
)

func NewSchemaDb(s *schema.Schema) *SchemaDb {
	m := SchemaDb{
		s:        s,
		tbls:     defaultSchemaTables,
		tableMap: make(map[string]*schema.Table),
	}
	return &m
}
func (m *SchemaDb) Close() error     { return nil }
func (m *SchemaDb) Tables() []string { return m.tbls }
func (m *SchemaDb) Table(table string) (*schema.Table, error) {

	switch table {
	case "tables":
		return m.tableForTables()
	case "databases":
		return m.tableForDatabases()
	default:
		//u.Warnf("found schema table %q", table)
		return m.tableForTable(table)
	}
	return nil, schema.ErrNotFound
}

// Create a schemaConn specific to schema object (table, database)
func (m *SchemaDb) Open(schemaObjectName string) (schema.SourceConn, error) {
	//u.Warnf("SchemaDb.Open(%q)", schemaObjectName)
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
		//u.Infof("msg: %#v", msg)
		m.cursor++
		return msg
	}

}

func (m *schemaConn) Get(key driver.Value) (schema.Message, error) {
	return nil, schema.ErrNotFound
}

func (m *SchemaDb) inspect(table string) {
	src, err := m.s.Open(table)
	if err != nil {
		return
	}
	scanner, hasScanner := src.(schema.Scanner)
	if hasScanner {
		iter := scanner.CreateIterator(nil)
		IntrospectSchema(m.s, table, iter)
	}
}

func (m *SchemaDb) tableForTable(table string) (*schema.Table, error) {

	ss := m.is.SourceSchemas["schema"]
	tbl, hasTable := m.tableMap[table]
	//u.Debugf("creating schema table for %q", table)
	if hasTable {
		//u.Infof("found existing table %q", table)
		return tbl, nil
	}
	srcTbl, err := m.s.Table(table)
	if err != nil {
		u.Errorf("wtf? %v", err)
	}
	if len(srcTbl.Columns()) > 0 && len(srcTbl.Fields) == 0 {
		// I really don't like where this is, needs to be in schema somewhere
		m.inspect(table)
	}
	//u.Infof("found srcTable %v fields?%v", srcTbl.Columns(), len(srcTbl.Fields))
	t := schema.NewTable(table, ss)
	t.AddField(schema.NewFieldBase("Field", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Type", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Collation", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Null", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Key", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Default", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Extra", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Privileges", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Comment", value.StringType, 64, "string"))
	t.SetColumns(schema.DescribeFullCols)
	rows := srcTbl.AsRows()
	//u.Debugf("found rows: %v", rows)
	t.SetRows(rows)

	ss.AddTable(t)
	m.tableMap[table] = t
	return t, nil
}

func (m *SchemaDb) tableForTables() (*schema.Table, error) {
	// This table doesn't belong in schema
	ss := m.is.SourceSchemas["schema"]

	t := schema.NewTable("tables", ss)
	t.AddField(schema.NewFieldBase("Table", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Table_type", value.StringType, 64, "string"))
	t.SetColumns(schema.ShowTableColumns)
	ss.AddTable(t)
	rows := make([][]driver.Value, len(m.s.Tables()))
	for i, tableName := range m.s.Tables() {
		rows[i] = []driver.Value{tableName, "BASE TABLE"}
	}
	t.SetRows(rows)
	return t, nil
}

func (m *SchemaDb) tableForDatabases() (*schema.Table, error) {
	ss := m.is.SourceSchemas["schema"]

	t := schema.NewTable("databases", ss)
	t.AddField(schema.NewFieldBase("Database", value.StringType, 64, "string"))
	t.SetColumns(schema.ShowDatabasesColumns)
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
	for _, tableName := range s.Tables() {
		u.Debugf("adding table: %q to infoSchema %p", tableName, infoSchema)
		ss.AddTableName(tableName)
	}

	infoSchema.AddSourceSchema(ss)

	s.InfoSchema = infoSchema
	schemaDb.is = infoSchema

	u.Warnf("%p created InfoSchema: %p", s, infoSchema)

	return nil
}
