package datasource

import (
	"bytes"
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

func init() {
	//u.SetupLogging("debug")
	//u.SetColorOutput()
}

const (
	SchemaDbSourceType = "schemadb"
)

var (
	_ = u.EMPTY

	// Different Features of this Static Schema Data Source
	_ schema.Source      = (*SchemaDb)(nil)
	_ schema.Conn        = (*SchemaSource)(nil)
	_ schema.ConnColumns = (*SchemaSource)(nil)
	_ schema.ConnScanner = (*SchemaSource)(nil)

	// normal tables
	defaultSchemaTables = []string{"tables", "databases", "columns", "global_variables", "session_variables",
		"functions", "procedures", "engines", "status", "indexes"}
	DialectWriterCols = []string{"mysql"}
	DialectWriters    = []schema.DialectWriter{&mysqlWriter{}}
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
	SchemaSource struct {
		db      *SchemaDb
		tbl     *schema.Table
		ctx     *plan.Context
		session bool
		cursor  int
		rows    [][]driver.Value
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
func (m *SchemaDb) Init()            {}
func (m *SchemaDb) Close() error     { return nil }
func (m *SchemaDb) Tables() []string { return m.tbls }
func (m *SchemaDb) Table(table string) (*schema.Table, error) {

	//u.Debugf("Table(%q)", table)
	switch table {
	case "tables":
		return m.tableForTables()
	case "databases":
		return m.tableForDatabases()
	case "session_variables", "global_variables":
		return m.tableForVariables(table)
	case "procedures", "functions":
		return m.tableForProcedures(table)
	case "engines":
		return m.tableForEngines()
	case "indexes", "keys":
		return m.tableForIndexes()
	default:
		//u.Debugf("Table(%q)", table)
		return m.tableForTable(table)
	}
	return nil, schema.ErrNotFound
}

// Create a SchemaSource specific to schema object (table, database)
func (m *SchemaDb) Open(schemaObjectName string) (schema.Conn, error) {

	tbl, err := m.Table(schemaObjectName)
	if err == nil && tbl != nil {

		switch schemaObjectName {
		case "session_variables", "global_variables":
			return &SchemaSource{db: m, tbl: tbl, session: true}, nil
		case "engines", "procedures", "functions", "indexes":
			return &SchemaSource{db: m, tbl: tbl, rows: nil}, nil
		default:
			return &SchemaSource{db: m, tbl: tbl, rows: tbl.AsRows()}, nil
		}

	}
	return nil, schema.ErrNotFound
}

func (m *SchemaSource) SetContext(ctx *plan.Context) {
	m.ctx = ctx
	if m.session {
		m.rows = RowsForSession(ctx)
	}
}

func (m *SchemaSource) Close() error                  { return nil }
func (m *SchemaSource) SetRows(rows [][]driver.Value) { m.rows = rows }
func (m *SchemaSource) Columns() []string             { return m.tbl.Columns() }
func (m *SchemaSource) Next() schema.Message {
	if m.cursor >= len(m.rows) {
		return nil
	}

	select {
	case <-m.db.exit:
		return nil
	default:
		msg := NewSqlDriverMessageMap(uint64(m.cursor-1), m.rows[m.cursor], m.tbl.FieldNamesPositions())
		//u.Debugf("msg: %#v", msg)
		m.cursor++
		return msg
	}
}

func (m *SchemaSource) Get(key driver.Value) (schema.Message, error) {
	return nil, schema.ErrNotFound
}

func (m *SchemaDb) inspect(table string) {
	src, err := m.s.Open(table)
	if err != nil {
		return
	}
	scanner, hasScanner := src.(schema.ConnScanner)
	if hasScanner {
		IntrospectSchema(m.s, table, scanner)
	}
}

func (m *SchemaDb) tableForTable(table string) (*schema.Table, error) {

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}

	tbl, hasTable := m.tableMap[table]
	//u.Debugf("s:%p infoschema:%p creating schema table for %q", m.s, m.is, table)
	if hasTable {
		//u.Infof("found existing table %q", table)
		return tbl, nil
	}
	srcTbl, err := m.s.Table(table)
	if err != nil {
		u.Errorf("no table? err=%v for=%s", err, table)
		return nil, err
	}
	if srcTbl == nil {
		return nil, schema.ErrNotFound
	}
	if len(srcTbl.Columns()) > 0 && len(srcTbl.Fields) == 0 {
		// I really don't like where/how this gets called
		//    needs to be in schema somewhere?
		m.inspect(table)
	} else {
		//u.Warnf("NOT INSPECTING")
	}
	//u.Infof("found srcTable %v fields?%v", srcTbl.Columns(), len(srcTbl.Fields))
	t := schema.NewTable(table)
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

func (m *SchemaDb) tableForProcedures(table string) (*schema.Table, error) {

	//table := "procedures"  // procedures, functions

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}
	tbl, hasTable := m.tableMap[table]

	if hasTable {
		u.Infof("found existing table %q", table)
		return tbl, nil
	}

	u.Debugf("s:%p infoschema:%p creating schema table for %q", m.s, m.is, table)

	//  SELECT Db, Name, Type, Definer, Modified, Created, Security_type, Comment,
	//     character_set_client, `collation_connection`, `Database Collation` from `context`.`procedures`;")

	t := schema.NewTable(table)
	t.AddField(schema.NewFieldBase("Db", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Name", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Type", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Definer", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Modified", value.TimeType, 8, "datetime"))
	t.AddField(schema.NewFieldBase("Created", value.TimeType, 8, "datetime"))
	t.AddField(schema.NewFieldBase("Security_type", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Comment", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("character_set_client", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("collation_connection", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Database Collation", value.StringType, 64, "string"))
	t.SetColumns(schema.ProdedureFullCols)
	ss.AddTable(t)
	m.tableMap[table] = t
	return t, nil
}

func (m *SchemaDb) tableForEngines() (*schema.Table, error) {

	table := "engines"

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}

	tbl, hasTable := m.tableMap[table]
	//u.Debugf("s:%p infoschema:%p creating schema table for %q", m.s, m.is, table)
	if hasTable {
		//u.Infof("found existing table %q", table)
		return tbl, nil
	}
	// Engine, Support, Comment, Transactions, XA, Savepoints

	t := schema.NewTable(table)
	t.AddField(schema.NewFieldBase("Engine", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Support", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Comment", value.StringType, 255, "string"))
	t.AddField(schema.NewFieldBase("Transactions", value.BoolType, 1, "tinyint"))
	t.AddField(schema.NewFieldBase("XA", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Savepoints", value.BoolType, 1, "tinyint"))
	t.SetColumns(schema.EngineFullCols)
	ss.AddTable(t)
	m.tableMap[table] = t
	return t, nil
}

func (m *SchemaDb) tableForVariables(table string) (*schema.Table, error) {

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}

	t := schema.NewTable(table)
	t.AddField(schema.NewFieldBase("Variable_name", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Value", value.StringType, 64, "string"))
	t.SetColumns(schema.ShowVariablesColumns)
	ss.AddTable(t)
	return t, nil
}

func (m *SchemaDb) tableForTables() (*schema.Table, error) {

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}

	//u.Debugf("schema:%p  table create infoschema:%p  ", m.s, m.is)
	t := schema.NewTable("tables")
	t.AddField(schema.NewFieldBase("Table", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Table_type", value.StringType, 64, "string"))

	cols := schema.ShowTableColumns
	for _, col := range DialectWriterCols {
		cols = append(cols, fmt.Sprintf("%s_create", col))
	}
	t.SetColumns(cols)

	ss.AddTable(t)
	rows := make([][]driver.Value, len(m.s.Tables()))
	for i, tableName := range m.s.Tables() {
		rows[i] = []driver.Value{tableName, "BASE TABLE"}
		tbl, err := m.s.Table(tableName)
		if tbl != nil && len(tbl.Columns()) > 0 && len(tbl.Fields) == 0 {
			// I really don't like where this is, needs to be in schema somewhere
			m.inspect(tbl.Name)
		} else {
			//u.Warnf("NOT INSPECTING")
		}
		for _, writer := range DialectWriters {
			if err != nil {
				rows[i] = append(rows[i], "error")
			} else {
				rows[i] = append(rows[i], writer.Table(tbl))
				//u.Debugf("%s", rows[i][len(rows[i])-1])
			}
		}

	}
	//u.Debugf("set rows: %v for tables: %v", rows, m.s.Tables())
	t.SetRows(rows)
	return t, nil
}

func (m *SchemaDb) tableForIndexes() (*schema.Table, error) {

	table := "indexes"

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}
	t, hasTable := m.tableMap[table]
	//u.Debugf("s:%p infoschema:%p creating schema table for %q", m.s, m.is, table)
	if hasTable {
		return t, nil
	}

	t = schema.NewTable(table)

	/*
		mysql> show keys from `user` from `mysql`;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| user  |          0 | PRIMARY  |            1 | Host        | A         |        NULL |     NULL | NULL   |      | BTREE      |         |               |
		| user  |          0 | PRIMARY  |            2 | User        | A         |           3 |     NULL | NULL   |      | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	t.AddField(schema.NewFieldBase("Table", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Non_unique", value.BoolType, 1, "tinyint"))
	t.AddField(schema.NewFieldBase("Key_name", value.StringType, 20, "string"))
	t.AddField(schema.NewFieldBase("Seq_in_index", value.IntType, 8, "integer"))
	t.AddField(schema.NewFieldBase("Column_name", value.StringType, 64, "string"))
	t.AddField(schema.NewFieldBase("Collation", value.StringType, 20, "string"))
	t.AddField(schema.NewFieldBase("Cardinality", value.IntType, 8, "integer"))
	t.AddField(schema.NewFieldBase("Sub_part", value.StringType, 1, "string"))
	t.AddField(schema.NewFieldBase("Packed", value.StringType, 20, "string"))
	t.AddField(schema.NewFieldBase("Null", value.StringType, 20, "string"))
	t.AddField(schema.NewFieldBase("Index_type", value.StringType, 20, "string"))
	t.AddField(schema.NewFieldBase("Comment", value.StringType, 255, "string"))
	t.AddField(schema.NewFieldBase("Index_comment", value.StringType, 255, "string"))

	t.SetColumns(schema.ShowIndexCols)
	ss.AddTable(t)
	//t.SetRows(rows)
	return t, nil
}

func (m *SchemaDb) tableForDatabases() (*schema.Table, error) {

	ss, err := m.is.SchemaSource("schema")
	if err != nil {
		return nil, err
	}

	t := schema.NewTable("databases")
	t.AddField(schema.NewFieldBase("Database", value.StringType, 64, "string"))
	t.SetColumns(schema.ShowDatabasesColumns)
	rows := make([][]driver.Value, 0, len(registry.schemas))
	for db, _ := range registry.schemas {
		rows = append(rows, []driver.Value{db})
	}
	t.SetRows(rows)
	ss.AddTable(t)
	//u.Debugf("ss:%p schemadb:%p   tbl:%#v", ss, m, t)
	return t, nil
}

type mysqlWriter struct {
}

func (m *mysqlWriter) Dialect() string {
	return "mysql"
}
func (m *mysqlWriter) FieldType(t value.ValueType) string {
	return MysqlValueString(t)
}

// Implement Dialect Specific Writers
//     ie, mysql, postgres, cassandra all have different dialects
//     so the Create statements are quite different

// Take a table and make create statement
func (m *mysqlWriter) Table(tbl *schema.Table) string {

	w := &bytes.Buffer{}
	//u.Infof("%s tbl=%p fields? %#v fields?%v", tbl.Name, tbl, tbl.FieldMap, len(tbl.Fields))
	fmt.Fprintf(w, "CREATE TABLE `%s` (", tbl.Name)
	for i, fld := range tbl.Fields {
		if i != 0 {
			w.WriteByte(',')
		}
		fmt.Fprint(w, "\n    ")
		mysqlWriteField(w, fld)
	}
	fmt.Fprint(w, "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	//tblStr := fmt.Sprintf("CREATE TABLE `%s` (\n\n);", tbl.Name, strings.Join(cols, ","))
	//return tblStr, nil
	return w.String()
}
func mysqlWriteField(w *bytes.Buffer, fld *schema.Field) {
	fmt.Fprintf(w, "`%s` ", fld.Name)
	deflen := fld.Length
	switch fld.Type {
	case value.BoolType:
		fmt.Fprint(w, "tinyint(1) DEFAULT NULL")
	case value.IntType:
		fmt.Fprint(w, "bigint DEFAULT NULL")
	case value.StringType:
		if deflen == 0 {
			deflen = 255
		}
		fmt.Fprintf(w, "varchar(%d) DEFAULT NULL", deflen)
	case value.NumberType:
		fmt.Fprint(w, "float DEFAULT NULL")
	case value.TimeType:
		fmt.Fprint(w, "datetime DEFAULT NULL")
	default:
		fmt.Fprint(w, "text DEFAULT NULL")
	}
	if len(fld.Description) > 0 {
		fmt.Fprintf(w, " COMMENT %q", fld.Description)
	}
}
func MysqlValueString(t value.ValueType) string {
	switch t {
	case value.NilType:
		return "NULL"
	case value.ErrorType:
		return "text"
	case value.UnknownType:
		return "text"
	case value.ValueInterfaceType:
		return "text"
	case value.NumberType:
		return "float"
	case value.IntType:
		return "long"
	case value.BoolType:
		return "boolean"
	case value.TimeType:
		return "datetime"
	case value.ByteSliceType:
		return "text"
	case value.StringType:
		return "varchar(255)"
	case value.StringsType:
		return "text"
	case value.MapValueType:
		return "text"
	case value.MapIntType:
		return "text"
	case value.MapStringType:
		return "text"
	case value.MapNumberType:
		return "text"
	case value.MapBoolType:
		return "text"
	case value.SliceValueType:
		return "text"
	case value.StructType:
		return "text"
	case value.JsonType:
		return "json"
	default:
		return "text"
	}
}
