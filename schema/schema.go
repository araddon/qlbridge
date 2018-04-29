// Package schema implements core Relational Algrebra schema objects such as Table,
// Schema, DataSource, Fields, Headers, Index.
package schema

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"
	"github.com/golang/protobuf/proto"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var (
	// SchemaRefreshInterval default schema Refresh Interval
	SchemaRefreshInterval = -time.Minute * 5

	// Static list of common field names for describe header on Show, Describe
	EngineFullCols       = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	ProdedureFullCols    = []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client ", "collation_connection", "Database Collation"}
	DescribeFullCols     = []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	DescribeFullColMap   = map[string]int{"Field": 0, "Type": 1, "Collation": 2, "Null": 3, "Key": 4, "Default": 5, "Extra": 6, "Privileges": 7, "Comment": 8}
	DescribeCols         = []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	DescribeColMap       = map[string]int{"Field": 0, "Type": 1, "Null": 2, "Key": 3, "Default": 4, "Extra": 5}
	ShowTableColumns     = []string{"Table", "Table_Type"}
	ShowVariablesColumns = []string{"Variable_name", "Value"}
	ShowDatabasesColumns = []string{"Database"}
	ShowTableColumnMap   = map[string]int{"Table": 0}
	ShowIndexCols        = []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Index_comment"}
	DescribeFullHeaders  = NewDescribeFullHeaders()
	DescribeHeaders      = NewDescribeHeaders()

	// Enforce interfaces
	_ SourceTableColumn = (*Table)(nil)

	// Enforce field as a message
	_ Message = (*Field)(nil)

	// InMemApplyer must implement Applyer
	_ Applyer = (*InMemApplyer)(nil)

	// Enforce proto marshalling
	_ proto.Marshaler   = (*Schema)(nil)
	_ proto.Unmarshaler = (*Schema)(nil)
	_ proto.Marshaler   = (*Table)(nil)
	_ proto.Unmarshaler = (*Table)(nil)
	_ proto.Marshaler   = (*Field)(nil)
	_ proto.Unmarshaler = (*Field)(nil)
)

const (
	// NoNulls defines if we allow nulls
	NoNulls = false
	// AllowNulls ?
	AllowNulls = true
)

type (
	// DialectWriter knows how to format the schema output specific to a dialect
	// such as postgres, mysql, bigquery all have different identity, value escape characters.
	DialectWriter interface {
		// Dialect ie "mysql", "postgres", "cassandra", "bigquery"
		Dialect() string
		Table(tbl *Table) string
		FieldType(t value.ValueType) string
	}

	// Alter interface for schema storage sources
	Alter interface {
		// DropTable drop given table
		DropTable(table string) error
	}

	// Schema defines the structure of a database Schema (set of tables, indexes, etc).
	// It is a "Virtual" Schema and may have multiple different backing sources.
	// - Multiple DataSource(s) (each may be discrete source type such as mysql, elasticsearch, etc)
	// - each schema supplies tables to the virtual table pool
	// - each table name across schemas must be unique (or aliased)
	Schema struct {
		SchemaPb
		DS           Source             // This datasource Interface
		InfoSchema   *Schema            // represent this Schema as sql schema like "information_schema"
		SchemaRef    *Schema            // IF this is infoschema, the schema it refers to
		parent       *Schema            // parent schema (optional) if nested.
		schemas      map[string]*Schema // map[schema-name]:Children Schemas
		tableSchemas map[string]*Schema // Tables to schema map for parent/child
		tableMap     map[string]*Table  // Tables and their field info, flattened from all child schemas
		tableNames   []string           // List Table names, flattened all schemas into one list
		mu           sync.RWMutex       // lock for schema mods
	}

	// Table represents traditional definition of Database Table.  It belongs to a Schema
	// and can be used to create a Datasource used to read this table.
	Table struct {
		TablePb
		Fields         []*Field          // List of Fields, in order
		FieldPositions map[string]int    // Maps name of column to ordinal position in array of []driver.Value's
		FieldMap       map[string]*Field // Map of Field-name -> Field
		Schema         *Schema           // The schema this is member of
		Source         Source            // The source
		cols           []string          // array of column names
		rows           [][]driver.Value
	}

	// Field Describes the column info, name, data type, defaults, index, null
	// - dialects (mysql, mongo, cassandra) have their own descriptors for these,
	//   so this is generic meant to be converted to Frontend at runtime
	Field struct {
		FieldPb
		row []driver.Value // memoized values of this fields descriptors for describe
	}
	// FieldData is the byte value of a "Described" field ready to write to the wire so we don't have
	// to continually re-serialize it.
	FieldData []byte

	/*
		// ConfigSchema is the config block for Schema, the data-sources
		// that make up this Virtual Schema.  Must have a name and list
		// of sources to include.
		ConfigSchema struct {
			Name    string   `json:"name"`    // Virtual Schema Name, must be unique
			Sources []string `json:"sources"` // List of sources , the names of the "Db" in source
			//ConfigNode []string `json:"-"`       // List of backend Servers
		}

		// ConfigSource are backend datasources ie : storage/database/csvfiles
		// Each represents a single source type/config.  May belong to more
		// than one schema.
		ConfigSource struct {
			Name         string            `json:"name"`            // Name
			Schema       string            `json:"schema"`          // Schema Name if different than Name, will join existing schema
			SourceType   string            `json:"type"`            // [mysql,elasticsearch,csv,etc] Name in DataSource Registry
			TablesToLoad []string          `json:"tables_to_load"`  // if non empty, only load these tables
			TableAliases map[string]string `json:"table_aliases"`   // convert underlying table names to friendly ones
			Nodes        []*ConfigNode     `json:"nodes"`           // List of nodes
			Hosts        []string          `json:"hosts"`           // List of hosts, replaces older "nodes"
			Settings     map[string]string `json:"settings"`        // Arbitrary settings specific to each source type
			Partitions   []*TablePartition `json:"partitions"`      // List of partitions per table (optional)
			PartitionCt  uint32            `json:"partition_count"` // Instead of array of per table partitions, raw partition count
		}

		// ConfigNode are Servers/Services, ie a running instance of said Source
		// - each must represent a single source type
		// - normal use is a server, describing partitions of servers
		// - may have arbitrary config info in Settings.
		ConfigNode struct {
			Name     string            `json:"name"`     // Name of this Node optional
			Source   string            `json:"source"`   // Name of source this node belongs to
			Address  string            `json:"address"`  // host/ip
			Settings map[string]string `json:"settings"` // Arbitrary settings
		}
	*/
)

// NewSchema create a new empty schema with given name.
func NewSchema(schemaName string) *Schema {
	return NewSchemaSource(schemaName, nil)
}

// NewInfoSchema create a new empty schema with given name.
func NewInfoSchema(schemaName string, s *Schema) *Schema {
	is := NewSchemaSource(schemaName, nil)
	is.InfoSchema = is
	is.SchemaRef = s
	return is
}

// NewSchemaSource create a new empty schema with given name and source.
func NewSchemaSource(schemaName string, ds Source) *Schema {
	m := &Schema{
		SchemaPb: SchemaPb{Name: strings.ToLower(schemaName)},
		DS:       ds,
	}
	m.initMaps()
	return m
}
func (m *Schema) initMaps() {
	m.schemas = make(map[string]*Schema)
	m.tableMap = make(map[string]*Table)
	m.tableSchemas = make(map[string]*Schema)
	m.tableNames = make([]string, 0)
}

// Tables gets list of all tables for this schema.
func (m *Schema) Tables() []string { return m.tableNames }

// Table gets Table definition for given table name
func (m *Schema) Table(tableIn string) (*Table, error) {

	tableName := strings.ToLower(tableIn)

	m.mu.RLock()
	defer m.mu.RUnlock()

	tbl, ok := m.tableMap[tableName]
	if ok && tbl != nil {
		return tbl, nil
	}

	// Lets see if it is   `schema`.`table` format
	ns, tableName, ok := expr.LeftRight(tableName)
	if ok {
		if m.Name != ns {
			return nil, fmt.Errorf("Could not find that table: %v", tableIn)
		}
		tbl, ok = m.tableMap[tableName]
		if ok && tbl != nil {
			return tbl, nil
		}
	}

	if m.SchemaRef != nil {
		return m.SchemaRef.Table(tableIn)
	}
	return nil, fmt.Errorf("Could not find that table: %v", tableIn)
}

// OpenConn get a connection from this schema by table name.
func (m *Schema) OpenConn(tableName string) (Conn, error) {
	tableName = strings.ToLower(tableName)
	m.mu.RLock()
	defer m.mu.RUnlock()
	sch, ok := m.tableSchemas[tableName]
	if !ok || sch == nil || sch.DS == nil {
		//u.WarnT(10)
		return nil, fmt.Errorf("Could not find a DataSource for that table %q", tableName)
	}

	conn, err := sch.DS.Open(tableName)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, fmt.Errorf("Could not establish a connection for %v", tableName)
	}
	return conn, nil
}

// Schema Find a child Schema for given schema name,
func (m *Schema) Schema(schemaName string) (*Schema, error) {
	// We always lower-case schema names
	schemaName = strings.ToLower(schemaName)
	m.mu.RLock()
	defer m.mu.RUnlock()
	child, ok := m.schemas[schemaName]
	if ok && child != nil && child.DS != nil {
		return child, nil
	}
	return nil, fmt.Errorf("Could not find a Schema by that name %q", schemaName)
}

// SchemaForTable Find a Schema for given Table
func (m *Schema) SchemaForTable(tableName string) (*Schema, error) {

	// We always lower-case table names
	tableName = strings.ToLower(tableName)

	if m.Name == "schema" {
		return m, nil
	}

	m.mu.RLock()
	ss, ok := m.tableSchemas[tableName]
	m.mu.RUnlock()
	if ok && ss != nil && ss.DS != nil {
		return ss, nil
	}

	u.Warnf("%p schema.SchemaForTable: no source!!!! schema=%q table=%q", m, m.Name, tableName)

	return nil, ErrNotFound
}

// Equal check deep equality.
func (m *Schema) Equal(s *Schema) bool {
	if m == nil && s == nil {
		u.Warnf("wtf1")
		return true
	}
	if m == nil && s != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && s == nil {
		u.Warnf("wtf3")
		return false
	}
	if m.Name != s.Name {
		u.Warnf("name %q != %q", m.Name, s.Name)
		return false
	}
	if len(m.tableNames) != len(s.tableNames) {
		return false
	}
	if len(m.tableMap) != len(s.tableMap) {
		return false
	}
	for k, mt := range m.tableMap {
		if st, ok := s.tableMap[k]; !ok || !mt.Equal(st) {
			return false
		}
	}
	return true
}

// Marshal this Schema as protobuf
func (m *Schema) Marshal() ([]byte, error) {
	m.SchemaPb.Tables = make(map[string]*TablePb, len(m.tableMap))
	//u.Debugf("tableMap: %#v", m)
	for k, t := range m.tableMap {
		m.SchemaPb.Tables[k] = &t.TablePb
		u.Infof("%p source=%T table %#v", t, t.Source, t.TablePb)
		u.Infof("%#v", t.Fields)
		u.Infof("table cols? %#v", t)
		for _, f := range t.TablePb.Fieldpbs {
			u.Debugf("%q %+v", t.Name, f)
		}
	}
	if m.Conf == nil {
		m.Conf = &ConfigSource{}
		if m.DS != nil {
			m.Conf.Type = m.DS.Type()
		}
	}
	u.Warnf("schema tables %#v", m.SchemaPb.Tables)
	return proto.Marshal(&m.SchemaPb)
}

// Unmarshal the protobuf bytes into a Schema.
func (m *Schema) Unmarshal(data []byte) error {
	//u.Infof("in Schema Unmarshal %s", string(data))
	m.initMaps()
	err := proto.Unmarshal(data, &m.SchemaPb)
	if err != nil {
		u.Errorf("%v", err)
		return err
	}
	/*
		Schema struct {
			SchemaPb
			Conf         *ConfigSource      // source configuration
			DS           Source             // This datasource Interface
			InfoSchema   *Schema            // represent this Schema as sql schema like "information_schema"
			SchemaRef    *Schema            // IF this is infoschema, the schema it refers to
			parent       *Schema            // parent schema (optional) if nested.
			schemas      map[string]*Schema // map[schema-name]:Children Schemas
			tableSchemas map[string]*Schema // Tables to schema map for parent/child
			tableMap     map[string]*Table  // Tables and their field info, flattened from all child schemas
			tableNames   []string           // List Table names, flattened all schemas into one list
			mu           sync.RWMutex       // lock for schema mods
		}
	*/

	for k, tbl := range m.SchemaPb.Tables {
		m.tableNames = append(m.tableNames, k)
		t := &Table{TablePb: *tbl}
		t.initPb()
		m.tableMap[k] = t
		u.Infof("found table %v", k)
	}

	return nil
}

// Discovery is introspect tables in sources to create schema.
func (m *Schema) Discovery() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refreshSchemaUnlocked()
}

// addChildSchema add a child schema to this one.  Schemas can be tree-in-nature
// with schema of multiple backend datasources being combined into parent Schema, but each
// child has their own unique defined schema.
func (m *Schema) addChildSchema(child *Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemas[child.Name] = child
	child.parent = m
	child.mu.RLock()
	defer child.mu.RUnlock()
	for _, tbl := range child.tableMap {
		if err := m.addTable(tbl); err != nil {
			return err
		}
	}
	return nil
}

func (m *Schema) refreshSchemaUnlocked() error {

	//u.WarnT(20)
	if m.DS != nil {
		for _, tableName := range m.DS.Tables() {
			//u.Debugf("%p:%s  DS T:%T table name %s", m, m.Name, m.DS, tableName)
			if err := m.loadTable(tableName); err != nil {
				if tableName == "columns" {
					continue
				}
				u.Errorf("Could not load table %q err=%v", tableName, err)
				return err
			}
		}
	}

	for _, ss := range m.schemas {
		//u.Infof("schema  %p:%s", ss, ss.Name)
		if err := ss.refreshSchemaUnlocked(); err != nil {
			u.Errorf("Could not load schema %q err=%v", ss.Name, err)
			return err
		}
		for tableName, tbl := range ss.tableMap {
			//u.Debugf("s:%p ss:%p add table name %s  tbl:%#v", m, ss, tableName, tbl)
			if err := m.addTable(tbl); err != nil {
				if tableName == "columns" {
					continue
				}
				u.Errorf("Could not load table %q err=%v", tableName, err)
				return err
			}
		}
	}
	return nil
}

func (m *Schema) addTable(tbl *Table) error {

	//u.Infof("table P %p add table: %v partitionct:%v conf:%+v cols:%v", tbl, tbl.Name, tbl.PartitionCt, m.Conf, tbl.Columns())

	if err := tbl.init(m); err != nil {
		u.Warnf("could not init table %v err=%v", tbl, err)
		return err
	}

	if _, exists := m.tableMap[tbl.Name]; !exists {
		m.tableNames = append(m.tableNames, tbl.Name)
		sort.Strings(m.tableNames)
	}
	m.tableMap[tbl.Name] = tbl
	m.tableSchemas[tbl.Name] = m
	return nil
}

func (m *Schema) loadTable(tableName string) error {

	//u.Infof("%p schema.%v loadTable(%q)", m, m.Name, tableName)

	if m.DS == nil {
		u.Warnf("no DS for %q", tableName)
		return nil
	}

	// Getting table from Source will ensure the table-schema is fresh/good
	tbl, err := m.DS.Table(tableName)
	if err != nil {
		//u.Warnf("could not get table %q", tableName)
		return err
	}
	if tbl == nil {
		u.Warnf("empty table %q", tableName)
		return ErrNotFound
	}
	//u.Warnf("DS T: %T  table=%q tablePB: %#v", m.DS, tbl.Name, tbl.TablePb.Fieldpbs)
	return m.addTable(tbl)
}

func (m *Schema) dropTable(tbl *Table) error {

	ts := m.tableSchemas[tbl.Name]
	if ts != nil {
		if as, ok := ts.DS.(Alter); ok {
			if err := as.DropTable(tbl.Name); err != nil {
				u.Errorf("could not drop table %v err=%v", tbl.Name, err)
				return err
			}
		}
	}

	delete(m.tableMap, tbl.Name)
	delete(m.tableSchemas, tbl.Name)
	tl := make([]string, 0, len(m.tableNames))
	for tn, _ := range m.tableMap {
		tl = append(tl, tn)
	}
	m.tableNames = tl
	sort.Strings(m.tableNames)

	if salter, ok := m.InfoSchema.DS.(Alter); ok {
		err := salter.DropTable(tbl.Name)
		if err != nil {
			u.Warnf("err %v", err)
			return err
		}
	}

	return nil
}

// NewTable create a new table for a schema.
func NewTable(table string) *Table {
	tpb := TablePb{
		Name:         strings.ToLower(table),
		NameOriginal: table,
	}
	t := &Table{
		TablePb:  tpb,
		Fields:   make([]*Field, 0),
		FieldMap: make(map[string]*Field),
	}
	return t
}
func (m *Table) init(s *Schema) error {
	m.Schema = s
	if s == nil {
		u.Warnf("No Schema for table %q?", m.Name)
		return nil
	}
	// Assign partitions
	if s.Conf != nil && s.Conf.PartitionCt > 0 {
		m.PartitionCt = uint32(s.Conf.PartitionCt)
	} else if s.Conf != nil {
		for _, pt := range s.Conf.Partitions {
			if m.Name == pt.Table && m.Partition == nil {
				m.Partition = pt
			}
		}
	}
	return nil
}

// HasField does this table have given field/column?
func (m *Table) HasField(name string) bool {
	if _, ok := m.FieldMap[name]; ok {
		return true
	}
	return false
}

// FieldsAsMessages get list of all fields as interface Message
// used in schema as sql "describe table"
func (m *Table) FieldsAsMessages() []Message {
	msgs := make([]Message, len(m.Fields))
	for i, f := range m.Fields {
		msgs[i] = f
	}
	return msgs
}

// AddField register a new field
func (m *Table) AddField(fld *Field) {
	found := false
	for i, curFld := range m.Fields {
		if curFld.Name == fld.Name {
			found = true
			m.Fields[i] = fld
			break
		}
	}
	if !found {
		fld.Position = uint64(len(m.Fields))
		m.Fields = append(m.Fields, fld)
		m.Fieldpbs = append(m.Fieldpbs, &fld.FieldPb)
	}
	m.FieldMap[fld.Name] = fld
	// Fieldpbs

}

// AddFieldType describe and register a new column
func (m *Table) AddFieldType(name string, valType value.ValueType) {
	// NewFieldBase(name string, valType value.ValueType, size int, desc string)
	// &Field{FieldPb: FieldPb{Type: uint32(valType), Name: name}}
	m.AddField(NewFieldBase(name, valType, 100, name))
}

// Column get the Underlying data type.
func (m *Table) Column(col string) (value.ValueType, bool) {
	f, ok := m.FieldMap[col]
	if ok {
		return f.ValueType(), true
	}
	f, ok = m.FieldMap[strings.ToLower(col)]
	if ok {
		return f.ValueType(), true
	}
	return value.UnknownType, false
}

// SetColumns Explicityly set column names.
func (m *Table) SetColumns(cols []string) {
	m.FieldPositions = make(map[string]int, len(cols))
	for idx, col := range cols {
		//col = strings.ToLower(col)
		m.FieldPositions[col] = idx
		cols[idx] = col
	}
	m.cols = cols
}

// SetColumnsFromFields Explicityly set column names from fields.
func (m *Table) SetColumnsFromFields() {
	m.FieldPositions = make(map[string]int, len(m.Fields))
	cols := make([]string, len(m.Fields))
	for idx, f := range m.Fields {
		col := strings.ToLower(f.Name)
		m.FieldPositions[col] = idx
		cols[idx] = col
	}
	m.cols = cols
}

// Columns list of all column names.
func (m *Table) Columns() []string { return m.cols }

// AsRows return all fields suiteable as list of values for Describe/Show statements.
func (m *Table) AsRows() [][]driver.Value {
	if len(m.rows) > 0 {
		return m.rows
	}
	m.rows = make([][]driver.Value, len(m.Fields))
	for i, f := range m.Fields {
		m.rows[i] = f.AsRow()
	}
	return m.rows
}

// SetRows set rows aka values for this table.  Used for schema/testing.
func (m *Table) SetRows(rows [][]driver.Value) {
	m.rows = rows
}

// FieldNamesPositions List of Field Names and ordinal position in Column list
func (m *Table) FieldNamesPositions() map[string]int { return m.FieldPositions }

// AddContext add key/value pairs to context (settings, metatadata).
func (m *Table) AddContext(key, value string) {
	if len(m.Context) == 0 {
		m.Context = make(map[string]string)
	}
	m.Context[key] = value
}

// Equal deep equality check for Table.
func (m *Table) Equal(t *Table) bool {
	if m == nil && t == nil {
		u.Warnf("wtf1")
		return true
	}
	if m == nil && t != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && t == nil {
		u.Warnf("wtf3")
		return false
	}
	if len(m.cols) != len(t.cols) {
		u.Warnf("wtf4")
		return false
	}
	for i, col := range m.cols {
		if t.cols[i] != col {
			u.Warnf("wtf4b")
			return false
		}
	}
	if (m.Source != nil && t.Source == nil) || (m.Source == nil && t.Source != nil) {
		if fmt.Sprintf("%T", m.Source) != fmt.Sprintf("%T", t.Source) {
			u.Warnf("wtf5 source type")
		}
		u.Warnf("wtf5")
		return false
	}
	/*
		Table struct {
			TablePb
			Fields         []*Field          // List of Fields, in order
			FieldPositions map[string]int    // Maps name of column to ordinal position in array of []driver.Value's
			FieldMap       map[string]*Field // Map of Field-name -> Field
			Schema         *Schema           // The schema this is member of
			Source         Source            // The source
			cols           []string          // array of column names
			rows           [][]driver.Value
		}
	*/
	if len(m.Fields) != len(t.Fields) {
		u.Warnf("wtf8")
		return false
	}
	for i, f := range m.Fields {
		if !f.Equal(t.Fields[i]) {
			u.Warnf("wtf8b")
			return false
		}
	}
	if len(m.FieldPositions) != len(t.FieldPositions) {
		u.Warnf("wtf9")
		return false
	}
	for k, v := range m.FieldPositions {
		if t.FieldPositions[k] != v {
			u.Warnf("wtf9b")
			return false
		}
	}
	if len(m.FieldMap) != len(t.FieldMap) {
		u.Warnf("wtf10")
		return false
	}
	if !m.TablePb.Equal(&t.TablePb) {
		return false
	}
	return true
}

// Equal deep equality check for TablePb.
func (m *TablePb) Equal(t *TablePb) bool {
	if m == nil && t == nil {
		u.Warnf("wtf1")
		return true
	}
	if m == nil && t != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && t == nil {
		u.Warnf("wtf3")
		return false
	}
	if m.Name != t.Name {
		u.Warnf("name %q != %q", m.Name, t.Name)
		return false
	}
	if m.NameOriginal != t.NameOriginal {
		u.Warnf("NameOriginal %q != %q", m.NameOriginal, t.NameOriginal)
		return false
	}
	if m.Parent != t.Parent {
		u.Warnf("Parent %q != %q", m.Parent, t.Parent)
		return false
	}
	if m.Charset != t.Charset {
		u.Warnf("Charset %q != %q", m.Charset, t.Charset)
		return false
	}
	if !m.Partition.Equal(t.Partition) {
		u.Warnf("partion")
		return false
	}
	if m.Charset != t.Charset {
		u.Warnf("Charset %q != %q", m.Charset, t.Charset)
		return false
	}
	if m.PartitionCt != t.PartitionCt {
		u.Warnf("PartitionCt %q != %q", m.PartitionCt, t.PartitionCt)
		return false
	}
	if len(m.Indexes) != len(t.Indexes) {
		return false
	}
	for i, idx := range m.Indexes {
		if !idx.Equal(t.Indexes[i]) {
			return false
		}
	}
	if len(m.Context) != len(t.Context) {
		return false
	}
	for k, mv := range m.Context {
		if tv, ok := t.Context[k]; !ok || mv != tv {
			return false
		}
	}
	/*
		type TablePb struct {
			// Name of table lowercased
			Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
			// Name of table (not lowercased)
			NameOriginal string `protobuf:"bytes,2,opt,name=nameOriginal,proto3" json:"nameOriginal,omitempty"`
			// some dbs are more hiearchical (table-column-family)
			Parent string `protobuf:"bytes,3,opt,name=parent,proto3" json:"parent,omitempty"`
			// Character set, default = utf8
			Charset uint32 `protobuf:"varint,4,opt,name=charset,proto3" json:"charset,omitempty"`
			// Partitions in this table, optional may be empty
			Partition *TablePartition `protobuf:"bytes,5,opt,name=partition" json:"partition,omitempty"`
			// Partition Count
			PartitionCt uint32 `protobuf:"varint,6,opt,name=PartitionCt,proto3" json:"PartitionCt,omitempty"`
			// List of indexes for this table
			Indexes []*Index `protobuf:"bytes,7,rep,name=indexes" json:"indexes,omitempty"`
			// context is additional arbitrary map values
			Context map[string]string `protobuf:"bytes,8,rep,name=context" json:"context,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
			// List of Fields, in order
			Fieldpbs []*FieldPb `protobuf:"bytes,9,rep,name=fieldpbs" json:"fieldpbs,omitempty"`
		}
	*/
	if len(m.Fieldpbs) != len(t.Fieldpbs) {
		u.Warnf("Fieldpbs")
		return false
	}
	for i, f := range m.Fieldpbs {
		if !f.Equal(t.Fieldpbs[i]) {
			return false
		}
	}
	return true
}

// Marshal this Table as protobuf
func (m *Table) Marshal() ([]byte, error) {
	sourceType := ""
	if m.Source != nil {
		sourceType = m.Source.Type()
	}
	u.Warnf("Table.Marshal() %q source.Type=%q fieldpbct=%v  cols=%v", m.Name, sourceType, len(m.TablePb.Fieldpbs), m.cols)
	return proto.Marshal(&m.TablePb)
}

// Unmarshal this protbuf bytes into a Table
func (m *Table) Unmarshal(data []byte) error {
	if err := proto.Unmarshal(data, &m.TablePb); err != nil {
		return err
	}
	return m.initPb()
}

func (m *Table) initPb() error {
	/*
		Table struct {
			TablePb
			Fields         []*Field          // List of Fields, in order
			FieldPositions map[string]int    // Maps name of column to ordinal position in array of []driver.Value's
			FieldMap       map[string]*Field // Map of Field-name -> Field
			Schema         *Schema           // The schema this is member of
			Source         Source            // The source
			cols           []string          // array of column names
			rows           [][]driver.Value
		}
	*/
	m.cols = make([]string, len(m.Fieldpbs))
	m.Fields = make([]*Field, len(m.Fieldpbs))
	m.FieldPositions = make(map[string]int, len(m.Fieldpbs))
	m.FieldMap = make(map[string]*Field, len(m.Fieldpbs))
	u.Warnf("initpb unmarshal %v", len(m.Fieldpbs))
	for i, f := range m.Fieldpbs {
		m.Fields[i] = &Field{FieldPb: *f}
		m.FieldPositions[f.Name] = int(f.Position)
		m.FieldMap[f.Name] = m.Fields[i]
		m.cols[int(f.Position)] = f.Name
	}

	return nil
}
func (m *Table) initSchema(s *Schema) error {
	/*
		Table struct {
			TablePb
			Fields         []*Field          // List of Fields, in order
			FieldPositions map[string]int    // Maps name of column to ordinal position in array of []driver.Value's
			FieldMap       map[string]*Field // Map of Field-name -> Field
			Schema         *Schema           // The schema this is member of
			Source         Source            // The source
			cols           []string          // array of column names
			rows           [][]driver.Value
		}
	*/
	return nil
}

/*
type TablePartition struct {
	Table      string       `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	Keys       []string     `protobuf:"bytes,2,rep,name=keys" json:"keys,omitempty"`
	Partitions []*Partition `protobuf:"bytes,3,rep,name=partitions" json:"partitions,omitempty"`
}
*/
// Equal deep equality check for TablePartition.
func (m *TablePartition) Equal(t *TablePartition) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && t == nil {
		u.Warnf("wtf3")
		return false
	}
	if m.Table != t.Table {
		u.Warnf("Table %q != %q", m.Table, t.Table)
		return false
	}
	if len(m.Keys) != len(t.Keys) {
		u.Warnf("Keys")
		return false
	}
	for i, k := range m.Keys {
		if t.Keys[i] != k {
			u.Warnf("keys %d != %v", i, k)
			return false
		}
	}
	if len(m.Partitions) != len(t.Partitions) {
		return false
	}
	for i, p := range m.Partitions {
		if !p.Equal(t.Partitions[i]) {
			return false
		}
	}
	return true
}

/*
type Partition struct {
	Id    string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Left  string `protobuf:"bytes,2,opt,name=left,proto3" json:"left,omitempty"`
	Right string `protobuf:"bytes,3,opt,name=right,proto3" json:"right,omitempty"`
}
*/
// Equal deep equality check for Partition.
func (m *Partition) Equal(t *Partition) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && t == nil {
		u.Warnf("wtf3")
		return false
	}
	if m.Id != t.Id {
		u.Warnf("Id %q != %q", m.Id, t.Id)
		return false
	}
	if m.Left != t.Left {
		u.Warnf("Left %q != %q", m.Left, t.Left)
		return false
	}
	if m.Right != t.Right {
		u.Warnf("Right %q != %q", m.Right, t.Right)
		return false
	}
	return true
}

/*
type Index struct {
	Name          string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Fields        []string `protobuf:"bytes,2,rep,name=fields" json:"fields,omitempty"`
	PrimaryKey    bool     `protobuf:"varint,3,opt,name=primaryKey,proto3" json:"primaryKey,omitempty"`
	HashPartition []string `protobuf:"bytes,4,rep,name=hashPartition" json:"hashPartition,omitempty"`
	PartitionSize int32    `protobuf:"varint,5,opt,name=partitionSize,proto3" json:"partitionSize,omitempty"`
}
*/
// Equal deep equality check for Partition.
func (m *Index) Equal(t *Index) bool {
	if m == nil && t == nil {
		return true
	}
	if m == nil && t != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && t == nil {
		u.Warnf("wtf3")
		return false
	}
	if m.Name != t.Name {
		u.Warnf("Name %q != %q", m.Name, t.Name)
		return false
	}
	if m.PrimaryKey != t.PrimaryKey {
		u.Warnf("PrimaryKey %v != %v", m.PrimaryKey, t.PrimaryKey)
		return false
	}
	if m.PartitionSize != t.PartitionSize {
		u.Warnf("PartitionSize %v != %v", m.PartitionSize, t.PartitionSize)
		return false
	}
	if len(m.Fields) != len(t.Fields) {
		u.Warnf("Fields")
		return false
	}
	for i, k := range m.Fields {
		if t.Fields[i] != k {
			u.Warnf("Fields %d != %v", i, k)
			return false
		}
	}
	if len(m.HashPartition) != len(t.HashPartition) {
		u.Warnf("HashPartition")
		return false
	}
	for i, k := range m.HashPartition {
		if t.HashPartition[i] != k {
			u.Warnf("HashPartition %d != %v", i, k)
			return false
		}
	}
	return true
}

func (m *FieldPb) Equal(f *FieldPb) bool {
	/*
	   type FieldPb struct {
	   	Name        string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	   	Description string   `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	   	Key         string   `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	   	Extra       string   `protobuf:"bytes,4,opt,name=extra,proto3" json:"extra,omitempty"`
	   	Data        string   `protobuf:"bytes,5,opt,name=data,proto3" json:"data,omitempty"`
	   	Length      uint32   `protobuf:"varint,6,opt,name=length,proto3" json:"length,omitempty"`
	   	Type        uint32   `protobuf:"varint,7,opt,name=type,proto3" json:"type,omitempty"`
	   	NativeType  uint32   `protobuf:"varint,8,opt,name=nativeType,proto3" json:"nativeType,omitempty"`
	   	DefLength   uint64   `protobuf:"varint,9,opt,name=defLength,proto3" json:"defLength,omitempty"`
	   	DefVal      []byte   `protobuf:"bytes,11,opt,name=defVal,proto3" json:"defVal,omitempty"`
	   	Indexed     bool     `protobuf:"varint,13,opt,name=indexed,proto3" json:"indexed,omitempty"`
	   	NoNulls     bool     `protobuf:"varint,14,opt,name=noNulls,proto3" json:"noNulls,omitempty"`
	   	Collation   string   `protobuf:"bytes,15,opt,name=collation,proto3" json:"collation,omitempty"`
	   	Roles       []string `protobuf:"bytes,16,rep,name=roles" json:"roles,omitempty"`
	   	Indexes     []*Index `protobuf:"bytes,17,rep,name=indexes" json:"indexes,omitempty"`
	   	// context is additional arbitrary map values
	   	Context  map[string]string `protobuf:"bytes,18,rep,name=context" json:"context,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	   	Position uint64            `protobuf:"varint,19,opt,name=position,proto3" json:"position,omitempty"`
	   }
	*/
	if m == nil && f == nil {
		u.Warnf("wtf nil fields?")
		return true
	}
	if m == nil && f != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && f == nil {
		u.Warnf("wtf3")
		return false
	}
	if m.Name != f.Name {
		u.Warnf("name %q != %q", m.Name, f.Name)
		return false
	}
	if m.Description != f.Description {
		u.Warnf("Description")
		return false
	}
	if m.Key != f.Key {
		u.Warnf("Key")
		return false
	}
	if m.Extra != f.Extra {
		u.Warnf("Type")
		return false
	}
	if m.Data != f.Data {
		u.Warnf("Data")
		return false
	}
	if m.Length != f.Length {
		u.Warnf("Length")
		return false
	}
	if m.Type != f.Type {
		u.Warnf("Type")
		return false
	}
	if m.NativeType != f.NativeType {
		u.Warnf("NativeType")
		return false
	}
	if m.DefLength != f.DefLength {
		u.Warnf("DefLength")
		return false
	}
	if !bytes.Equal(m.DefVal, f.DefVal) {
		u.Warnf("DefVal")
		return false
	}
	if m.Indexed != f.Indexed {
		u.Warnf("Indexed")
		return false
	}
	if m.NoNulls != f.NoNulls {
		u.Warnf("NoNulls")
		return false
	}
	if m.Collation != f.Collation {
		u.Warnf("Collation")
		return false
	}
	if len(m.Roles) != len(f.Roles) {
		u.Warnf("Roles")
		return false
	}
	for i, k := range m.Roles {
		if f.Roles[i] != k {
			u.Warnf("Roles %d != %v", i, k)
			return false
		}
	}
	if len(m.Indexes) != len(f.Indexes) {
		return false
	}
	for i, idx := range m.Indexes {
		if !idx.Equal(f.Indexes[i]) {
			return false
		}
	}
	if len(m.Context) != len(f.Context) {
		return false
	}
	for k, mv := range m.Context {
		if fv, ok := f.Context[k]; !ok || mv != fv {
			return false
		}
	}
	if m.Position != f.Position {
		u.Warnf("Position")
		return false
	}
	return true
}

// NewFieldBase create a new field with base attributes.
func NewFieldBase(name string, valType value.ValueType, size int, desc string) *Field {
	f := FieldPb{
		Name:        name,
		Description: desc,
		Length:      uint32(size),
		Type:        uint32(valType),
		NativeType:  uint32(valType), // You need to over-ride this to change it
	}
	return &Field{FieldPb: f}
}

// NewField creates new field with more attributes.
func NewField(name string, valType value.ValueType, size int, allowNulls bool, defaultVal driver.Value, key, collation, description string) *Field {
	jb, _ := json.Marshal(defaultVal)
	f := FieldPb{
		Name:        name,
		Extra:       description,
		Description: description,
		Collation:   collation,
		Length:      uint32(size),
		Type:        uint32(valType),
		NativeType:  uint32(valType),
		NoNulls:     !allowNulls,
		DefVal:      jb,
		Key:         key,
	}
	return &Field{
		FieldPb: f,
	}
}
func (m *Field) ValueType() value.ValueType { return value.ValueType(m.Type) }
func (m *Field) Id() uint64                 { return m.Position }
func (m *Field) Body() interface{}          { return m }
func (m *Field) AsRow() []driver.Value {
	if len(m.row) > 0 {
		return m.row
	}
	m.row = make([]driver.Value, len(DescribeFullCols))
	// []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	m.row[0] = m.Name
	m.row[1] = value.ValueType(m.Type).String() // should we send this through a dialect-writer?  bc dialect specific?
	m.row[2] = m.Collation
	m.row[3] = ""
	m.row[4] = ""
	m.row[5] = ""
	m.row[6] = m.Extra
	m.row[7] = ""
	m.row[8] = m.Description // should we put native type in here?
	return m.row
}
func (m *Field) AddContext(key, value string) {
	if len(m.Context) == 0 {
		m.Context = make(map[string]string)
	}
	m.Context[key] = value
}
func (m *Field) Equal(f *Field) bool {
	if m == nil && f == nil {
		u.Warnf("wtf1")
		return true
	}
	if m == nil && f != nil {
		u.Warnf("wtf2")
		return false
	}
	if m != nil && f == nil {
		u.Warnf("wtf3")
		return false
	}
	if !m.FieldPb.Equal(&f.FieldPb) {
		return false
	}
	return true
}
func (m *Field) Marshal() ([]byte, error) {
	return proto.Marshal(&m.FieldPb)
}
func (m *Field) Unmarshal(data []byte) error {
	err := proto.Unmarshal(data, &m.FieldPb)
	if err != nil {
		return err
	}
	return nil
}

func NewDescribeFullHeaders() []*Field {
	fields := make([]*Field, 9)
	//[]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	fields[0] = NewFieldBase("Field", value.StringType, 255, "COLUMN_NAME")
	fields[1] = NewFieldBase("Type", value.StringType, 32, "COLUMN_TYPE")
	fields[2] = NewFieldBase("Collation", value.StringType, 32, "COLUMN_COLLATION")
	fields[3] = NewFieldBase("Null", value.StringType, 4, "IS_NULLABLE")
	fields[4] = NewFieldBase("Key", value.StringType, 64, "COLUMN_KEY")
	fields[5] = NewFieldBase("Default", value.StringType, 32, "COLUMN_DEFAULT")
	fields[6] = NewFieldBase("Extra", value.StringType, 255, "")
	fields[7] = NewFieldBase("Privileges", value.StringType, 255, "")
	fields[8] = NewFieldBase("Comment", value.StringType, 255, "")
	return fields
}
func NewDescribeHeaders() []*Field {
	fields := make([]*Field, 6)
	//[]string{"Field", "Type",  "Null", "Key", "Default", "Extra"}
	fields[0] = NewFieldBase("Field", value.StringType, 255, "COLUMN_NAME")
	fields[1] = NewFieldBase("Type", value.StringType, 32, "COLUMN_TYPE")
	fields[2] = NewFieldBase("Null", value.StringType, 4, "IS_NULLABLE")
	fields[3] = NewFieldBase("Key", value.StringType, 64, "COLUMN_KEY")
	fields[4] = NewFieldBase("Default", value.StringType, 32, "COLUMN_DEFAULT")
	fields[5] = NewFieldBase("Extra", value.StringType, 255, "")
	return fields
}

func NewSourceConfig(name, sourceType string) *ConfigSource {
	return &ConfigSource{
		Name: name,
		Type: sourceType,
	}
}
