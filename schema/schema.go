// Package schema implements core Relational Algrebra schema objects such as Table,
// Schema, DataSource, Fields, Headers, Index.
package schema

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"hash/fnv"
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

	// We use Fields, and Tables as messages in Schema (SHOW, DESCRIBE)
	_ Message = (*Field)(nil)
	_ Message = (*Table)(nil)

	// Enforce interfaces
	_ SourceTableColumn = (*Table)(nil)

	// Schema In Mem must implement applyer
	_ Applyer = (*InMemApplyer)(nil)

	// Enforce proto marshalling
	_ proto.Marshaler = (*Table)(nil)
	//_ proto.Unmarshaler = (*Table)(nil)

	_ = u.EMPTY
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

	// Schema is a "Virtual" Schema and may have multiple different backing sources.
	// - Multiple DataSource(s) (each may be discrete source type such as mysql, elasticsearch, etc)
	// - each schema supplies tables to the virtual table pool
	// - each table name across schemas must be unique (or aliased)
	Schema struct {
		Name          string             // Name of schema
		Conf          *ConfigSource      // source configuration
		DS            Source             // This datasource Interface
		InfoSchema    *Schema            // represent this Schema as sql schema like "information_schema"
		SchemaRef     *Schema            // IF this is infoschema, the schema it refers to
		parent        *Schema            // parent schema (optional) if nested.
		schemas       map[string]*Schema // map[schema-name]:Children Schemas
		tableSchemas  map[string]*Schema // Tables to schema map for parent/child
		tableMap      map[string]*Table  // Tables and their field info, flattened from all child schemas
		tableNames    []string           // List Table names, flattened all schemas into one list
		lastRefreshed time.Time          // Last time we refreshed this schema
		mu            sync.RWMutex       // lock for schema mods
	}

	// Table represents traditional definition of Database Table.  It belongs to a Schema
	// and can be used to create a Datasource used to read this table.
	Table struct {
		TablePb
		Fields         []*Field               // List of Fields, in order
		Context        map[string]interface{} // During schema discovery of underlying source, may need to store additional info
		FieldPositions map[string]int         // Maps name of column to ordinal position in array of []driver.Value's
		FieldMap       map[string]*Field      // Map of Field-name -> Field
		Schema         *Schema                // The schema this is member of
		Source         Source                 // The source
		tblID          uint64                 // internal tableid, hash of table name + schema?
		cols           []string               // array of column names
		lastRefreshed  time.Time              // Last time we refreshed this schema
		rows           [][]driver.Value
	}

	// Field Describes the column info, name, data type, defaults, index, null
	// - dialects (mysql, mongo, cassandra) have their own descriptors for these,
	//   so this is generic meant to be converted to Frontend at runtime
	Field struct {
		idx uint64         // Positional index in array of fields
		row []driver.Value // memoized values of this fields descriptors for describe
		FieldPb
		Context map[string]interface{} // During schema discovery of underlying source, may need to store additional info
	}
	// FieldData is the byte value of a "Described" field ready to write to the wire so we don't have
	// to continually re-serialize it.
	FieldData []byte

	// ConfigSchema is the json/config block for Schema, the data-sources
	// that make up this Virtual Schema.  Must have a name and list
	// of sources to include.
	ConfigSchema struct {
		Name       string   `json:"name"`    // Virtual Schema Name, must be unique
		Sources    []string `json:"sources"` // List of sources , the names of the "Db" in source
		ConfigNode []string `json:"-"`       // List of backend Servers
	}

	// ConfigSource are backend datasources ie : storage/database/csvfiles
	// Each represents a single source type/config.  May belong to more
	// than one schema.
	ConfigSource struct {
		Name         string            `json:"name"`            // Name
		Schema       string            `json:"schema"`          // Schema Name if different than Name, will join existing schema
		SourceType   string            `json:"type"`            // [mysql,elasticsearch,csv,etc] Name in DataSource Registry
		TablesToLoad []string          `json:"tables_to_load"`  // if non empty, only load these tables
		TableAliases map[string]string `json:"table_aliases"`   // if non empty, only load these tables
		Nodes        []*ConfigNode     `json:"nodes"`           // List of nodes
		Hosts        []string          `json:"hosts"`           // List of hosts, replaces older "nodes"
		Settings     u.JsonHelper      `json:"settings"`        // Arbitrary settings specific to each source type
		Partitions   []*TablePartition `json:"partitions"`      // List of partitions per table (optional)
		PartitionCt  uint32            `json:"partition_count"` // Instead of array of per table partitions, raw partition count
	}

	// ConfigNode are Servers/Services, ie a running instance of said Source
	// - each must represent a single source type
	// - normal use is a server, describing partitions of servers
	// - may have arbitrary config info in Settings.
	ConfigNode struct {
		Name     string       `json:"name"`     // Name of this Node optional
		Source   string       `json:"source"`   // Name of source this node belongs to
		Address  string       `json:"address"`  // host/ip
		Settings u.JsonHelper `json:"settings"` // Arbitrary settings
	}
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
		Name:         strings.ToLower(schemaName),
		schemas:      make(map[string]*Schema),
		tableMap:     make(map[string]*Table),
		tableSchemas: make(map[string]*Schema),
		tableNames:   make([]string, 0),
		DS:           ds,
	}
	return m
}

// Since Is this schema object been refreshed within time window described by @dur time ago ?
func (m *Schema) Since(dur time.Duration) bool {
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}

// Current Is this schema up to date?
func (m *Schema) Current() bool { return m.Since(SchemaRefreshInterval) }

// Tables gets list of all tables for this schema.
func (m *Schema) Tables() []string { return m.tableNames }

// Table gets Table definition for given table name
func (m *Schema) Table(tableIn string) (*Table, error) {

	tableName := strings.ToLower(tableIn)

	m.mu.RLock()
	defer m.mu.RUnlock()

	// u.Debugf("%p looking up %q", m, tableName)

	tbl, ok := m.tableMap[tableName]
	if ok && tbl != nil {
		return tbl, nil
	}

	// Lets see if it is   `schema`.`table` format
	_, tableName, ok = expr.LeftRight(tableName)
	if ok {
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

// addChildSchema add a child schema to this one.  Schemas can be tree-in-nature
// with schema of multiple backend datasources being combined into parent Schema, but each
// child has their own unique defined schema.
func (m *Schema) addChildSchema(child *Schema) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemas[child.Name] = child
	child.parent = m
	child.mu.RLock()
	defer child.mu.RUnlock()
	for tableName, tbl := range child.tableMap {
		m.tableSchemas[tableName] = child
		m.tableMap[tableName] = tbl
	}
}

/*
// AddSchemaForTable add table.
func (m *Schema) addSchemaForTable(tableName string, ss *Schema) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addschemaForTableUnlocked(tableName, ss)
}
*/
func (m *Schema) refreshSchemaUnlocked() {

	m.lastRefreshed = time.Now()

	if m.DS != nil {
		for _, tableName := range m.DS.Tables() {
			//u.Debugf("%p:%s  DS T:%T table name %s", m, m.Name, m.DS, tableName)
			m.addschemaForTableUnlocked(tableName, m)
		}
	}

	for _, ss := range m.schemas {
		//u.Infof("schema  %p:%s", ss, ss.Name)
		ss.refreshSchemaUnlocked()
		for _, tableName := range ss.Tables() {
			//tbl := ss.tableMap[tableName]
			//u.Debugf("s:%p ss:%p add table name %s  tbl:%#v", m, ss, tableName, tbl)
			m.addschemaForTableUnlocked(tableName, ss)
		}
	}
}

func (m *Schema) dropTable(tbl *Table) error {

	// u.Warnf("%p drop %s %v", m, m.Name, m.Tables())
	//u.Infof("infoschema %#v", m.InfoSchema)

	tl := make([]string, 0, len(m.tableNames))
	for _, tn := range m.tableNames {
		if tbl.Name != tn {
			tl = append(tl, tn)
		}
	}

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
	m.tableNames = tl

	if salter, ok := m.InfoSchema.DS.(Alter); ok {
		err := salter.DropTable(tbl.Name)
		if err != nil {
			u.Warnf("err %v", err)
			return err
		}
	}

	return nil
}

func (m *Schema) addTable(tbl *Table) error {

	// u.Debugf("schema:%p AddTable %#v", m, tbl)

	// create consistent-hash-id of this table name, and or table+schema
	hash := fnv.New64()
	hash.Write([]byte(tbl.Name))
	tbl.tblID = hash.Sum64()

	// Assign partitions
	if m.Conf != nil && m.Conf.PartitionCt > 0 {
		tbl.PartitionCt = uint32(m.Conf.PartitionCt)
	} else if m.Conf != nil {
		for _, pt := range m.Conf.Partitions {
			if tbl.Name == pt.Table && tbl.Partition == nil {
				tbl.Partition = pt
			}
		}
	}

	//u.Infof("add table: %v partitionct:%v conf:%+v", tbl.Name, tbl.PartitionCt, m.Conf)
	tbl.init(m)

	m.tableMap[tbl.Name] = tbl

	m.addschemaForTableUnlocked(tbl.Name, tbl.Schema)
	return nil
}

func (m *Schema) addschemaForTableUnlocked(tableName string, ss *Schema) {
	found := false
	for _, curTableName := range m.tableNames {
		if tableName == curTableName {
			found = true
		}
	}
	if !found {
		// u.Debugf("%p:%s Schema addschemaForTableUnlocked %q  ", m, m.Name, tableName)
		m.tableNames = append(m.tableNames, tableName)
		sort.Strings(m.tableNames)
		tbl := ss.tableMap[tableName]
		if tbl == nil {
			if err := m.loadTable(tableName); err != nil {
				u.Debugf("could not load table %v", err)
				return
			} else {
				tbl = ss.tableMap[tableName]
			}
		}
		if _, ok := m.tableMap[tableName]; !ok {
			m.tableSchemas[tableName] = ss
			m.tableMap[tableName] = tbl
		}
	}
}

func (m *Schema) loadTable(tableName string) error {

	// u.Infof("%p schema.%v loadTable(%q)", m, m.Name, tableName)

	if m.DS == nil {
		return nil
	}

	tbl, err := m.DS.Table(tableName)
	if err != nil {
		if tableName == "tables" {
			return err
		}
		return err
	}
	if tbl == nil {
		return ErrNotFound
	}
	tbl.Schema = m

	// Add partitions
	if m.Conf != nil {
		for _, tp := range m.Conf.Partitions {
			if tp.Table == tableName {
				tbl.Partition = tp
			}
		}
	}

	m.tableMap[tbl.Name] = tbl
	m.tableSchemas[tbl.Name] = m
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
	t.init(nil)
	return t
}
func (m *Table) init(s *Schema) {
	m.Schema = s
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

// Id satisifieds Message Interface
func (m *Table) Id() uint64 { return m.tblID }

// Body satisifies Message Interface
func (m *Table) Body() interface{} { return m }

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
		fld.idx = uint64(len(m.Fields))
		m.Fields = append(m.Fields, fld)
	}
	m.FieldMap[fld.Name] = fld
}

// AddFieldType describe and register a new column
func (m *Table) AddFieldType(name string, valType value.ValueType) {
	m.AddField(&Field{FieldPb: FieldPb{Type: uint32(valType), Name: name}})
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

// Current Is this schema object current?  ie, have we refreshed it from
// source since refresh interval.
func (m *Table) Current() bool { return m.Since(SchemaRefreshInterval) }

// SetRefreshed update the refreshed date to now.
func (m *Table) SetRefreshed() { m.lastRefreshed = time.Now() }

// Since Is this schema object within time window described by @dur time ago ?
func (m *Table) Since(dur time.Duration) bool {
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}

// AddContext add key/value pairs to context (settings, metatadata).
func (m *Table) AddContext(key string, value interface{}) {
	if len(m.Context) == 0 {
		m.Context = make(map[string]interface{})
	}
	m.Context[key] = value
}

func (m *Table) Marshal() ([]byte, error) {
	return proto.Marshal(&m.TablePb)
}

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
func (m *Field) Id() uint64                 { return m.idx }
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
func (m *Field) AddContext(key string, value interface{}) {
	if len(m.Context) == 0 {
		m.Context = make(map[string]interface{})
	}
	m.Context[key] = value
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
		Name:       name,
		SourceType: sourceType,
	}
}

func (m *ConfigSource) String() string {
	return fmt.Sprintf(`<sourceconfig name=%q type=%q settings=%v/>`, m.Name, m.SourceType, m.Settings)
}
