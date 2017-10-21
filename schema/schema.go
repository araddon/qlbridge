// The core Relational Algrebra schema objects such as Table,
// Schema, DataSource, Fields, Headers, Index.
package schema

import (
	"database/sql/driver"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// default schema Refresh Interval
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
	//columnColumns       = []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	ShowTableColumnMap = map[string]int{"Table": 0}
	//columnsColumnMap = map[string]int{"Field": 0, "Type": 1, "Null": 2, "Key": 3, "Default": 4, "Extra": 5}
	ShowIndexCols       = []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Index_comment"}
	DescribeFullHeaders = NewDescribeFullHeaders()
	DescribeHeaders     = NewDescribeHeaders()

	// We use Fields, and Tables as messages in Schema (SHOW, DESCRIBE)
	_ Message = (*Field)(nil)
	_ Message = (*Table)(nil)

	// Enforce interfaces
	_ SourceTableColumn = (*Table)(nil)
)

const (
	NoNulls    = false
	AllowNulls = true
)

type (
	// DialectWriter knows how to format the schema output
	//  specific to a dialect like mysql
	DialectWriter interface {
		Dialect() string
		Table(tbl *Table) string
		FieldType(t value.ValueType) string
	}
)

type (
	// Schema is a "Virtual" Schema and may have multiple different backing sources.
	// - Multiple DataSource(s) (each may be discrete source type such as mysql, elasticsearch, etc)
	// - each datasource supplies tables to the virtual table pool
	// - each table name across source's for single schema must be unique (or aliased)
	Schema struct {
		Name          string             // Name of schema
		Conf          *ConfigSource      // source configuration
		DS            Source             // This datasource Interface
		InfoSchema    *Schema            // represent this Schema as sql schema like "information_schema"
		parent        *Schema            // parent schema (optional)
		schemas       map[string]*Schema // map[schema-name]:Children Schemas
		tableSchemas  map[string]*Schema // Tables to schema map for parent/child
		tableMap      map[string]*Table  // Tables and their field info, flattened from all sources
		tableNames    []string           // List Table names, flattened all sources into one list
		lastRefreshed time.Time          // Last time we refreshed this schema
		mu            sync.RWMutex
	}

	// Table represents traditional definition of Database Table.  It belongs to a Schema
	// and can be used to create a Datasource used to read this table.
	Table struct {
		Name           string                 // Name of table lowercased
		NameOriginal   string                 // Name of table
		Parent         string                 // some dbs are more hiearchical (table-column-family)
		FieldPositions map[string]int         // Maps name of column to ordinal position in array of []driver.Value's
		Fields         []*Field               // List of Fields, in order
		FieldMap       map[string]*Field      // Map of Field-name -> Field
		Schema         *Schema                // The schema this is member of
		Source         Source                 // The source
		Charset        uint16                 // Character set, default = utf8
		Partition      *TablePartition        // Partitions in this table, optional may be empty
		PartitionCt    int                    // Partition Count
		Indexes        []*Index               // List of indexes for this table
		Context        map[string]interface{} // During schema discovery of underlying source, may need to store additional info
		tblId          uint64                 // internal tableid, hash of table name + schema?
		cols           []string               // array of column names
		lastRefreshed  time.Time              // Last time we refreshed this schema
		rows           [][]driver.Value
	}

	// Field Describes the column info, name, data type, defaults, index, null
	// - dialects (mysql, mongo, cassandra) have their own descriptors for these,
	//   so this is generic meant to be converted to Frontend at runtime
	Field struct {
		idx                uint64                 // Positional index in array of fields
		row                []driver.Value         // memoized values of this fields descriptors for describe
		Name               string                 // Column Name
		Description        string                 // Comment/Description
		Key                string                 // Key info (primary, etc) should be stored in indexes
		Extra              string                 // no idea difference with Description
		Data               FieldData              // Pre-generated dialect specific data???
		Length             uint32                 // field-size, ie varchar(20)
		Type               value.ValueType        // wire & stored type (often string, text, blob, []bytes for protobuf, json)
		NativeType         value.ValueType        // Native type for contents of stored type if stored as bytes but is json map[string]date etc
		DefaultValueLength uint64                 // Default
		DefaultValue       driver.Value           // Default value
		Indexed            bool                   // Is this indexed, if so we will have a list of indexes
		NoNulls            bool                   // Do we allow nulls?  default = false = yes allow nulls
		Collation          string                 // ie, utf8, none
		Roles              []string               // ie, {select,insert,update,delete}
		Indexes            []*Index               // Indexes this participates in
		Context            map[string]interface{} // During schema discovery of underlying source, may need to store additional info
	}
	FieldData []byte

	// Index a description of how data is/should be indexed
	Index struct {
		Name          string
		Fields        []string
		PrimaryKey    bool
		HashPartition []string
		PartitionSize int
	}

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
		SourceType   string            `json:"type"`            // [mysql,elasticsearch,csv,etc] Name in DataSource Registry
		TablesToLoad []string          `json:"tables_to_load"`  // if non empty, only load these tables
		TableAliases map[string]string `json:"table_aliases"`   // if non empty, only load these tables
		Nodes        []*ConfigNode     `json:"nodes"`           // List of nodes
		Hosts        []string          `json:"hosts"`           // List of hosts, replaces older "nodes"
		Settings     u.JsonHelper      `json:"settings"`        // Arbitrary settings specific to each source type
		Partitions   []*TablePartition `json:"partitions"`      // List of partitions per table (optional)
		PartitionCt  int               `json:"partition_count"` // Instead of array of per table partitions, raw partition count
	}

	// ConfigNode are Servers/Services, ie a running instance of said Source
	// - each must represent a single source type
	// - normal use is a server, describing partitions of servers
	// - may have arbitrary config info in Settings such as
	//     - user     = username
	//     - password = password
	//     - # connections
	ConfigNode struct {
		Name     string       `json:"name"`     // Name of this Node optional
		Source   string       `json:"source"`   // Name of source this node belongs to
		Address  string       `json:"address"`  // host/ip
		Settings u.JsonHelper `json:"settings"` // Arbitrary settings
	}
)

func NewSchema(schemaName string) *Schema {
	m := &Schema{
		Name:         strings.ToLower(schemaName),
		schemas:      make(map[string]*Schema),
		tableMap:     make(map[string]*Table),
		tableSchemas: make(map[string]*Schema),
		tableNames:   make([]string, 0),
	}
	return m
}

// RefreshSchema force a refresh of the underlying schema
func (m *Schema) RefreshSchema() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshSchemaUnlocked()
}
func (m *Schema) refreshSchemaUnlocked() {
	for _, tableName := range m.DS.Tables() {
		//tbl := ss.tableMap[tableName]
		u.Debugf("T:%T table name %s", m.DS, tableName)
		m.addschemaForTableUnlocked(tableName, m)
	}
	for _, ss := range m.schemas {
		ss.refreshSchemaUnlocked()
		for _, tableName := range ss.Tables() {
			//tbl := ss.tableMap[tableName]
			//u.Debugf("s:%p ss:%p add table name %s  tbl:%#v", m, ss, tableName, tbl)
			m.addschemaForTableUnlocked(tableName, ss)
		}
	}
}

func (m *Schema) AddChildSchema(child *Schema) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemas[child.Name] = child
	child.parent = m
	//m.refreshSchemaUnlocked()
}

// Schema Find a child Schema for given schema name
func (m *Schema) Schema(schemaName string) (*Schema, error) {
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

	m.mu.RLock()
	ss, ok := m.tableSchemas[tableName]
	if ok && ss != nil && ss.DS != nil {
		m.mu.RUnlock()
		return ss, nil
	}

	// In the event of schema tables, we are going to
	// lazy load??? fixme
	var schemaName string
	for schemaName, ss = range m.schemas {
		if schemaName == "schema" {
			break
		}
	}
	m.mu.RUnlock()

	// Lets Try to find in Schema Table?  Should we whitelist table names?
	if schemaName == "schema" {
		tbl, err := ss.Table(tableName)
		if err == nil && tbl.Name == tableName {
			return ss, nil
		}
		tbl, _ = ss.DS.Table(tableName)
		if tbl != nil {
			//ss.AddTable(tbl)
			return ss, nil
		}
	}

	u.Debugf("Schema: %p  no source!!!! %q", m, tableName)
	return nil, ErrNotFound
}

// OpenConn get a connection from this schema by table name.
func (m *Schema) OpenConn(tableName string) (Conn, error) {

	sch, ok := m.tableSchemas[tableName]
	if !ok {
		return nil, ErrNotFound
	}
	if sch.DS == nil {
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

// Is this schema uptodate?
func (m *Schema) Current() bool    { return m.Since(SchemaRefreshInterval) }
func (m *Schema) Tables() []string { return m.tableNames }
func (m *Schema) Table(tableName string) (*Table, error) {

	tableName = strings.ToLower(tableName)

	m.mu.RLock()
	defer m.mu.RUnlock()

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
	if tableName != "schema" {
		u.Debugf("s:%p could not find table in schema %q", m, tableName)
	}
	return nil, fmt.Errorf("Could not find that table: %v", tableName)
}

func (m *Schema) AddSchemaForTable(tableName string, ss *Schema) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addschemaForTableUnlocked(tableName, ss)
}
func (m *Schema) addschemaForTableUnlocked(tableName string, ss *Schema) {
	found := false
	for _, curTableName := range m.tableNames {
		if tableName == curTableName {
			found = true
		}
	}
	if !found {
		u.Debugf("Schema:%p addschemaForTableUnlocked %q  ", m, tableName)
		m.tableNames = append(m.tableNames, tableName)
		sort.Strings(m.tableNames)
		tbl := ss.tableMap[tableName]
		if tbl == nil {
			if err := m.loadTable(tableName); err != nil {
				u.Errorf("could not load table %v", err)
			} else {
				tbl = ss.tableMap[tableName]
				u.Infof("schema:%p did load table %v tables:%v", ss, tableName, ss.tableMap, tbl)
			}

		}
		if _, ok := m.tableMap[tableName]; !ok {
			m.tableSchemas[tableName] = ss
			m.tableMap[tableName] = tbl
		} else {
			u.Warnf("s:%p  no table? %v", m, m.tableMap)
		}
	}
}
func (m *Schema) addTable(tbl *Table) {
	m.tableSchemas[tbl.Name] = tbl.Schema
	m.tableMap[tbl.Name] = tbl
	tbl.init(m)
	m.addschemaForTableUnlocked(tbl.Name, tbl.Schema)
}

// Is this schema object within time window described by @dur time ago ?
func (m *Schema) Since(dur time.Duration) bool {
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}

func (m *Schema) AddTable(tbl *Table) {

	u.Debugf("schema:%p AddTable %#v", m, tbl)
	m.mu.Lock()
	defer m.mu.Unlock()

	hash := fnv.New64()
	hash.Write([]byte(tbl.Name))

	// create consistent-hash-id of this table name, and or table+schema
	tbl.tblId = hash.Sum64()
	m.tableMap[tbl.Name] = tbl
	if m.Conf != nil && m.Conf.PartitionCt > 0 {
		tbl.PartitionCt = m.Conf.PartitionCt
	} else if m.Conf != nil {
		for _, pt := range m.Conf.Partitions {
			if tbl.Name == pt.Table && tbl.Partition == nil {
				tbl.Partition = pt
			}
		}
	}

	//u.Infof("add table: %v partitionct:%v conf:%+v", tbl.Name, tbl.PartitionCt, m.Conf)
	m.addschemaForTableUnlocked(tbl.Name, m)
}

func (m *Schema) loadTable(tableName string) error {

	//u.Debugf("ss:%p  find: %v  tableMap:%v  %T", m, tableName, m.tableMap, m.DS)

	tbl, err := m.DS.Table(tableName)
	u.Debugf("tbl:%s  tbl=nil?%v  err=%v", tableName, tbl, err)
	if err != nil {
		if tableName == "tables" {
			return err
		}
		u.Debugf("could not find table %q for %#v", tableName, m.DS)
		return err
	}
	if tbl == nil {
		u.WarnT(10)
		return ErrNotFound
	}
	tbl.Schema = m

	// Add partitions
	if m.Conf != nil {
		for _, tp := range m.Conf.Partitions {
			if tp.Table == tableName {
				tbl.Partition = tp
				// for _, part := range tbl.Partitions {
				// 	u.Warnf("Found Partitions for %q = %#v", tableName, part)
				// }
			}
		}
	}

	//u.Infof("ss:%p about to add table %q", m, tableName)
	m.tableMap[tbl.Name] = tbl
	m.tableSchemas[tbl.Name] = m
	return nil
}

func NewTable(table string) *Table {
	t := &Table{
		Name:         strings.ToLower(table),
		NameOriginal: table,
		Fields:       make([]*Field, 0),
		FieldMap:     make(map[string]*Field),
	}
	t.SetRefreshed()
	return t
}
func (m *Table) init(s *Schema) {
	m.Schema = s
}
func (m *Table) HasField(name string) bool {
	if _, ok := m.FieldMap[name]; ok {
		return true
	}
	return false
}
func (m *Table) FieldsAsMessages() []Message {
	msgs := make([]Message, len(m.Fields))
	for i, f := range m.Fields {
		msgs[i] = f
	}
	return msgs
}
func (m *Table) Id() uint64        { return m.tblId }
func (m *Table) Body() interface{} { return m }

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

func (m *Table) AddFieldType(name string, valType value.ValueType) {
	m.AddField(&Field{Type: valType, Name: name})
}

// Underlying data type of column
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

// Explicityly set column names
func (m *Table) SetColumns(cols []string) {
	m.FieldPositions = make(map[string]int, len(cols))
	for idx, col := range cols {
		//col = strings.ToLower(col)
		m.FieldPositions[col] = idx
		cols[idx] = col
	}
	m.cols = cols
}

func (m *Table) Columns() []string { return m.cols }
func (m *Table) AsRows() [][]driver.Value {
	if len(m.rows) > 0 {
		return m.rows
	}
	m.rows = make([][]driver.Value, len(m.Fields))
	for i, f := range m.Fields {
		//u.Debugf("i:%d  f:%v", i, f)
		m.rows[i] = f.AsRow()
	}
	return m.rows
}
func (m *Table) SetRows(rows [][]driver.Value) {
	m.rows = rows
}

// List of Field Names and ordinal position in Column list
func (m *Table) FieldNamesPositions() map[string]int { return m.FieldPositions }

// Is this schema object current?  ie, have we refreshed it from
//  source since refresh interval
func (m *Table) Current() bool { return m.Since(SchemaRefreshInterval) }

// update the refreshed date to now
func (m *Table) SetRefreshed() { m.lastRefreshed = time.Now() }

// Is this schema object within time window described by @dur time ago ?
func (m *Table) Since(dur time.Duration) bool {
	u.Debugf("table?  %+v", m)
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}
func (m *Table) AddContext(key string, value interface{}) {
	if len(m.Context) == 0 {
		m.Context = make(map[string]interface{})
	}
	m.Context[key] = value
}

func NewFieldBase(name string, valType value.ValueType, size int, desc string) *Field {
	return &Field{
		Name:        name,
		Description: desc,
		Length:      uint32(size),
		Type:        valType,
		NativeType:  valType, // You need to over-ride this to change it
	}
}
func NewField(name string, valType value.ValueType, size int, allowNulls bool, defaultVal driver.Value, key, collation, description string) *Field {
	return &Field{
		Name:         name,
		Extra:        description,
		Description:  description,
		Collation:    collation,
		Length:       uint32(size),
		Type:         valType,
		NativeType:   valType,
		NoNulls:      !allowNulls,
		DefaultValue: defaultVal,
		Key:          key,
	}
}
func (m *Field) ValueType() value.ValueType { return m.Type }
func (m *Field) Id() uint64                 { return m.idx }
func (m *Field) Body() interface{}          { return m }
func (m *Field) AsRow() []driver.Value {
	if len(m.row) > 0 {
		return m.row
	}
	m.row = make([]driver.Value, len(DescribeFullCols))
	// []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	m.row[0] = m.Name
	m.row[1] = m.Type.String() // should we send this through a dialect-writer?  bc dialect specific?
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
