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
)

const (
	NoNulls    = false
	AllowNulls = true
)

// DialectWriter knows how to format the schema output
//  specific to a dialect like mysql
type DialectWriter interface {
	Dialect() string
	Table(tbl *Table) string
	FieldType(t value.ValueType) string
}

type (
	// Schema is a "Virtual" Schema Database.
	//  - Multiple DataSource(s) (each may be discrete source type such as mysql, elasticsearch, etc)
	//  - each datasource supplies tables to the virtual table pool
	//  - each table name across source's for single schema must be unique (or aliased)
	Schema struct {
		Name          string                   // Name of schema
		InfoSchema    *Schema                  // represent this Schema as sql schema like "information_schema"
		SchemaSources map[string]*SchemaSource // map[source_name]:Source Schemas
		tableSources  map[string]*SchemaSource // Tables to source map
		tableMap      map[string]*Table        // Tables and their field info, flattened from all sources
		tableNames    []string                 // List Table names, flattened all sources into one list
		lastRefreshed time.Time                // Last time we refreshed this schema
		mu            sync.RWMutex
	}

	// SchemaSource is a schema for a single DataSource (elasticsearch, mysql, filesystem, elasticsearch)
	//  each DataSource would have multiple tables
	SchemaSource struct {
		Name       string            // Source specific Schema name, generally underlying db name
		Conf       *ConfigSource     // source configuration
		Schema     *Schema           // Schema this is participating in
		Partitions []*TablePartition // List of partitions per table (optional)
		DS         Source            // This datasource Interface
		tableMap   map[string]*Table // Tables from this Source
		tableNames []string          // List Table names
		address    string
	}

	// Table represents traditional definition of Database Table.  It belongs to a Schema
	// and can be used to create a Datasource used to read this table.
	Table struct {
		Name           string            // Name of table lowercased
		NameOriginal   string            // Name of table
		FieldPositions map[string]int    // Maps name of column to ordinal position in array of []driver.Value's
		Fields         []*Field          // List of Fields, in order
		FieldMap       map[string]*Field // Map of Field-name -> Field
		Schema         *Schema           // The schema this is member of
		SchemaSource   *SchemaSource     // The source schema this is member of
		Charset        uint16            // Character set, default = utf8
		Partition      *TablePartition   // Partitions in this table, optional may be empty
		PartitionCt    int               // Partition Count
		Indexes        []*Index          // List of indexes for this table
		tblId          uint64            // internal tableid, hash of table name + schema?
		cols           []string          // array of column names
		lastRefreshed  time.Time         // Last time we refreshed this schema
		rows           [][]driver.Value
	}

	// Field Describes the column info, name, data type, defaults, index, null
	//  - dialects (mysql, mongo, cassandra) have their own descriptors for these,
	//    so this is generic meant to be converted to Frontend at runtime
	Field struct {
		idx                uint64          // Positional index in array of fields
		row                []driver.Value  // memoized value of this field
		Name               string          // Column Name
		Description        string          // Comment/Description
		Key                string          // Key info (primary, etc) should be stored in indexes
		Extra              string          // no idea difference with Description
		Data               FieldData       // Pre-generated dialect specific data???
		Length             uint32          // field-size, ie varchar(20)
		Type               value.ValueType // wire & stored type (often string, text, blob, []bytes for protobuf, json)
		NativeType         value.ValueType // Native type for contents of stored type if stored as bytes but is json map[string]date etc
		DefaultValueLength uint64          // Default
		DefaultValue       driver.Value    // Default value
		Indexed            bool            // Is this indexed, if so we will have a list of indexes
		NoNulls            bool            // Do we allow nulls?  default = false = yes allow nulls
		Collation          string          // ie, utf8, none
		Roles              []string        // ie, {select,insert,update,delete}
		Indexes            []*Index        // Indexes this participates in
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

	// A SchemaConfig is the json/config block for Schema, the data-sources that make up this Virtual Schema
	//  - config to map name to multiple sources
	//  - connection info
	ConfigSchema struct {
		Name       string   `json:"name"`    // Virtual Schema Name, must be unique
		Sources    []string `json:"sources"` // List of sources , the names of the "Db" in source
		ConfigNode []string `json:"-"`       // List of backend Servers
	}

	// Config for Source are storage/database/csvfiles
	//  - this represents a single source type
	//  - may have more than one node
	//  - belongs to one or more virtual schemas
	ConfigSource struct {
		Name         string            `json:"name"`            // Name
		SourceType   string            `json:"type"`            // [mysql,elasticsearch,csv,etc] Name in DataSource Registry
		TablesToLoad []string          `json:"tables_to_load"`  // if non empty, only load these tables
		Nodes        []*ConfigNode     `json:"nodes"`           // List of nodes
		Hosts        []string          `json:"hosts"`           // List of hosts, replaces older "nodes"
		Settings     u.JsonHelper      `json:"settings"`        // Arbitrary settings specific to each source type
		Partitions   []*TablePartition `json:"partitions"`      // List of partitions per table (optional)
		PartitionCt  int               `json:"partition_count"` // Instead of array of per table partitions, raw partition count
	}

	// Nodes are Servers/Services, ie a running instance of said Source
	//  - each must represent a single source type
	//  - normal use is a server, describing partitions of servers
	//  - may have arbitrary config info in Settings such as
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
		Name:          strings.ToLower(schemaName),
		SchemaSources: make(map[string]*SchemaSource),
		tableMap:      make(map[string]*Table),
		tableSources:  make(map[string]*SchemaSource),
		tableNames:    make([]string, 0),
	}
	return m
}

// RefreshSchema force a refresh of the underlying schema
func (m *Schema) RefreshSchema() {
	m.mu.Lock()
	defer m.mu.Unlock()
	//u.Debugf("%p RefreshSchema START %#v", m, m.SchemaSources)
	m.refreshSchemaUnlocked()
	//u.Debugf("%p RefreshSchema END %#v", m, m.SchemaSources)
}
func (m *Schema) refreshSchemaUnlocked() {
	//u.WarnT(6)
	//defer func() { u.Debugf("exit schema refresh") }()
	for _, ss := range m.SchemaSources {
		if ss.DS == nil {
			for _, tableName := range ss.Tables() {
				//u.Infof("tableName %s", tableName)
				ss.AddTableName(tableName)
				m.addTableNameUnlocked(tableName, ss)
			}
			return
		}
		//u.Infof("ss %#v", ss)
		for _, tableName := range ss.DS.Tables() {
			//u.Infof("tableName %s", tableName)
			ss.AddTableName(tableName)
			m.addTableNameUnlocked(tableName, ss)
		}
	}
}

func (m *Schema) AddSourceSchema(ss *SchemaSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SchemaSources[ss.Name] = ss
	m.refreshSchemaUnlocked()
}

// Find a SchemaSource for this Table
func (m *Schema) Source(tableName string) (*SchemaSource, error) {
	//u.Debugf("%p Schema Source(%q) %v", m, tableName, m.tableSources)
	m.mu.RLock()

	ss, ok := m.tableSources[tableName]

	if ok && ss != nil && ss.DS != nil {
		//u.Infof("%p %p  found? %v  ss=%#v", m, ss, ok, ss)
		m.mu.RUnlock()
		return ss, nil
	}
	if ok && ss != nil && ss.DS == nil {
		//u.Warnf("no DS? %q  ", tableName)
		//return nil, fmt.Errorf("no DataSource for %q", tableName)
	} else {
		ss, ok = m.tableSources[strings.ToLower(tableName)]
		if ok && ss != nil {
			m.mu.RUnlock()
			return ss, nil
		}
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// If a table source has been added since we built this
	// internal schema table cache, it may be missing so try to refresh it
	for _, ss2 := range m.SchemaSources {
		if ss2.DS == nil {
			//u.Debugf("missing ds? %#v", ss2)
			continue
		}
		for _, tbl := range ss2.DS.Tables() {
			if _, exists := m.tableSources[tbl]; !exists {
				//m.tableSources[tbl] = ss
				//u.Debugf("%p Schema  new table? %s:%v", ss2.Schema, sourceName, tbl)
				if ss2.Schema == m {
					m.refreshSchemaUnlocked()
				} else {
					ss2.Schema.RefreshSchema()
				}
				return ss2, nil
			} else if tbl == tableName {
				//u.Warnf("WHAT?  we should have a DS on tableSources?")
				if ss != nil {
					ss.DS = ss2.DS
				}
				//ss.DS = ss2.DS
				return ss2, nil
			}
		}
	}
	return nil, fmt.Errorf("Could not find a source for that table %q", tableName)
}

// Open get a connection from this schema via table name
func (m *Schema) Open(tableName string) (Conn, error) {
	//u.Debugf("%p Schema Open(%q) %v", m, tableName, m.tableSources)
	source, err := m.Source(tableName)
	if err != nil {
		//u.LogTracef(u.WARN, "What, no table? %v", tableName)
		return nil, err
	}
	if source.DS == nil {
		//u.Warnf("%p Schema no table? %v", m, tableName)
		return nil, fmt.Errorf("Could not find a DataSource for that table %q", tableName)
	}

	conn, err := source.DS.Open(tableName)
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
	//u.Debugf("%p schema.Table(%q)", tableName)
	m.mu.RLock()
	tbl, ok := m.tableMap[tableName]
	if ok && tbl != nil {
		m.mu.RUnlock()
		return tbl, nil
	}
	// Lets see if it is   `schema`.`table` format
	_, tableName, ok = expr.LeftRight(tableName)
	if ok {
		tbl, ok = m.tableMap[tableName]
		if ok && tbl != nil {
			m.mu.RUnlock()
			return tbl, nil
		}
	}

	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.findTable(strings.ToLower(tableName))
}
func (m *Schema) findTable(tableName string) (*Table, error) {
	//u.Debugf("%p Schema  %v  tableMap:%v", m, m.tableSources, m.tableMap)
	if ss, ok := m.tableSources[tableName]; ok {
		//u.Debugf("try to get from source schema table:%q %T", tableName, ss.DS)
		if sourceTable, ok := ss.DS.(SourceTableSchema); ok {
			tbl, err := sourceTable.Table(tableName)
			//u.Infof("table:%q  tbl%v  err=%v", tableName, tbl, err)
			if err != nil {
				return nil, err
			}
			if tbl == nil {
				return nil, ErrNotFound
			}
			// Add partitions
			for _, tp := range ss.Partitions {
				if tp.Table == tableName {
					tbl.Partition = tp
					// for _, part := range tbl.Partitions {
					// 	u.Warnf("Found Partitions for %q = %#v", tableName, part)
					// }
				}
			}
			//u.Infof("about to add table %q", tableName)
			m.addTable(tbl)
			return tbl, nil
		}
	}
	u.Warnf("could not find table in schema %q", tableName)
	return nil, fmt.Errorf("Could not find that table: %v", tableName)
}

func (m *Schema) AddTableName(tableName string, ss *SchemaSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addTableNameUnlocked(tableName, ss)
}
func (m *Schema) addTableNameUnlocked(tableName string, ss *SchemaSource) {
	found := false
	for _, curTableName := range m.tableNames {
		if tableName == curTableName {
			found = true
		}
	}
	if !found {
		m.tableNames = append(m.tableNames, tableName)
		sort.Strings(m.tableNames)
		if _, ok := m.tableMap[tableName]; !ok {
			m.tableSources[tableName] = ss
			m.tableMap[tableName] = nil
		}
	}
}
func (m *Schema) addTable(tbl *Table) {
	//u.Infof("add table %+v", tbl)
	m.tableSources[tbl.Name] = tbl.SchemaSource
	m.tableMap[tbl.Name] = tbl
	m.addTableNameUnlocked(tbl.Name, tbl.SchemaSource)
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

func NewSchemaSource(name, sourceType string) *SchemaSource {
	m := &SchemaSource{
		Name:       name,
		Conf:       NewSourceConfig(name, sourceType),
		tableNames: make([]string, 0),
		tableMap:   make(map[string]*Table),
	}
	return m
}
func (m *SchemaSource) AddTableName(tableName string) {

	// check if we only want to load certain tables from this source
	lowerTable := tableName
	if len(m.Conf.TablesToLoad) > 0 {
		loadTable := false
		for _, tblToLoad := range m.Conf.TablesToLoad {
			if strings.ToLower(tblToLoad) == lowerTable {
				loadTable = true
				break
			}
		}
		if !loadTable {
			return
		}
	}

	// see if we already have this table
	found := false
	for _, curTableName := range m.tableNames {
		if tableName == curTableName {
			found = true
		}
	}
	if !found {
		m.tableNames = append(m.tableNames, tableName)
		sort.Strings(m.tableNames)
		if m.Schema == nil {
			//u.LogTracef(u.WARN, "%p WAT?  nil schema?  %#v", m, m)
			//u.Warnf("%p SchemaSource no schema ", m)
		} else {
			m.Schema.addTableNameUnlocked(tableName, m)
		}
		if _, ok := m.tableMap[tableName]; !ok {
			m.tableMap[tableName] = nil
		}
	}
}
func (m *SchemaSource) AddTable(tbl *Table) {
	hash := fnv.New64()
	if m.Schema != nil {
		// Do id's need to be unique across schemas?   seems bit overkill
		//hash.Write([]byte(m.Name + tbl.Name))
		hash.Write([]byte(tbl.Name))
		m.Schema.addTable(tbl)
	} else {
		hash.Write([]byte(tbl.Name))
		//u.Warnf("no SCHEMA for table!!!!!! %#v", tbl)
	}
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
	m.AddTableName(tbl.Name)
}
func (m *SchemaSource) Tables() []string { return m.tableNames }
func (m *SchemaSource) Table(tableName string) (*Table, error) {
	tbl, ok := m.tableMap[tableName]
	if ok && tbl != nil {
		return tbl, nil
	} else if ok && tbl == nil {
		//u.Infof("try to get table from source schema %v", tableName)
		if sourceTable, ok := m.DS.(SourceTableSchema); ok {
			tbl, err := sourceTable.Table(tableName)
			if err == nil {
				m.AddTable(tbl)
			}
			return tbl, err
		}
	}
	if tbl != nil && !tbl.Current() {
		// What?
		if sourceTable, ok := m.DS.(SourceTableSchema); ok {
			tbl, err := sourceTable.Table(tableName)
			if err == nil {
				m.AddTable(tbl)
			}
			return tbl, err
		}
	}
	return nil, fmt.Errorf("Could not find that table: %v", tableName)
}
func (m *SchemaSource) HasTable(table string) bool {
	_, hasTable := m.tableMap[table]
	return hasTable
}

func NewTable(table string, s *SchemaSource) *Table {
	t := &Table{
		Name:         strings.ToLower(table),
		NameOriginal: table,
		Fields:       make([]*Field, 0),
		FieldMap:     make(map[string]*Field),
		SchemaSource: s,
	}
	t.SetRefreshed()
	t.init()
	return t
}

func (m *Table) init() {}

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

// Explicityly set column names
func (m *Table) SetColumns(cols []string) {
	//u.LogTracef(u.WARN, "who uses me?")
	m.cols = cols
	m.FieldPositions = make(map[string]int, len(cols))
	for idx, col := range cols {
		m.FieldPositions[col] = idx
	}
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

func NewFieldBase(name string, valType value.ValueType, size int, extra string) *Field {
	return &Field{
		Name:   name,
		Extra:  extra,
		Length: uint32(size),
		Type:   valType,
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
		NoNulls:      !allowNulls,
		DefaultValue: defaultVal,
		Key:          key,
	}
}

func (m *Field) Id() uint64        { return m.idx }
func (m *Field) Body() interface{} { return m }
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
	m.row[8] = m.Description
	return m.row
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
