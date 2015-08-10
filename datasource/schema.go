package datasource

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// default schema Refresh Interval
	SchemaRefreshInterval = -time.Minute * 5

	// Static list of common field names for describe header
	describeCols    = []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	DescribeHeaders = NewDescribeHeaders()
)

type (
	// Schema is a "Virtual" Schema Database.  Made up of
	//  - Multiple DataSource(s) (each may be discrete source type)
	//  - each datasource supplies tables to the virtual table pool
	//  - each table from each source must be unique (or aliased)
	Schema struct {
		Name                string                   `json:"name"`
		SourceSchemas       map[string]*SourceSchema // map[schema_name]:Source Schemas
		TableMap            map[string]*Table        // Tables and their field info, flattened from all sources
		TableNames          []string                 // List Table names, flattened all sources into one list
		lastRefreshed       time.Time                // Last time we refreshed this schema
		showTableProjection *expr.Projection
	}

	// SourceSchema is a schema for a single DataSource
	SourceSchema struct {
		Name       string              // Source specific Schema name
		Conf       *SourceConfig       // source configuration
		Schema     *Schema             // Schema this is participating in
		TableMap   map[string]*Table   // Tables from this Source
		TableNames []string            // List Table names
		DSFeatures *DataSourceFeatures // The datasource Interface
		DS         DataSource          // This datasource Interface
		address    string
	}

	// Table represents traditional definition of Database Table
	//   It belongs to a Schema and can be used to
	//   create a Datasource used to read this table
	Table struct {
		Name            string            // Name of table lowercased
		NameOriginal    string            // Name of table
		FieldPositions  map[string]int    // Maps name of column to ordinal position in array of []driver.Value's
		Fields          []*Field          // List of Fields, in order
		FieldMap        map[string]*Field // List of Fields, in order
		DescribeValues  [][]driver.Value  // The Values that will be output for Describe
		Schema          *Schema           // The schema this is member of
		SourceSchema    *SourceSchema     // The source schema this is member of
		Charset         uint16            // Character set, default = utf8
		cols            []string          // array of column names
		lastRefreshed   time.Time         // Last time we refreshed this schema
		tableProjection *expr.Projection
	}

	// Field Describes the column info, name, data type, defaults, index
	Field struct {
		Name               string
		Description        string
		Data               FieldData
		Length             uint32
		Type               value.ValueType
		DefaultValueLength uint64
		DefaultValue       driver.Value
		Indexed            bool
	}
	FieldData []byte

	// Config for Source are storage/database/csvfiles
	//  - this represents a single source type
	//  - may have more than one node
	//  - belongs to a Schema ( or schemas)
	SourceConfig struct {
		Name          string       `json:"name"`           // Name
		SourceType    string       `json:"type"`           // [mysql,elasticsearch,csv,etc] Name in DataSource Registry
		TablesToLoad  []string     `json:"tables_to_load"` // if non empty, only load these tables
		Nodes         []NodeConfig `json:"nodes"`          // List of nodes
		Settings      u.JsonHelper `json:"settings"`       // Arbitrary settings specific to each source type
		tablesLoadMap map[string]struct{}
	}

	// Nodes are Servers
	//  - this represents a single source type
	//  - may have config info
	NodeConfig struct {
		Name     string       `json:"name"`     // Name of this Source, ie a database schema
		Settings u.JsonHelper `json:"settings"` // Arbitrary settings
	}
)

func NewSchema(schemaName string) *Schema {
	m := &Schema{
		Name:          strings.ToLower(schemaName),
		SourceSchemas: make(map[string]*SourceSchema),
		TableMap:      make(map[string]*Table),
		TableNames:    make([]string, 0),
	}
	return m
}

func (m *Schema) RefreshSchema() {
	for _, ss := range m.SourceSchemas {
		for _, tbl := range ss.TableMap {
			if _, exists := m.TableMap[tbl.Name]; !exists {
				m.TableNames = append(m.TableNames, tbl.Name)
			}
			m.TableMap[tbl.Name] = tbl
		}
	}
}

func (m *Schema) AddSourceSchema(ss *SourceSchema) {
	m.SourceSchemas[ss.Name] = ss
	m.RefreshSchema()
}

// Is this schema uptodate?
func (m *Schema) Current() bool    { return m.Since(SchemaRefreshInterval) }
func (m *Schema) Tables() []string { return m.TableNames }

func (m *Schema) Table(tableName string) (*Table, error) {
	tbl, ok := m.TableMap[tableName]
	if ok {
		return tbl, nil
	}
	return nil, fmt.Errorf("Could not find that table: %v", tableName)
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

func NewSourceSchema(name, sourceType string) *SourceSchema {
	m := &SourceSchema{
		Conf:       NewSourceConfig(name, sourceType),
		TableNames: make([]string, 0),
		TableMap:   make(map[string]*Table),
	}
	return m
}

func (m *SourceSchema) AddTable(tbl *Table) {
	if _, exists := m.TableMap[tbl.Name]; !exists {
		m.TableNames = append(m.TableNames, tbl.Name)
	}
	m.TableMap[tbl.Name] = tbl
}

func NewTable(table string, s *SourceSchema) *Table {
	t := &Table{
		Name:         strings.ToLower(table),
		NameOriginal: table,
		Fields:       make([]*Field, 0),
		FieldMap:     make(map[string]*Field),
		SourceSchema: s,
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

func (m *Table) AddValues(values []driver.Value) {
	m.DescribeValues = append(m.DescribeValues, values)
}

func (m *Table) AddField(fld *Field) {
	m.Fields = append(m.Fields, fld)
	m.FieldMap[fld.Name] = fld
}

func (m *Table) AddFieldType(name string, valType value.ValueType) {
	m.AddField(&Field{Type: valType, Name: name})
}

func (m *Table) SetColumns(cols []string) {
	m.cols = cols
	m.FieldPositions = make(map[string]int, len(cols))
	for idx, col := range cols {
		m.FieldPositions[col] = idx
	}
}

func (m *Table) Columns() []string { return m.cols }

// List of Field Names and ordinal position in Column list
func (m *Table) FieldNamesPositions() map[string]int { return m.FieldPositions }

// Is this schema object current?
func (m *Table) Current() bool { return m.Since(SchemaRefreshInterval) }

// update the refreshed date to now
func (m *Table) SetRefreshed() { m.lastRefreshed = time.Now() }

// Is this schema object within time window described by @dur time ago ?
func (m *Table) Since(dur time.Duration) bool {
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}

func NewField(name string, valType value.ValueType, size int, description string) *Field {
	return &Field{
		Name:        name,
		Description: description,
		Length:      uint32(size),
		Type:        valType,
	}
}

func NewDescribeHeaders() []*Field {
	fields := make([]*Field, 6)
	fields[0] = NewField("Field", value.StringType, 255, "COLUMN_NAME")
	fields[1] = NewField("Type", value.StringType, 32, "COLUMN_TYPE")
	fields[2] = NewField("Null", value.StringType, 4, "IS_NULLABLE")
	fields[3] = NewField("Key", value.StringType, 64, "COLUMN_KEY")
	fields[4] = NewField("Default", value.StringType, 32, "COLUMN_DEFAULT")
	fields[5] = NewField("Extra", value.StringType, 255, "EXTRA")
	return fields
}

func NewSourceConfig(name, sourceType string) *SourceConfig {
	return &SourceConfig{
		Name:       name,
		SourceType: sourceType,
	}
}

func (m *SourceConfig) Init() {
	if len(m.TablesToLoad) > 0 && len(m.tablesLoadMap) == 0 {
		tm := make(map[string]struct{})
		for _, tbl := range m.TablesToLoad {
			tm[tbl] = struct{}{}
		}
		m.tablesLoadMap = tm
	}
}

func (m *SourceConfig) String() string {
	return fmt.Sprintf(`<sourceconfig name=%q type=%q />`, m.Name, m.SourceType)
}
