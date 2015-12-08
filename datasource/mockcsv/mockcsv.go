package mockcsv

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	//"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	// Enforce Features of this MockCsv Data Source
	// - the rest are implemented in the static data source which has a Static per table
	_ datasource.DataSource = (*MockCsvSource)(nil)
	//_ datasource.SourceMutation = (*MockCsvSource)(nil)
	//_ datasource.Upsert   = (*MockCsvSource)(nil)
	//_ datasource.Deletion = (*MockCsvSource)(nil)

	MockCsvGlobal = NewMockSource()
)

func init() {
	datasource.Register("mockcsv", MockCsvGlobal)
}
func LoadTable(name, csvRaw string) {
	MockCsvGlobal.raw[name] = csvRaw
}

type MockCsvSource struct {
	tablenamelist []string
	tables        map[string]*membtree.StaticDataSource
	raw           map[string]string
}
type MockCsvTable struct {
	*membtree.StaticDataSource
	insert *expr.SqlInsert
}

func NewMockSource() *MockCsvSource {
	return &MockCsvSource{
		tablenamelist: make([]string, 0),
		raw:           make(map[string]string),
		tables:        make(map[string]*membtree.StaticDataSource),
	}
}

func (m *MockCsvSource) Open(tableName string) (datasource.SourceConn, error) {

	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		//u.Debugf("found cached mockcsv table:%q  len=%v", tableName, ds.Length())
		return &MockCsvTable{StaticDataSource: ds}, nil
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds := m.tables[tableName]
	return &MockCsvTable{StaticDataSource: ds}, nil
}

func (m *MockCsvSource) Table(tableName string) (*datasource.Table, error) {

	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		//u.Debugf("found cached mockcsv table:%q  len=%v", tableName, tbl.Length())
		return ds.Table(tableName)
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds, ok := m.tables[tableName]
	if !ok {
		return nil, datasource.ErrNotFound
	}
	return ds.Table(tableName)
}

func (m *MockCsvSource) loadTable(tableName string) error {

	csvRaw, ok := m.raw[tableName]
	if !ok {
		return datasource.ErrNotFound
	}
	sr := strings.NewReader(csvRaw)
	//u.Debugf("load mockcsv: %q  data:%v", tableName, csvRaw)
	csvSource, _ := datasource.NewCsvSource(tableName, 0, sr, make(<-chan bool, 1))
	tbl := membtree.NewStaticData(tableName)
	tbl.SetColumns(csvSource.Columns())
	//u.Infof("set index col for %v: %v -- %v", tableName, 0, csvSource.Columns()[0])
	m.tables[tableName] = tbl

	// Now we are going to page through the Csv Source and Put into
	//  Static Data Source, ie copy into memory
	for {
		msg := csvSource.Next()
		if msg == nil {
			//u.Infof("table:%v  len=%v", tableName, tbl.Length())
			return nil
		}
		dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
		if !ok {
			return fmt.Errorf("Expected []driver.Value but got %T", msg.Body())
		}

		// We don't know the Key
		tbl.Put(nil, nil, dm.Values())
	}
	return nil
}

func (m *MockCsvSource) Close() error     { return nil }
func (m *MockCsvSource) Tables() []string { return m.tablenamelist }
func (m *MockCsvSource) SetTable(tableName, csvRaw string) {
	if _, exists := m.raw[tableName]; !exists {
		m.tablenamelist = append(m.tablenamelist, tableName)
	}
	// Even if it exists, replace it?  Which would not work
	//  because the raw wouldn't get converted to
	m.raw[tableName] = csvRaw
}

//func (m *MockCsvTable) Close() error { return nil }

/*
// interface for Upsert.Put()
func (m *MockCsvTable) Put(ctx context.Context, key datasource.Key, row interface{}) (datasource.Key, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *MockCsvTable) PutMulti(ctx context.Context, keys []datasource.Key, src interface{}) ([]datasource.Key, error) {
	return nil, fmt.Errorf("not implemented")
}
*/

/*
// SourceMutation, is a statefull connection similar to Open() connection for select
//  - accepts the table used in this upsert/insert/update
//
type SourceMutation interface {
	CreateMutator(stmt expr.SqlStatement) (Mutator, error)
	//Create(tbl *Table, stmt expr.SqlStatement) (Mutator, error)
}

type Mutator interface {
	Upsert
	Deletion
}

// Mutation interface for Put
//  - assumes datasource understands key(s?)
type Upsert interface {
	Put(ctx context.Context, key Key, value interface{}) (Key, error)
	PutMulti(ctx context.Context, keys []Key, src interface{}) ([]Key, error)
}

// Patch Where, pass through where expression to underlying datasource
//  Used for update statements WHERE x = y
type PatchWhere interface {
	PatchWhere(ctx context.Context, where expr.Node, patch interface{}) (int64, error)
}

type Deletion interface {
	// Delete using this key
	Delete(driver.Value) (int, error)
	// Delete with given expression
	DeleteExpression(expr.Node) (int, error)
}

*/
