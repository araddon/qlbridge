package schema_test

import (
	"database/sql/driver"
	"encoding/json"
	"sort"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

func init() {
	testutil.Setup()
}
func TestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}

func TestRegisterSchema(t *testing.T) {

	reg := schema.DefaultRegistry()

	inrow := []driver.Value{122, "bob", "bob@email.com"}

	cols := []string{"user_id", "name", "email"}
	db, err := memdb.NewMemDbData("users", [][]driver.Value{inrow}, cols)
	assert.Equal(t, nil, err)
	db.Init()
	db.Setup(nil)

	assert.Equal(t, []string{"users"}, db.Tables())

	c, err := db.Open("users")
	assert.Equal(t, nil, err)
	_, ok := c.(schema.ConnAll)
	assert.True(t, ok)

	err = schema.RegisterSourceAsSchema("user_csv", db)
	assert.Equal(t, nil, err)

	s, ok := schema.DefaultRegistry().Schema("user_csv")
	assert.Equal(t, true, ok)
	assert.NotEqual(t, nil, s)

	assert.Equal(t, []string{"users"}, s.Tables())

	inrow2 := []driver.Value{122, "bob", "bob@email.com"}
	cols2 := []string{"account_id", "name", "email"}
	db2, err := memdb.NewMemDbData("accounts", [][]driver.Value{inrow2}, cols2)
	assert.Equal(t, nil, err)
	db.Init()
	db.Setup(nil)
	childSchema := schema.NewSchemaSource("user_child", db2)
	err = reg.SchemaAddChild("user_csv", childSchema)
	assert.Equal(t, nil, err)

	expectTables := []string{"users", "accounts"}
	sort.Strings(expectTables)
	gotTables := s.Tables()
	sort.Strings(gotTables)
	assert.Equal(t, expectTables, gotTables)

	child2, err := s.Schema("user_child")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, child2)
	assert.Equal(t, []string{"accounts"}, child2.Tables())

	_, err = s.Schema("does_not_exist")
	assert.NotEqual(t, nil, err)
}

func TestAddSchemaFromConfig(t *testing.T) {

	reg := schema.DefaultRegistry()

	inrow := []driver.Value{122, "bob", "bob@email.com"}
	cols := []string{"user_id", "name", "email"}
	db, err := memdb.NewMemDbData("users", [][]driver.Value{inrow}, cols)
	assert.Equal(t, nil, err)

	schema.RegisterSourceType("schema_from_config_db", db)

	by := []byte(`{
      "name": "myschema",
      "type": "schema_from_config_db"
    }`)

	sourceConf := &schema.ConfigSource{}
	err = json.Unmarshal(by, sourceConf)
	assert.Equal(t, nil, err)
	err = reg.SchemaAddFromConfig(sourceConf)
	assert.Equal(t, nil, err)

	s, ok := reg.Schema("myschema")
	assert.Equal(t, true, ok)
	assert.NotEqual(t, nil, s)

	reg.RemoveSchema("myschema")
}

func TestSchema(t *testing.T) {
	a := schema.NewApplyer(func(s *schema.Schema) schema.Source {
		sdb := datasource.NewSchemaDb(s)
		s.InfoSchema.DS = sdb
		return sdb
	})
	reg := schema.NewRegistry(a)
	a.Init(reg)

	inrow := []driver.Value{122, "bob", "bob@email.com"}
	cols := []string{"user_id", "name", "email"}
	db, err := memdb.NewMemDbData("users", [][]driver.Value{inrow}, cols)
	assert.Equal(t, nil, err)

	assert.Equal(t, []string{"users"}, db.Tables())

	s := schema.NewSchema("user_csv2")
	assert.Equal(t, false, s.Current())
	s.DS = db

	err = reg.SchemaAdd(s)
	assert.Equal(t, nil, err)
	s, ok := reg.Schema("user_csv2")
	assert.Equal(t, true, ok)

	assert.Equal(t, true, s.Current())
	reg.RemoveSchema("user_csv2")

	tbl, err := s.Table("use_csv2.users")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, tbl)

	_, err = s.OpenConn("not_a_table")
	assert.NotEqual(t, nil, err)

	_, err = s.SchemaForTable("not_a_table")
	assert.NotEqual(t, nil, err)
}
func TestTable(t *testing.T) {
	tbl := schema.NewTable("users")

	assert.Equal(t, "users", tbl.Name)
	assert.Equal(t, uint64(0), tbl.Id())
	assert.Equal(t, false, tbl.Current())
	tbl.SetRefreshed()
	assert.Equal(t, true, tbl.Current())
	schema.SchemaRefreshInterval = time.Minute * 5
	tbl.SetRefreshed()
	assert.Equal(t, false, tbl.Current())

	f := schema.NewFieldBase("first_name", value.StringType, 255, "string")
	tbl.AddField(f)

	tbl.AddField(schema.NewFieldBase("last_name", value.StringType, 255, "string"))
	vt, ok := tbl.Column("first_name")
	assert.True(t, ok)
	assert.Equal(t, value.StringType, vt)
	vt, ok = tbl.Column("FIRST_NAME")
	assert.True(t, ok)
	assert.Equal(t, value.StringType, vt)
	_, ok = tbl.Column("age")
	assert.True(t, !ok)
	assert.True(t, tbl.HasField("first_name"))
	assert.Equal(t, false, tbl.HasField("not_name"))

	tbl.SetColumnsFromFields()
	assert.Equal(t, []string{"first_name", "last_name"}, tbl.Columns())
	fc := tbl.FieldNamesPositions()
	assert.Equal(t, 1, fc["last_name"])

	// should ignore 2nd repeat of first_name
	tbl.AddField(f)
	tbl.SetColumnsFromFields()
	assert.Equal(t, []string{"first_name", "last_name"}, tbl.Columns())
	assert.Equal(t, 2, len(tbl.FieldsAsMessages()))

	tbl.AddContext("hello", "world")
	assert.Equal(t, 1, len(tbl.Context))

	tbl.SetRows(nil)
	assert.Equal(t, 2, len(tbl.AsRows()))
	assert.Equal(t, 2, len(tbl.AsRows()))

	assert.NotEqual(t, nil, tbl.Body())
	assert.Equal(t, uint64(0), tbl.Id())
}
func TestFields(t *testing.T) {
	f := schema.NewFieldBase("Field", value.StringType, 64, "string")
	assert.NotEqual(t, nil, f)
	assert.Equal(t, "Field", f.Name)
	r := f.AsRow()
	assert.Equal(t, 9, len(r))
	r = f.AsRow()
	assert.Equal(t, 9, len(r))

	f.AddContext("hello", "world")
	assert.Equal(t, 1, len(f.Context))

	// NewField(name string, valType value.ValueType, size int, allowNulls bool, defaultVal driver.Value, key, collation, description string)
	f = schema.NewField("Field", value.StringType, 64, false, "world", "Key", "utf-8", "this is a description")
	r = f.AsRow()
	assert.Equal(t, 9, len(r))
	assert.Equal(t, value.StringType, f.ValueType())
	assert.NotEqual(t, nil, f.Body())
	assert.Equal(t, uint64(0), f.Id())
}
func TestConfig(t *testing.T) {
	c := schema.NewSourceConfig("test", "test")
	assert.NotEqual(t, nil, c)
	assert.NotEqual(t, "", c.String())
}
