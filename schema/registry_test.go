package schema_test

import (
	"database/sql/driver"
	"sort"
	"testing"
	"time"

	"github.com/araddon/qlbridge/lex"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

var _ = u.EMPTY

func init() {
	testutil.Setup()
}

func TestRegistry(t *testing.T) {

	reg := schema.DefaultRegistry()
	reg.Init()

	created, _ := dateparse.ParseAny("2015/07/04")

	inrow := []driver.Value{122, "bob", "bob@email.com", created.In(time.UTC).Add(time.Hour * -24), []string{"not_admin"}}

	db, err := memdb.NewMemDbData("memdb_users", [][]driver.Value{inrow}, []string{"user_id", "name", "email", "created", "roles"})
	assert.Equal(t, nil, err)

	c, err := db.Open("memdb_users")
	assert.Equal(t, nil, err)
	dc, ok := c.(schema.ConnAll)
	assert.True(t, ok)

	_, err = dc.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "email@email.com", created.In(time.UTC), []string{"admin"}})
	assert.Equal(t, nil, err)

	// We need to register our DataSource provider here
	err = schema.RegisterSourceAsSchema("memdb_reg_test", db)
	assert.Equal(t, nil, err)

	// Repeating this will cause error, dupe schema
	err = schema.RegisterSourceAsSchema("memdb_reg_test", db)
	assert.NotEqual(t, nil, err)

	reg.SchemaRefresh("memdb_reg_test")

	c2, err := schema.OpenConn("memdb_reg_test", "memdb_users")
	assert.Equal(t, nil, err)
	_, ok = c2.(schema.ConnAll)
	assert.True(t, ok)
	_, err = schema.OpenConn("invalid_schema", "memdb_users")
	assert.NotEqual(t, nil, err)

	sl := reg.Schemas()
	sort.Strings(sl)
	assert.Equal(t, []string{"memdb_reg_test", "mockcsv"}, sl)
	assert.NotEqual(t, "", reg.String())

	schema.RegisterSourceType("alias_to_memdb", db)
	c, err = reg.GetSource("alias_to_memdb")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, c)

	c, err = reg.GetSource("fake_not_real")
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, c)

	s := schema.NewSchema("hello-world")
	s.DS = db
	err = schema.RegisterSchema(s)
	assert.Equal(t, nil, err)
	err = schema.RegisterSchema(s)
	assert.NotEqual(t, nil, err)

	err = reg.SchemaAddChild("not.valid.schema", s)
	assert.NotEqual(t, nil, err)

	// test did panic bc nil source
	dp := didPanic(func() {
		schema.RegisterSourceType("nil_source", nil)
	})
	assert.Equal(t, true, dp)
	// dupe causes panic
	dp = didPanic(func() {
		schema.RegisterSourceType("alias_to_memdb", db)
	})
	assert.Equal(t, true, dp)

	rs, _ := reg.Schema(td.MockSchema.Name)
	assert.NotEqual(t, nil, rs)
	assert.Equal(t, 2, len(rs.Tables()))

	// Load in a "csv file" into our mock data store
	mockcsv.CreateCsvTable(td.MockSchema.Name, "droptable1", `user_id,t1
		9Ip1aKbeZe2njCDM,"2014-01-01"`)

	rs, _ = reg.Schema(td.MockSchema.Name)
	assert.NotEqual(t, nil, rs)
	assert.Equal(t, 3, len(rs.Tables()))

	err = reg.SchemaDrop(td.MockSchema.Name, "droptable1", lex.TokenTable)
	assert.Equal(t, nil, err)

	err = reg.SchemaDrop("bad.schema", "droptable1", lex.TokenTable)
	assert.NotEqual(t, nil, err)

	err = reg.SchemaDrop("bad.schema", "bad.schema", lex.TokenSchema)
	assert.NotEqual(t, nil, err)

	err = reg.SchemaDrop(td.MockSchema.Name, "not.a.table", lex.TokenTable)
	assert.NotEqual(t, nil, err)

	err = reg.SchemaDrop(td.MockSchema.Name, "not.a.table", lex.TokenAnd)
	assert.NotEqual(t, nil, err)

	rs, _ = reg.Schema(td.MockSchema.Name)
	assert.NotEqual(t, nil, rs)
	assert.Equal(t, 2, len(rs.Tables()))

	reg.Init()
}
func didPanic(f func()) (dp bool) {
	defer func() {
		if r := recover(); r != nil {
			dp = true
		}
	}()
	f()
	return dp
}
