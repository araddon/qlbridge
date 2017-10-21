package schema_test

import (
	"database/sql/driver"
	"sort"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
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
	//s.AddChildSchema(childSchema)
	//s.RefreshSchema()
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
