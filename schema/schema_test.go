package schema_test

import (
	"database/sql/driver"
	"sort"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

var _ = u.EMPTY

func TestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}

func TestRegisterSchema(t *testing.T) {

	inrow := []driver.Value{122, "bob", "bob@email.com"}

	cols := []string{"user_id", "name", "email"}
	db, err := memdb.NewMemDbData("users", [][]driver.Value{inrow}, cols)
	assert.Equal(t, nil, err)
	db.Init()
	db.Setup(nil)

	c, err := db.Open("users")
	assert.Equal(t, nil, err)
	_, ok := c.(schema.ConnAll)
	assert.True(t, ok)

	err = datasource.RegisterSourceAsSchema("user_csv", db)
	assert.Equal(t, nil, err)

	s, ok := datasource.DataSourcesRegistry().Schema("user_csv")
	assert.Equal(t, true, ok)
	assert.NotEqual(t, nil, s)

	inrow2 := []driver.Value{122, "bob", "bob@email.com"}
	cols2 := []string{"account_id", "name", "email"}
	db2, err := memdb.NewMemDbData("accounts", [][]driver.Value{inrow2}, cols2)
	assert.Equal(t, nil, err)
	db.Init()
	db.Setup(nil)
	childSchema := schema.NewSchema("user_child")
	childSchema.DS = db2
	s.AddChildSchema(childSchema)
	s.RefreshSchema()
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
