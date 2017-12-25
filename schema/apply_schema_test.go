package schema_test

import (
	"database/sql/driver"
	"testing"

	"github.com/araddon/qlbridge/datasource"

	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"
	"github.com/stretchr/testify/assert"
)

func fakeReplicator(cmd *schema.Command) error {
	return nil
}
func applyTest(a schema.Applyer) func(*testing.T) {
	return func(t *testing.T) {
		inrow := []driver.Value{122, "bob", "bob@email.com"}
		db, err := memdb.NewMemDbData("users", [][]driver.Value{inrow}, []string{"user_id", "name", "email"})
		assert.Equal(t, nil, err)

		s := schema.NewSchema("hello")
		s.DS = db
		err = a.Apply(schema.Command_AddUpdate, s, s)
		assert.Equal(t, nil, err)

		err = a.Apply(schema.Command_AddUpdate, s, "not_real")
		assert.NotEqual(t, nil, err)

		err = a.Apply(schema.Command_Drop, s, s)
		assert.Equal(t, nil, err)

		err = a.Apply(schema.Command_Drop, s, "fake")
		assert.NotEqual(t, nil, err)
	}
}
func TestApplySchema(t *testing.T) {
	a := schema.NewApplyer(func(s *schema.Schema) schema.Source {
		sdb := datasource.NewSchemaDb(s)
		s.InfoSchema.DS = sdb
		return sdb
	})
	reg := schema.NewRegistry(a)
	a.Init(reg, nil)

	t.Run("Applyer 1", applyTest(a))

	a = schema.NewApplyer(func(s *schema.Schema) schema.Source {
		sdb := datasource.NewSchemaDb(s)
		s.InfoSchema.DS = sdb
		return sdb
	})
	reg = schema.NewRegistry(a)
	a.Init(reg, fakeReplicator)

	t.Run("Applyer replicator", applyTest(a))
}
