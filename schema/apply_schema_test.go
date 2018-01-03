package schema_test

import (
	"database/sql/driver"
	"testing"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
)

var (
	apply_cols = []string{"user_id", "name", "email"}
)

type distributedRepl struct {
	a  *schema.InMemApplyer
	a2 *schema.InMemApplyer
}

func (m *distributedRepl) fakeReplicator(cmd *schema.Command) error {
	u.Infof("running apply command")
	if err := m.a.ApplyCommand(cmd); err != nil {
		u.Warnf("Could not apply command %v", err)
		return err
	}
	return m.a2.ApplyCommand(cmd)
}

func applyTest(reg *schema.Registry, a schema.Applyer) func(*testing.T) {
	return func(t *testing.T) {
		inrow := []driver.Value{122, "bob", "bob@email.com"}
		db, err := memdb.NewMemDbData("users", [][]driver.Value{inrow}, apply_cols)
		assert.Equal(t, nil, err)

		s := schema.NewSchema("schema_apply_test")
		s.DS = db
		err = s.Discovery()
		assert.Equal(t, nil, err)

		err = a.Apply(schema.Command_AddUpdate, s, s)
		assert.Equal(t, nil, err)

		// Should error, can only apply *Table or *Schema
		err = a.Apply(schema.Command_AddUpdate, s, "not_real")
		assert.NotEqual(t, nil, err)

		err = a.Apply(schema.Command_Drop, s, "fake")
		assert.NotEqual(t, nil, err)

		sd := schema.NewSchema("schema_apply_drop_test")
		sd.DS = db
		err = a.Apply(schema.Command_AddUpdate, sd, sd)
		assert.Equal(t, nil, err)

		err = a.Apply(schema.Command_Drop, sd, sd)
		assert.Equal(t, nil, err)

		// must have dropped
		_, ok := reg.Schema("schema_apply_drop_test")
		assert.Equal(t, false, ok)

		// must have found
		s, ok = reg.Schema("schema_apply_test")
		assert.Equal(t, true, ok)

		u.Infof("%p found schema %#v", s, s)
	}
}
func verifySchema(reg *schema.Registry) func(*testing.T) {
	return func(t *testing.T) {
		u.Warnf("running verifySchema")
		s, ok := reg.Schema("schema_apply_test")
		assert.True(t, ok)
		assert.NotEqual(t, nil, s)

		tbl, err := s.Table("users")
		assert.Equal(t, nil, err, "What? %p %#v", s, s)
		assert.NotEqual(t, nil, tbl)
		if tbl == nil {
			u.Warnf("WTF no users")
			return
		}

		assert.Equal(t, apply_cols, tbl.Columns())
	}
}
func TestApplySchema(t *testing.T) {
	/*
		a := schema.NewApplyer(func(s *schema.Schema) schema.Source {
			sdb := datasource.NewSchemaDb(s)
			s.InfoSchema.DS = sdb
			return sdb
		})

			reg := schema.NewRegistry(a)
			a.Init(reg, nil)

			t.Run("Applyer in-mem", applyTest(reg, a))

			t.Run("Verify In-Mem schema", verifySchema(reg))
	*/
	ad1 := schema.NewApplyer(func(s *schema.Schema) schema.Source {
		sdb := datasource.NewSchemaDb(s)
		s.InfoSchema.DS = sdb
		return sdb
	})
	ad2 := schema.NewApplyer(func(s *schema.Schema) schema.Source {
		sdb := datasource.NewSchemaDb(s)
		s.InfoSchema.DS = sdb
		return sdb
	})

	regd1 := schema.NewRegistry(ad1)
	regd2 := schema.NewRegistry(ad2)

	dr := &distributedRepl{}
	dr.a = ad1.(*schema.InMemApplyer)
	dr.a2 = ad2.(*schema.InMemApplyer)

	ad1.Init(regd1, dr.fakeReplicator)
	ad2.Init(regd2, nil)

	u.Warnf("about to do distributed applyer test")

	t.Run("Applyer replicator", applyTest(regd1, ad1))

	t.Run("Verify In-Mem schema", verifySchema(regd1))

	return
	t.Run("Verify In-Mem schema", verifySchema(regd2))
}
