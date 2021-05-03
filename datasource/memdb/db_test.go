package memdb

import (
	"database/sql/driver"
	"os"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/schema"
	"github.com/lytics/qlbridge/testutil"
)

func TestMain(m *testing.M) {
	testutil.Setup() // will call flag.Parse()

	// Now run the actual Tests
	os.Exit(m.Run())
}

func TestMemDb(t *testing.T) {

	created := dateparse.MustParse("2015/07/04")
	inrow := []driver.Value{122, "bob", "bob@email.com", created.In(time.UTC).Add(time.Hour * -24), []string{"not_admin"}}

	cols := []string{"user_id", "name", "email", "created", "roles"}
	db, err := NewMemDbData("users", [][]driver.Value{inrow}, cols)
	assert.Equal(t, nil, err)
	db.Init()
	db.Setup(nil)

	c, err := db.Open("users")
	assert.Equal(t, nil, err)
	dc, ok := c.(schema.ConnAll)
	assert.True(t, ok)

	dc.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "email@email.com", created.In(time.UTC), []string{"admin"}})
	row, err := dc.Get(123)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, row)
	di, ok := row.(*datasource.SqlDriverMessage)
	assert.True(t, ok)
	vals := di.Vals
	assert.Equal(t, 5, len(vals), "want 5 cols in user but got %v", len(vals))
	assert.Equal(t, 123, vals[0].(int))
	assert.Equal(t, "email@email.com", vals[2].(string))

	_, err = dc.Put(nil, &datasource.KeyInt{Id: 225}, []driver.Value{})
	assert.NotEqual(t, nil, err)

	dc.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "aaron@email.com", created.In(time.UTC), []string{"root", "admin"}})
	row, _ = dc.Get(123)
	assert.NotEqual(t, nil, row)
	vals2 := row.Body().([]driver.Value)

	assert.Equal(t, "aaron@email.com", vals2[2].(string))
	assert.Equal(t, []string{"root", "admin"}, vals2[4], "Roles should match updated vals")
	assert.Equal(t, created, vals2[3], "created date should match updated vals")

	ct := 0
	for {
		msg := dc.Next()
		if msg == nil {
			break
		}
		ct++
	}
	assert.Equal(t, 2, ct)
	err = dc.Close()
	assert.Equal(t, nil, err)

	// Schema
	tbl, err := db.Table("users")
	assert.Equal(t, nil, err)
	assert.Equal(t, cols, tbl.Columns())
	assert.Equal(t, []string{"users"}, db.Tables())

	// error testing
	_, err = NewMemDbData("users", [][]driver.Value{inrow}, nil)
	assert.NotEqual(t, nil, err)

	exprNode := expr.MustParse(`email == "bob@email.com"`)

	delCt, err := dc.DeleteExpression(nil, exprNode)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, delCt)

	delCt, err = dc.Delete(driver.Value(123))
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, delCt)

	key := datasource.KeyInt{Id: 123}
	_, err = dc.PutMulti(nil, []schema.Key{&key}, [][]driver.Value{{123, "aaron", "aaron@email.com", created.In(time.UTC), []string{"root", "admin"}}})
	assert.Equal(t, nil, err)

	err = db.Close()
	assert.Equal(t, nil, err)

	// Make sure we can cancel/stop
	c2, err := db.Open("users")
	assert.Equal(t, nil, err)
	dc2, ok := c2.(schema.ConnAll)

	ct = 0
	for {
		msg := dc2.Next()
		if msg == nil {
			break
		}
		ct++
	}
	assert.Equal(t, 0, ct)
}
