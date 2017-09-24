package memdb

import (
	"database/sql/driver"
	"flag"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
)

func init() {
	flag.Parse()
	if testing.Verbose() {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
}

func TestMemDb(t *testing.T) {

	created, _ := dateparse.ParseAny("2015/07/04")

	inrow := []driver.Value{122, "bob", "bob@email.com", created.In(time.UTC).Add(time.Hour * -24), []string{"not_admin"}}

	db, err := NewMemDbData("users", [][]driver.Value{inrow}, []string{"user_id", "name", "email", "created", "roles"})
	assert.True(t, err == nil, "wanted no error got %v", err)

	c, err := db.Open("users")
	assert.True(t, err == nil, "wanted no error got %v", err)
	dc, ok := c.(schema.ConnAll)
	assert.True(t, ok)

	dc.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "email@email.com", created.In(time.UTC), []string{"admin"}})
	row, err := dc.Get(123)
	assert.True(t, err == nil)
	assert.True(t, row != nil, "Should find row with Get() part of Seeker interface")
	di, ok := row.(*datasource.SqlDriverMessage)
	assert.True(t, ok, "Must be []driver.Value type: %T", row)
	vals := di.Vals
	assert.True(t, len(vals) == 5, "want 5 cols in user but got %v", len(vals))
	assert.True(t, vals[0].(int) == 123, "want user_id=123 but got %v", vals[0])
	assert.True(t, vals[2].(string) == "email@email.com", "want email=email@email.com but got %v", vals[2])

	dc.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "aaron@email.com", created.In(time.UTC), []string{"root", "admin"}})
	row, _ = dc.Get(123)
	assert.True(t, row != nil, "Should find row with Get() part of Seeker interface")
	vals2 := row.Body().([]driver.Value)

	assert.True(t, vals2[2].(string) == "aaron@email.com", "want email=email@email.com but got %v", vals2[2])
	assert.Equal(t, []string{"root", "admin"}, vals2[4], "Roles should match updated vals")
	assert.Equal(t, created, vals2[3], "created date should match updated vals")

	ic := c.(schema.ConnScannerIterator)
	it := ic.CreateIterator()
	ct := 0
	for {
		msg := it.Next()
		if msg == nil {
			break
		}
		ct++
	}
	assert.Equal(t, 2, ct)
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
}
