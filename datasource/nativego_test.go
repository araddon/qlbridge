package datasource

import (
	"database/sql/driver"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
)

func init() {
	// Register our Datasources in registry
	Register("gonative", &StaticDataSource{})
}

func TestStaticDatasource(t *testing.T) {

	static := NewStaticDataValue(12345, "@@varname")

	iter := static.CreateIterator(nil)
	iterCt := 0
	u.Infof("static:  %v  len(data)=%v", static.cursor, len(static.data))
	for msg := iter.Next(); msg != nil; msg = iter.Next() {
		iterCt++
		u.Infof("row:  %#v", msg.Body())
		dm, ok := msg.Body().(*SqlDriverMessageMap)
		vals := dm.Values()
		assert.T(t, ok)
		assert.Tf(t, len(vals) == 1, "should have one row")
		assert.Tf(t, vals[0].(int) == 12345, "Should be 12345: %v", vals[0])
	}
	assert.Tf(t, iterCt == 1, "should have 1 rows: %v", iterCt)

	row, err := static.Get(12345)
	assert.Tf(t, err == nil, "%v", err)
	assert.Tf(t, row != nil, "Should find row")
	vals, ok := row.Body().([]driver.Value)
	assert.Tf(t, ok, "Must be []driver.Value type: %T", row.Body())
	assert.Tf(t, len(vals) == 1 && vals[0].(int) == 12345, "must implement seeker")

	assert.Tf(t, len(static.data) == 1, "has 1 row")

	// Test Upsert() interface
	static.Put([]driver.Value{12346})
	assert.Tf(t, len(static.data) == 2, "has 2 rows after Put()")

	row, _ = static.Get(12346)
	assert.Tf(t, row != nil, "Should find row with Get() part of Seeker interface")
	vals, ok = row.Body().([]driver.Value)
	assert.Tf(t, ok, "Must be []driver.Value type: %T", row.Body())
	assert.Tf(t, len(vals) == 1 && vals[0].(int) == 12346, "must implement seeker")

	static.Put([]driver.Value{12347})
	assert.Tf(t, len(static.data) == 3, "has 3 rows after Put()")

	rows, err := static.MultiGet([]driver.Value{12345, 12347})
	assert.Tf(t, err == nil, "%v", err)
	assert.Tf(t, rows != nil && len(rows) == 2, "Should find 2 rows with MultiGet() part of Seeker interface")
	vals = rows[0].Body().([]driver.Value)
	assert.Tf(t, len(vals) == 1 && vals[0].(int) == 12345, "must implement seeker")
	vals = rows[1].Body().([]driver.Value)
	assert.Tf(t, len(vals) == 1 && vals[0].(int) == 12347, "must implement seeker")

	delCt, err := static.Delete(12345)
	assert.T(t, err == nil)
	assert.T(t, delCt == 1)
	assert.T(t, len(static.data) == 2)
	row, err = static.Get(12345)
	assert.T(t, err == ErrNotFound)
	assert.T(t, row == nil)

	delCt, err = static.Delete(driver.Value(4444))
	assert.T(t, err == ErrNotFound)
	assert.T(t, delCt == 0)
}
