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
		u.Infof("row:  %v", msg.Body())
		vals, ok := msg.Body().([]driver.Value)
		assert.T(t, ok)
		assert.Tf(t, len(vals) == 1, "should have one row")
		assert.Tf(t, vals[0].(int) == 12345, "Should be 12345: %v", vals[0])
	}
	assert.Tf(t, iterCt == 1, "should have 1 rows: %v", iterCt)

	row := static.Get(12345)
	assert.Tf(t, row != nil, "Should find row")
	vals := row.Body().([]driver.Value)
	assert.Tf(t, len(vals) == 1 && vals[0].(int) == 12345, "must implement seeker")

	assert.Tf(t, len(static.data) == 1, "has 1 row")

	// Test Upsert() interface
	static.Put([]driver.Value{12346})
	assert.Tf(t, len(static.data) == 2, "has 2 row")

	row = static.Get(12346)
	assert.Tf(t, row != nil, "Should find row")
	vals = row.Body().([]driver.Value)
	assert.Tf(t, len(vals) == 1 && vals[0].(int) == 12346, "must implement seeker")
}
