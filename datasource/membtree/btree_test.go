package membtree_test

import (
	"database/sql/driver"
	"os"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/datasource/membtree"
	"github.com/lytics/qlbridge/plan"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/schema"
	"github.com/lytics/qlbridge/testutil"
)

const (
	sourceType = "btree"
)

var (
	sch *schema.Schema
)

func TestMain(m *testing.M) {
	testutil.Setup() // will call flag.Parse()
	// Register our Datasources in registry
	schema.RegisterSourceType(sourceType, &membtree.StaticDataSource{})
	// Now run the actual Tests
	os.Exit(m.Run())
}

func planContext(query string) *plan.Context {
	ctx := plan.NewContext(query)
	ctx.DisableRecover = true
	ctx.Schema = sch
	ctx.Session = datasource.NewMySqlSessionVars()
	return ctx
}

func TestStaticValues(t *testing.T) {

	static := membtree.NewStaticDataValue("@@varname", 12345)

	iter := static.CreateIterator()
	iterCt := 0

	for msg := iter.Next(); msg != nil; msg = iter.Next() {
		iterCt++
		u.Infof("row:  %#v", msg.Body())
		dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
		vals := dm.Values()
		assert.True(t, ok)
		assert.Equal(t, 1, len(vals), "should have one row")
		assert.Equal(t, 12345, vals[0].(int), "Should be 12345: %v", vals[0])
	}
	assert.Equal(t, 1, iterCt, "should have 1 rows: %v", iterCt)

	row, err := static.Get(12345)
	assert.Equal(t, nil, err)
	assert.True(t, row != nil, "Should find row")
	di, ok := row.Body().(*datasource.SqlDriverMessageMap)
	assert.True(t, ok, "Must be *SqlDriverMessageMap but was type: %T", row.Body())
	vals := di.Values()
	assert.True(t, len(vals) == 1 && vals[0].(int) == 12345, "must implement seeker")
	assert.True(t, static.Length() == 1, "has 1 row")

	// Test Upsert() interface
	static.Put(nil, &datasource.KeyInt{Id: 123456}, []driver.Value{12346})
	assert.True(t, static.Length() == 2, "has 2 rows after Put()")

	row, _ = static.Get(12346)
	assert.True(t, row != nil, "Should find row with Get() part of Seeker interface")
	di, ok = row.Body().(*datasource.SqlDriverMessageMap)
	assert.True(t, ok, "Must be []driver.Value type: %T", row.Body())
	vals = di.Values()
	assert.True(t, len(vals) == 1 && vals[0].(int) == 12346, "must implement seeker")

	static.Put(nil, nil, []driver.Value{12347})
	assert.True(t, static.Length() == 3, "has 3 rows after Put()")

	rows, err := static.MultiGet([]driver.Value{12345, 12347})
	assert.Equal(t, nil, err)
	assert.True(t, rows != nil && len(rows) == 2, "Should find 2 rows with MultiGet() part of Seeker interface")
	vals = rows[0].Body().(*datasource.SqlDriverMessageMap).Values()
	assert.True(t, len(vals) == 1 && vals[0].(int) == 12345, "must implement seeker")
	vals = rows[1].Body().(*datasource.SqlDriverMessageMap).Values()
	assert.True(t, len(vals) == 1 && vals[0].(int) == 12347, "must implement seeker")

	delCt, err := static.Delete(12345)
	assert.Equal(t, nil, err)
	assert.True(t, delCt == 1)
	assert.True(t, static.Length() == 2)
	row, err = static.Get(12345)
	assert.True(t, err == schema.ErrNotFound)
	assert.True(t, row == nil)

	delCt, err = static.Delete(driver.Value(4444))
	assert.True(t, err == schema.ErrNotFound)
	assert.True(t, delCt == 0)
}

func TestStaticDataSource(t *testing.T) {

	static := membtree.NewStaticDataSource("users", 0, nil, []string{"user_id", "name", "email", "created", "roles"})

	created, _ := dateparse.ParseAny("2015/07/04")
	static.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "email@email.com", created.In(time.UTC), []string{"admin"}})
	assert.Equal(t, 1, static.Length(), "has 1 rows after Put()")

	row, _ := static.Get(123)
	assert.True(t, row != nil, "Should find row with Get() part of Seeker interface")
	di, ok := row.Body().(*datasource.SqlDriverMessageMap)
	assert.True(t, ok, "Must be []driver.Value type: %T", row.Body())
	vals := di.Values()
	assert.True(t, len(vals) == 5, "want 5 cols in user but got %v", len(vals))
	assert.True(t, vals[0].(int) == 123, "want user_id=123 but got %v", vals[0])
	assert.True(t, vals[2].(string) == "email@email.com", "want email=email@email.com but got %v", vals[2])

	static.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "aaron@email.com", created.In(time.UTC), []string{"root", "admin"}})
	assert.True(t, static.Length() == 1, "has 1 rows after Put()")
	row, _ = static.Get(123)
	assert.True(t, row != nil, "Should find row with Get() part of Seeker interface")
	vals2 := row.Body().(*datasource.SqlDriverMessageMap).Values()

	assert.True(t, vals2[2].(string) == "aaron@email.com", "want email=email@email.com but got %v", vals2[2])
	assert.Equal(t, []string{"root", "admin"}, vals2[4], "Roles should match updated vals")
	assert.Equal(t, created, vals2[3], "created date should match updated vals")

	curSize := static.Length()

	err := schema.RegisterSourceAsSchema("btreetest", static)
	assert.Equal(t, nil, err)
	sch, ok = schema.DefaultRegistry().Schema("btreetest")
	assert.Equal(t, true, ok)

	ctx := planContext("DELETE from users WHERE EXISTS user_id;")

	stmt, err := rel.ParseSql(ctx.Raw)
	assert.Equal(t, nil, err, "Must parse but got %v", err)
	ctx.Stmt = stmt
	planner := plan.NewPlanner(ctx)
	pln, err := plan.WalkStmt(ctx, stmt, planner)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, pln, "must have plan")

	dp, ok := pln.(*plan.Delete)
	assert.True(t, ok)

	delCt, err := static.DeleteExpression(pln, dp.Stmt.Where.Expr)
	assert.Equal(t, nil, err)
	assert.Equal(t, curSize, delCt, "Should have deleted all records")
}
