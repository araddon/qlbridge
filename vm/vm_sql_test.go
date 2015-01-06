package vm

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"
	"github.com/bmizerany/assert"
)

var (
	_    = u.EMPTY
	rows = []ContextReader{NewContextSimpleTs(
		map[string]value.Value{
			"int5":       value.NewIntValue(5),
			"item_count": value.NewStringValue("5"),
			"bval":       value.NewBoolValue(true),
			"bvalf":      value.NewBoolValue(false),
			"reg_date":   value.NewStringValue("2014/11/01"),
			"user_id":    value.NewStringValue("abc")},
		time.Now(),
	)}
)

func TestSqlSelectEval(t *testing.T) {
	wc := verifySql(t, `select 
		user_id
		, item_count * 2 as itemsx2
		, yy(reg_date) > 10 as regyy
		, 'literal' as lit_val 
	FROM stdio`, rows)
	wcAll := wc.All()
	assert.Tf(t, len(wcAll) == 4, "must have 4 fields: %v", wcAll)

	userId, _ := wc.Get("user_id")
	assert.Tf(t, userId.Value().(string) == "abc", "Should not have nil result: %v", userId)
	itemCount, _ := wc.Get("itemsx2")
	assert.Tf(t, itemCount.Value().(float64) == float64(10), "Should not have nil result: %T:%v", itemCount, itemCount)
	regYy, _ := wc.Get("regyy")
	//u.Infof("%v %T", regYy, regYy)
	assert.Tf(t, regYy.Value().(bool) == true, "Should not have nil result: %T:%v", regYy, regYy)

	litVal, _ := wc.Get("lit_val")
	assert.Tf(t, litVal.ToString() == "literal", "Should process literal values %v", litVal)
}

func TestSqlSelectVmResults(t *testing.T) {

	wc := verifySql(t, `
		select 
			user_id 
		FROM stdio
		WHERE
			yy(reg_date) > 10 and bval == true`, rows)

	// should not be filtered out
	assert.Tf(t, len(wc.Data) == 1, "must have 1 col: %v  %v", len(wc.Data), wc.Data)

	// Test literal values
	wc = verifySql(t, `
		select 
			user_id,
			'intertubes' AS great
		FROM stdio`, rows)

	// have two cols
	assert.Tf(t, len(wc.Data) == 2, "must have 2 cols: %v  %v", len(wc.Data), wc.Data)
	assert.Tf(t, wc.Data["great"].ToString() == "intertubes", "must have literal val: %v  %v", len(wc.Data), wc.Data)

	wc = verifySql(t, `
		select 
			user_id 
		FROM stdio
		WHERE
			bval == false`, rows)

	assert.Tf(t, len(wc.Data) == 0, "must have 0 rows: %v  %v", len(wc.Data), wc.Data)

	wc = verifySql(t, `
		select 
			user_id 
		FROM stdio
		WHERE
			yy(reg_date) <= 10 and bval == true`, rows)

	assert.Tf(t, len(wc.Data) == 0, "must have 0 rows: %v  %v", len(wc.Data), wc.Data)

	wc = verifySql(t, `
		select 
			user_id 
		FROM stdio
		WHERE
			yy(reg_date) == 14 and !(bval == false)`, rows)

	assert.Tf(t, len(wc.Data) == 1, "must have 1 rows: %v  %v", len(wc.Data), wc.Data)
}

func TestSqlSelectStarVm(t *testing.T) {

	wc := verifySql(t, `
		select 
			* 
		FROM stdio
		WHERE
			yy(reg_date) > 10 and bval == true`, rows)

	row := wc.Row()
	// should not be filtered out
	assert.Tf(t, len(row) == 6, "must have 1 rows: %v  %v", len(row), row)
}

func TestSqlInsert(t *testing.T) {

	wc := verifySqlWrite(t, `
		insert into mytable (a,b,c) 
		VALUES
			('a1','b1', 3),
			('a2','b2', 3)
		`)

	rows := wc.Rows
	assert.Tf(t, len(rows) == 2, "must have 3 rows: %v  %v", len(rows), rows)
}

func TestSqlDelete(t *testing.T) {

	db := NewContextSimple()
	user1 := map[string]value.Value{
		"user_id":    value.NewIntValue(5),
		"item_count": value.NewStringValue("5"),
		"bval":       value.NewBoolValue(true),
		"bvalf":      value.NewBoolValue(false),
		"reg_date":   value.NewStringValue("2014/11/01"),
		"name":       value.NewStringValue("bob")}
	db.Insert(user1)
	user2 := map[string]value.Value{
		"user_id":    value.NewIntValue(6),
		"item_count": value.NewStringValue("5"),
		"reg_date":   value.NewStringValue("2012/11/01"),
		"name":       value.NewStringValue("allison")}
	db.Insert(user2)
	assert.Tf(t, len(db.Rows) == 2, "has 2 users")
	verifySqlDelete(t, `
		DELETE FROM mytable
		WHERE yy(reg_date) == 14
		`, db)

	assert.Tf(t, len(db.Rows) == 1, "must have 1 rows: %v  %v", len(db.Rows), db.Rows)
	assert.Tf(t, db.Rows[0]["name"].ToString() == "allison", "%v", db.Rows)
}

func verifySql(t *testing.T, sql string, readrows []ContextReader) *ContextSimple {

	sqlVm, err := NewSqlVm(sql)
	assert.Tf(t, err == nil, "Should not err %v", err)
	assert.Tf(t, sqlVm != nil, "Should create vm & parse sql %v", sqlVm)

	writeContext := NewContextSimple()
	for _, row := range readrows {

		err = sqlVm.Execute(writeContext, row)
		assert.Tf(t, err == nil, "non nil err: %v", err)
	}

	return writeContext
}

func verifySqlWrite(t *testing.T, sql string) *ContextSimple {

	sqlVm, err := NewSqlVm(sql)
	assert.Tf(t, err == nil, "Should not err %v", err)
	assert.Tf(t, sqlVm != nil, "Should create vm & parse sql %v", sqlVm)

	writeContext := NewContextSimple()

	err = sqlVm.ExecuteInsert(writeContext)
	assert.Tf(t, err == nil, "non nil err: %v", err)

	return writeContext
}

func verifySqlDelete(t *testing.T, sql string, source *ContextSimple) {

	sqlVm, err := NewSqlVm(sql)
	assert.Tf(t, err == nil, "Should not err %v", err)
	assert.Tf(t, sqlVm != nil, "Should create vm & parse sql %v", sqlVm)

	err = sqlVm.ExecuteDelete(source, source)
	assert.Tf(t, err == nil, "non nil err: %v", err)

}
