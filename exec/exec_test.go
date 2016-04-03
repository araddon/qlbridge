package exec_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

func init() {
	testutil.Setup()
}
func TestStatements(t *testing.T) {

	// - yy func evaluates
	// - projection (user_id, email)
	testutil.TestSelect(t, `select user_id, email FROM users WHERE yy(reg_date) > 10;`,
		[][]driver.Value{{"9Ip1aKbeZe2njCDM", "aaron@email.com"}},
	)
	// - ensure we can evaluate against "NULL"
	// - extra paren in where
	// - `db`.`col` syntax
	testutil.TestSelect(t, "SELECT user_id FROM users WHERE (`users.user_id` != NULL)",
		[][]driver.Value{{"hT2impsabc345c"}, {"9Ip1aKbeZe2njCDM"}, {"hT2impsOPUREcVPc"}},
	)
	testutil.TestSelect(t, "SELECT email FROM users WHERE interests != NULL)",
		[][]driver.Value{{"aaron@email.com"}, {"bob@email.com"}},
	)

	return
	// TODO:

	testutil.TestSelect(t, "SELECT COUNT(*) AS count FROM users WHERE (`users.user_id` != NULL)",
		[][]driver.Value{{3}},
	)
	// requires aggregations, note also lack of group-by
	testutil.TestSelect(t, "SELECT AVG(CHAR_LENGTH(CAST(`user`.`email` AS CHAR))) AS `len` FROM `users`",
		[][]driver.Value{{14.5}},
	)
}

func TestExecSqlSet(t *testing.T) {
	sqlText := `
		set @myvarname = "var value"
	`
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 0, "should not have messages %v", len(msgs))
	//u.Debugf("msg: %#v", msgs[0])

	ctx2 := td.TestContext(`SELECT 3, @myvarname;`)
	ctx2.Session = ctx.Session
	job, err = exec.BuildSqlJob(ctx2)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs = make([]schema.Message, 0)
	resultWriter = exec.NewResultBuffer(ctx2, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have 1 messages %v", len(msgs))
	msg := msgs[0].Body().(*datasource.SqlDriverMessageMap)

	assert.Tf(t, msg.Vals[0] == int64(3), "Has 3?")
	assert.Tf(t, msg.Vals[1] == "var value", "Has variable value? %v", msg.Vals[1])
}

func TestExecSelectWhere(t *testing.T) {
	sqlText := `
		select 
	        user_id, email, referral_count * 2, 5, yy(reg_date) > 10
	    FROM users
	    WHERE yy(reg_date) > 10 
	`
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered out 2 messages %v", len(msgs))
	//u.Debugf("msg: %#v", msgs[0])
	row := msgs[0].(*datasource.SqlDriverMessageMap).Values()
	//u.Debugf("row: %#v", row)
	assert.Tf(t, len(row) == 5, "expects 5 cols but got %v", len(row))
	assert.T(t, row[0] == "9Ip1aKbeZe2njCDM")
	// I really don't like this float behavior?
	assert.Tf(t, int(row[2].(float64)) == 164, "expected %v == 164  T:%T", row[2], row[2])
	assert.Tf(t, row[3] == int64(5), "wanted 5 got %v  T:%T", row[3], row[3])
	assert.T(t, row[4] == true)
}

func TestExecGroupBy(t *testing.T) {
	// TODO:  this test is bad, it occasionally fails
	sqlText := `
		select 
	        user_id, count(user_id), avg(price)
	    FROM orders
	    GROUP BY user_id
	`
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 2, "should have grouped orders into 2 users %v", len(msgs))
	u.Debugf("msg: %#v", msgs[0])
	row := msgs[0].(*datasource.SqlDriverMessageMap).Values()
	u.Debugf("row: %#v", row)
	assert.Tf(t, len(row) == 3, "expects 3 cols but got %v", len(row))
	assert.T(t, row[0] == "9Ip1aKbeZe2njCDM", "%#v", row)
	// I really don't like this float behavior?
	assert.Tf(t, int(row[1].(int64)) == 2, "expected 2 orders for %v", row)
	assert.Tf(t, int(row[2].(float64)) == 30, "expected avg=30 for price %v", row)

	sqlText = `
		select 
	        avg(len(email))
	    FROM users
	    GROUP BY "-"
	`
	ctx = td.TestContext(sqlText)
	job, err = exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs = make([]schema.Message, 0)
	resultWriter = exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have grouped orders into 1 record %v", len(msgs))
	u.Debugf("msg: %#v", msgs[0])
	row = msgs[0].(*datasource.SqlDriverMessageMap).Values()
	u.Debugf("row: %#v", row)
	assert.Tf(t, len(row) == 1, "expects 1 cols but got %v", len(row))
	assert.Tf(t, int(row[0].(float64)) == 13, "expected avg(len(email))=15 for %v", int(row[0].(float64)))
}

func TestExecHaving(t *testing.T) {
	sqlText := `
		select 
	        user_id, count(user_id) AS order_ct
	    FROM orders
	    GROUP BY user_id
	    HAVING order_ct > 1
	`
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered HAVING orders into 1 users %v", len(msgs))
	u.Debugf("msg: %#v", msgs[0])
	row := msgs[0].(*datasource.SqlDriverMessageMap).Values()
	u.Debugf("row: %#v", row)
	assert.Tf(t, len(row) == 2, "expects 2 cols but got %v", len(row))
	assert.T(t, row[0] == "9Ip1aKbeZe2njCDM")
	// I really don't like this float behavior?
	assert.Tf(t, int(row[1].(int64)) == 2, "expected 2 orders for %v", row)
}

type UserEvent struct {
	Id     string
	UserId string
	Event  string
	Date   time.Time
}

func TestExecInsert(t *testing.T) {

	//mockSchema, _ = registry.Schema("mockcsv")

	// By "Loading" table we force it to exist in this non DDL mock store
	mockcsv.LoadTable("user_event", "id,user_id,event,date\n1,abcabcabc,signup,\"2012-12-24T17:29:39.738Z\"")

	//u.Infof("%p schema", mockSchema)
	td.TestContext("select * from user_event")

	db, err := datasource.OpenConn("mockcsv", "user_event")
	assert.Tf(t, err == nil, "%v", err)
	dbTable, ok := db.(*mockcsv.MockCsvTable)
	assert.Tf(t, ok, "Should be type StaticDataSource but was T %T", db)
	assert.Tf(t, dbTable.Length() == 1, "Should have inserted 1 but was %v", dbTable.Length())

	sqlText := `
		INSERT into user_event (id, user_id, event, date)
		VALUES
			(uuid(), "9Ip1aKbeZe2njCDM", "logon", now())
	`
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "%v", err)

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	//u.Infof("running tasks?  %v", len(job.RootTask.Children()))
	err = job.Run()
	assert.T(t, err == nil)
	db2, err := datasource.OpenConn("mockcsv", "user_event")
	assert.Tf(t, err == nil, "%v", err)
	dbTable2, ok := db2.(*mockcsv.MockCsvTable)
	assert.Tf(t, ok, "Should be type StaticDataSource but was T %T", db2)
	//u.Infof("db:  %#v", dbTable2)
	assert.Tf(t, dbTable2.Length() == 2, "Should have inserted 2 but was %v", dbTable2.Length())

	// Now lets query it, we are going to use QLBridge Driver
	sqlText = `
		select id, user_id, event, date
	    FROM user_event
	    WHERE user_id = "9Ip1aKbeZe2njCDM"
	`
	sqlDb, err := sql.Open("qlbridge", "mockcsv")
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, sqlDb != nil, "has conn: %v", sqlDb)
	defer func() { sqlDb.Close() }()

	rows, err := sqlDb.Query(sqlText)
	assert.Tf(t, err == nil, "error: %v", err)
	defer rows.Close()
	assert.Tf(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 4, "4 cols: %v", cols)
	events := make([]*UserEvent, 0)
	for rows.Next() {
		var ue UserEvent
		err = rows.Scan(&ue.Id, &ue.UserId, &ue.Event, &ue.Date)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("events=%+v", ue)
		events = append(events, &ue)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(events) == 1, "has 1 event row: %+v", events)

	ue1 := events[0]
	assert.T(t, ue1.Event == "logon")
	assert.T(t, ue1.UserId == "9Ip1aKbeZe2njCDM")

	sqlText = `
		INSERT into user_event (id, user_id, event, date)
		VALUES
			(uuid(), "9Ip1aKbeZe2njCDM", "logon", now())
			, (uuid(), "9Ip1aKbeZe2njCDM", "click", now())
			, (uuid(), "abcd", "logon", now())
			, (uuid(), "abcd", "click", now())
	`
	result, err := sqlDb.Exec(sqlText)
	assert.Tf(t, err == nil, "error: %v", err)
	assert.Tf(t, result != nil, "has results: %v", result)
	insertedCt, err := result.RowsAffected()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, insertedCt == 4, "should have inserted 4 but was %v", insertedCt)
	assert.Tf(t, dbTable.Length() == 6, "should have 6 rows now")
	// TODO:  this doesn't work
	// row := sqlDb.QueryRow("SELECT count(*) from user_event")
	// assert.Tf(t, err == nil, "count(*) shouldnt error: %v", err)
	// var rowCt int
	// row.Scan(&rowCt)
	// assert.Tf(t, rowCt == 6, "has rowct=6: %v", rowCt)
}

func TestExecUpdateAndUpsert(t *testing.T) {

	// By "Loading" table we force it to exist in this non DDL mock store
	mockcsv.LoadTable("user_event3", "id,user_id,event,date\n1,abcabcabc,signup,\"2012-12-24T17:29:39.738Z\"")
	dbPre, err := datasource.OpenConn("mockcsv", "user_event3")
	assert.Tf(t, err == nil, "%v", err)
	dbTablePre, ok := dbPre.(*mockcsv.MockCsvTable)
	assert.Tf(t, ok, "Should be type MockCsvTable but was T:%T", dbTablePre)
	//u.Infof("db:  %#v", dbTable)
	assert.Tf(t, dbTablePre.Length() == 1, "Should have inserted and have 1 but was %v", dbTablePre.Length())

	sqlText := `
		UPSERT into user_event3 (id, user_id, event, date)
		VALUES
			("1234abcd", "9Ip1aKbeZe2njCDM", "logon", todate("2012/07/07"))
	`
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "%v", err)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	assert.T(t, err == nil)

	db, err := datasource.OpenConn("mockcsv", "user_event3")
	assert.Tf(t, err == nil, "%v", err)
	dbTable, ok := db.(*mockcsv.MockCsvTable)
	assert.T(t, ok, "Should be type MockCsvTable ", dbTable)
	//u.Infof("db:  %#v", dbTable)
	assert.Tf(t, dbTable.Length() == 2, "Should have inserted and have 2 but was %v", dbTable.Length())

	// Now we are going to upsert the same row with changes
	sqlText = `
		UPSERT into user_event3 (id, user_id, event, date)
		VALUES
			("1234abcd", "9Ip1aKbeZe2njCDM", "logon", todate("2013/07/07"))
	`
	ctx = td.TestContext(sqlText)
	job, err = exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "%v", err)
	job.Setup()
	err = job.Run()
	assert.T(t, err == nil)

	// Should not have inserted, due to id being same
	assert.Tf(t, dbTable.Length() == 2, "Should have inserted and have 2 but was %v", dbTable.Length())

	// Now lets query it, we are going to use QLBridge Driver
	sqlSelect1 := `
		select id, user_id, event, date
	    FROM user_event3
	    WHERE user_id = "9Ip1aKbeZe2njCDM"
    `
	sqlDb, err := sql.Open("qlbridge", "mockcsv")
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, sqlDb != nil, "has conn: %v", sqlDb)
	defer func() { sqlDb.Close() }()

	rows, err := sqlDb.Query(sqlSelect1)
	assert.Tf(t, err == nil, "error: %v", err)
	assert.Tf(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 4, "4 cols: %v", cols)
	events := make([]*UserEvent, 0)
	for rows.Next() {
		var ue UserEvent
		err = rows.Scan(&ue.Id, &ue.UserId, &ue.Event, &ue.Date)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("events=%+v", ue)
		events = append(events, &ue)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(events) == 1, "has 1 event row: %+v", events)

	ue1 := events[0]
	assert.T(t, ue1.Event == "logon")
	assert.T(t, ue1.UserId == "9Ip1aKbeZe2njCDM")
	assert.Tf(t, ue1.Date.Year() == 2013, "Upsert should have changed date")

	// Global Update on user_id
	sqlUpdate := `UPDATE user_event3 SET event = "fake" WHERE id = "1234abcd"`
	ctx = td.TestContext(sqlUpdate)
	job, err = exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "%v", err)
	job.Setup()
	err = job.Run()
	assert.T(t, err == nil)

	rows, err = sqlDb.Query(sqlSelect1)
	assert.Tf(t, err == nil, "no error: %v", err)
	events = make([]*UserEvent, 0)
	for rows.Next() {
		var ue UserEvent
		rows.Scan(&ue.Id, &ue.UserId, &ue.Event, &ue.Date)
		events = append(events, &ue)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(events) == 1, "has 1 event row: %+v", events)

	ue1 = events[0]
	assert.Tf(t, ue1.Event == "fake", "%+v", ue1)
	assert.T(t, ue1.UserId == "9Ip1aKbeZe2njCDM")
	assert.Tf(t, ue1.Date.Year() == 2013, "Upsert should have changed date")
}

func TestExecDelete(t *testing.T) {

	// By "Loading" table we force it to exist in this non DDL mock store
	mockcsv.LoadTable("user_event2",
		"id,user_id,event,date\n1,abcd,signup,\"2012-12-24T17:29:39.738Z\"")
	sqlText := `
		INSERT into user_event2 (id, user_id, event, date)
		VALUES
			(uuid(), "9Ip1aKbeZe2njCDM", "logon", now())
			, (uuid(), "9Ip1aKbeZe2njCDM", "click", now())
			, (uuid(), "abcd", "logon", now())
			, (uuid(), "abcd", "click", now())
	`

	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.T(t, err == nil, "build job failed ", err)
	job.Setup()
	err = job.Run()
	assert.T(t, err == nil)

	db, err := datasource.OpenConn("mockcsv", "user_event2")
	assert.Tf(t, err == nil, "%v", err)
	userEvt2, ok := db.(*mockcsv.MockCsvTable)
	assert.Tf(t, ok, "Should be type StaticDataSource %p  %v", userEvt2, userEvt2)
	//u.Warnf("how many?  %v", userEvt2.Length())
	assert.Tf(t, userEvt2.Length() == 5, "Should have inserted 4, for 5 total rows but %p has: %d", userEvt2, userEvt2.Length())

	/*
			dbTable, ok := db.(*mockcsv.MockCsvTable)
		assert.Tf(t, ok, "Should be type StaticDataSource but was T %T", db)
		assert.Tf(t, dbTable.Length() == 1, "Should have inserted 1 but was %v", dbTable.Length())
	*/
	// Now lets delete a few rows
	sqlText = `
		DELETE FROM user_event2
	    WHERE user_id = "abcd"
    `
	// This is a go sql/driver not same as qlbridge db above
	sqlDb, err := sql.Open("qlbridge", "mockcsv")
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, sqlDb != nil, "has conn: %v", sqlDb)
	defer func() { sqlDb.Close() }()

	result, err := sqlDb.Exec(sqlText)
	assert.Tf(t, err == nil, "error: %v", err)
	assert.Tf(t, result != nil, "has results: %v", result)
	delCt, err := result.RowsAffected()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, delCt == 3, "should have deleted 3 but was %v", delCt)
}

// sub-select not implemented in exec yet
func testSubselect(t *testing.T) {
	sqlText := `
		select 
	        user_id, email
	    FROM users
	    WHERE user_id in 
	        (select user_id from orders)
    `
	ctx := td.TestContext(sqlText)
	job, err := exec.BuildSqlJob(ctx)
	assert.Tf(t, err == nil, "no error %v", err)

	//writeCtx := NewContextSimple()
	msgs := make([]schema.Message, 0)
	go func() {
		outChan := job.DrainChan()
		for msg := range outChan {
			u.Infof("msg: %v", msg)
			msgs = append(msgs, msg)
		}
	}()
	err = job.Run()
	time.Sleep(time.Millisecond * 30)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered out 2 messages")
}
