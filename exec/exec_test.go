package exec

import (
	"database/sql"
	"flag"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	"github.com/araddon/qlbridge/expr/builtins"
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
	_                  = u.EMPTY
	loadData     sync.Once
)

func LoadTestDataOnce() {
	loadData.Do(func() {
		// Load in a "csv file" into our mock data store
		mockcsv.LoadTable("users", `user_id,email,interests,reg_date,referral_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",82
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`)

		mockcsv.LoadTable("orders", `order_id,user_id,item_id,price,order_date,item_count
1,9Ip1aKbeZe2njCDM,1,22.50,"2012-12-24T17:29:39.738Z",82
2,9Ip1aKbeZe2njCDM,2,37.50,"2013-10-24T17:29:39.738Z",82
3,abcabcabc,1,22.50,"2013-10-24T17:29:39.738Z",82
`)
	})

}
func init() {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
	} else if testing.Verbose() {
		u.SetupLogging("info")
	} else {
		u.SetupLogging("warn")
	}
	u.SetColorOutput()

	LoadTestDataOnce()

	builtins.LoadAllBuiltins()
}

func TestEngineWhere(t *testing.T) {
	sqlText := `
		select 
	        user_id, email, referral_count * 2, yy(reg_date) > 10
	    FROM users
	    WHERE yy(reg_date) > 10 
	`
	job, err := BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs := make([]datasource.Message, 0)
	resultWriter := NewResultBuffer(&msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	time.Sleep(time.Millisecond * 10)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered out 2 messages %v", len(msgs))
}

type UserEvent struct {
	Id     string
	UserId string
	Event  string
	Date   time.Time
}

func TestEngineInsert(t *testing.T) {

	// By "Loading" table we force it to exist in this non DDL mock store
	mockcsv.LoadTable("user_event", "id,user_id,event,date\n1,abcabcabc,signup,\"2012-12-24T17:29:39.738Z\"")
	sqlText := `
		INSERT into user_event (id, user_id, event, date)
		VALUES
			(uuid(), "9Ip1aKbeZe2njCDM", "logon", now())
	`
	job, err := BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "%v", err)

	msgs := make([]datasource.Message, 0)
	resultWriter := NewResultBuffer(&msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	//u.Infof("running tasks?  %v", len(job.RootTask.Children()))
	err = job.Run()
	assert.T(t, err == nil)
	db, err := datasource.OpenConn("mockcsv", "user_event")
	assert.Tf(t, err == nil, "%v", err)
	gomap, ok := db.(*membtree.StaticDataSource)
	assert.T(t, ok, "Should be type StaticDataSource ", gomap)
	//u.Infof("db:  %#v", gomap)
	assert.T(t, gomap.Length() == 2, "Should have inserted")

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
	assert.Tf(t, gomap.Length() == 6, "should have 6 rows now")
	// TODO:  this doesn't work
	// row := sqlDb.QueryRow("SELECT count(*) from user_event")
	// assert.Tf(t, err == nil, "count(*) shouldnt error: %v", err)
	// var rowCt int
	// row.Scan(&rowCt)
	// assert.Tf(t, rowCt == 6, "has rowct=6: %v", rowCt)
}

func TestEngineUpdateAndUpsert(t *testing.T) {

	// By "Loading" table we force it to exist in this non DDL mock store
	mockcsv.LoadTable("user_event3", "id,user_id,event,date\n1,abcabcabc,signup,\"2012-12-24T17:29:39.738Z\"")
	sqlText := `
		UPSERT into user_event3 (id, user_id, event, date)
		VALUES
			("1234abcd", "9Ip1aKbeZe2njCDM", "logon", todate("2012/07/07"))
	`
	job, err := BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "%v", err)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	assert.T(t, err == nil)

	db, err := datasource.OpenConn("mockcsv", "user_event3")
	assert.Tf(t, err == nil, "%v", err)
	gomap, ok := db.(*membtree.StaticDataSource)
	assert.T(t, ok, "Should be type StaticDataSource ", gomap)
	//u.Infof("db:  %#v", gomap)
	assert.Tf(t, gomap.Length() == 2, "Should have inserted and have 2 but was %v", gomap.Length())

	// Now we are going to upsert the same row with changes
	sqlText = `
		UPSERT into user_event3 (id, user_id, event, date)
		VALUES
			("1234abcd", "9Ip1aKbeZe2njCDM", "logon", todate("2013/07/07"))
	`
	job, err = BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "%v", err)
	job.Setup()
	err = job.Run()
	assert.T(t, err == nil)

	// Should not have inserted, due to id being same
	assert.Tf(t, gomap.Length() == 2, "Should have inserted and have 2 but was %v", gomap.Length())

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
	job, err = BuildSqlJob(rtConf, "mockcsv", sqlUpdate)
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

func TestEngineDelete(t *testing.T) {

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
	job, _ := BuildSqlJob(rtConf, "mockcsv", sqlText)
	job.Setup()
	err := job.Run()
	//time.Sleep(time.Second * 1)
	assert.T(t, err == nil)
	//u.Infof("about to open DB to check size")
	db, err := datasource.OpenConn("mockcsv", "user_event2")
	assert.Tf(t, err == nil, "%v", err)
	userEvt2, ok := db.(*membtree.StaticDataSource)
	assert.Tf(t, ok, "Should be type StaticDataSource %p  %v", userEvt2, userEvt2)
	//u.Warnf("how many?  %v", userEvt2.Length())
	assert.Tf(t, userEvt2.Length() == 5, "Should have inserted 4, for 5 total rows but %p has: %d", userEvt2, userEvt2.Length())

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
	job, err := BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "no error %v", err)

	//writeCtx := NewContextSimple()
	msgs := make([]datasource.Message, 0)
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
