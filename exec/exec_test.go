package exec

import (
	"database/sql"
	"flag"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/bmizerany/assert"
)

var (
	_        = u.EMPTY
	loadData sync.Once
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
	if testing.Verbose() {
		u.SetupLogging("debug")
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
	        user_id, email, item_count * 2, yy(reg_date) > 10
	    FROM users
	    WHERE 
	    	yy(reg_date) > 10 
    `
	job, err := BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "no error %v", err)

	msgs := make([]datasource.Message, 0)
	resultWriter := NewResultBuffer(&msgs)
	job.Tasks.Add(resultWriter)

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
	mockcsv.LoadTable("user_event", `id,user_id,event,date
1,abcabcabc,signup,"2012-12-24T17:29:39.738Z"
`)
	sqlText := `
		INSERT into user_event (id, user_id, event, date)
		VALUES
			(uuid(), "9Ip1aKbeZe2njCDM", "logon", now())
    `
	job, err := BuildSqlJob(rtConf, "mockcsv", sqlText)
	assert.Tf(t, err == nil, "%v", err)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	assert.T(t, err == nil)
	db, err := datasource.OpenConn("mockcsv", "user_event")
	assert.Tf(t, err == nil, "%v", err)
	gomap, ok := db.(*datasource.StaticDataSource)
	assert.T(t, ok, "Should be type StaticDataSource ", gomap)
	u.Infof("db:  %#v", gomap)
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
