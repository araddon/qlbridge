package exec

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/bmizerany/assert"
)

var (
	_ = u.EMPTY
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()

	builtins.LoadAllBuiltins()

	u.Infof("show curent env info: %v", rtConf.Sources.String())

	mockcsv.MockData["users"] = `user_id,email,interests,reg_date,item_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",82
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`

	mockcsv.MockData["orders"] = `user_id,item_id,price,order_date,item_count
9Ip1aKbeZe2njCDM,1,22.50,"2012-10-24T17:29:39.738Z",82
9Ip1aKbeZe2njCDM,1,22.50,"2012-10-24T17:29:39.738Z",82
`

}

func TestWhere(t *testing.T) {

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

func testSubselect(t *testing.T) {

	// sub-select not implemented in lexer yet
	sqlText := `
		select 
	        user_id, email
	    FROM mockcsv.users
	    WHERE user_id in 
	    	(select user_id from mockcsv.orders)
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
