package vm

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	"github.com/bmizerany/assert"
)

var (
	_      = u.EMPTY
	rtConf = NewRuntimeConfig()
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()

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

func TestExecWhere(t *testing.T) {

	sqlText := `
		select 
	        user_id, email, item_count * 2, yy(reg_date) > 10 
	    FROM mockcsv.users
	    WHERE 
	    	yy(reg_date) > 10 
    `
	tasks, err := BuildSqlJob(rtConf, sqlText)
	assert.Tf(t, err == nil, "no error %v", err)

	//writeCtx := NewContextSimple()
	msgs := make([]datasource.Message, 0)
	go func() {
		outChan := tasks[len(tasks)-1].MessageOut()
		for msg := range outChan {
			u.Infof("msg: %v", msg)
			msgs = append(msgs, msg)
		}
	}()
	err = RunJob(tasks)
	time.Sleep(time.Millisecond * 30)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered out 2 messages")

}

func TestExecSubselect(t *testing.T) {

	sqlText := `
		select 
	        user_id, email
	    FROM mockcsv.users
	    WHERE user_id in 
	    	(select user_id from mockcsv.orders)
    `
	tasks, err := BuildSqlJob(rtConf, sqlText)
	assert.Tf(t, err == nil, "no error %v", err)

	//writeCtx := NewContextSimple()
	msgs := make([]datasource.Message, 0)
	go func() {
		outChan := tasks[len(tasks)-1].MessageOut()
		for msg := range outChan {
			u.Infof("msg: %v", msg)
			msgs = append(msgs, msg)
		}
	}()
	err = RunJob(tasks)
	time.Sleep(time.Millisecond * 30)
	assert.Tf(t, err == nil, "no error %v", err)
	assert.Tf(t, len(msgs) == 1, "should have filtered out 2 messages")

}
