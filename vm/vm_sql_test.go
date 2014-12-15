package vm

import (
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

var (
	_ = u.EMPTY
)

func verifySql(t *testing.T, sql string, readContext ContextReader) ContextSimple {

	sqlVm, err := NewSqlVm(sql)
	assert.Tf(t, err == nil && sqlVm != nil, "Should create vm & parse sql")

	writeContext := NewContextSimple()
	err = sqlVm.Execute(writeContext, readContext)
	assert.Tf(t, err == nil, "non nil err: %v", err)
	return writeContext
}

func TestSqlSelectEval(t *testing.T) {
	msg := ContextSimple{
		map[string]Value{
			"int5":       NewIntValue(5),
			"item_count": NewStringValue("5"),
			"reg_date":   NewStringValue("2014/11/01"),
			"user_id":    NewStringValue("abc")},
		time.Now(),
	}
	wc := verifySql(t, `select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`, msg)
	wcAll := wc.All()
	assert.Tf(t, len(wcAll) == 3, "must have 3 fields: %v", wcAll)

	userId, _ := wc.Get("user_id")
	assert.Tf(t, userId.Value().(string) == "abc", "Should not have nil result: %v", userId)
	itemCount, _ := wc.Get("itemsx2")
	assert.Tf(t, itemCount.Value().(float64) == float64(10), "Should not have nil result: %T:%v", itemCount, itemCount)
	regYy, _ := wc.Get("regyy")
	u.Infof("%v %T", regYy, regYy)
	assert.Tf(t, regYy.Value().(bool) == true, "Should not have nil result: %T:%v", regYy, regYy)
}
