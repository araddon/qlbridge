package vm_test

import (
	"testing"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	st1, _ = dateparse.ParseAny("12/18/2014")
	st2, _ = dateparse.ParseAny("12/18/2019")

	// This is the message context which will be added to all tests below
	// and be available to the VM runtime for evaluation by using
	// key's such as "int5" or "user_id"
	sqlData = datasource.NewContextSimpleData(map[string]value.Value{
		"int5":    value.NewIntValue(5),
		"str5":    value.NewStringValue("5"),
		"created": value.NewTimeValue(st1),
		"updated": value.NewTimeValue(st2),
		"bvalt":   value.NewBoolValue(true),
		"bvalf":   value.NewBoolValue(false),
		"user_id": value.NewStringValue("abc"),
		"urls":    value.NewStringsValue([]string{"abc", "123"}),
		"hits":    value.NewMapIntValue(map[string]int64{"google.com": 5, "bing.com": 1}),
		"email":   value.NewStringValue("bob@bob.com"),
	})
	// list of tests
	sqlTests = []sqlTest{
		st(`select int5 FROM mycontext`, map[string]interface{}{"int5": 5}),
		st(`select int5 FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
		st(`select int5 FROM mycontext WHERE not_a_field < "now-1M"`, map[string]interface{}{}),
		st(`select int5 IF EXISTS urls FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
		st(`select int5, str5 IF EXISTS not_a_field FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
		st(`select int5, str5 IF toint(str5) FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
		st(`select int5, "hello" AS hello IF user_id > true FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
		st(`select int5, todate("hello") AS hello FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
		// this should fail
		st(`select int5 FROM mycontext WHERE not_a_field > 10`, nil),
		st(`select int5 FROM mycontext WHERE user_id > true`, nil),
		st(`select int5 FROM mycontext WHERE int5 + 6`, nil),
	}
)

func TestRunSqlTests(t *testing.T) {

	for _, test := range sqlTests {

		ss, err := rel.ParseSql(test.sql)
		assert.Equal(t, nil, err, "expected no error but got %v for %s", err, test.sql)

		sel, ok := ss.(*rel.SqlSelect)
		assert.True(t, ok, "expected rel.SqlSelect but got %T", ss)

		writeContext := datasource.NewContextSimple()
		_, err = vm.EvalSql(sel, writeContext, test.context)
		assert.Equal(t, nil, err, "expected no error but got %v for %s", err, test.sql)

		for key, v := range test.result.Data {
			v2, ok := writeContext.Get(key)
			assert.True(t, ok, "Expected ok for get %s output: %#v", key, writeContext.Data)
			assert.Equal(t, v2.Value(), v.Value(), "?? %s  %v!=%v %T %T", key, v.Value(), v2.Value(), v.Value(), v2.Value())
		}
	}
}

type sqlTest struct {
	sql     string
	context expr.EvalContext
	result  *datasource.ContextSimple // ?? what is this?
	rowct   int                       // expected row count
}

func st(sql string, results map[string]interface{}) sqlTest {
	return sqlTest{sql: sql, result: datasource.NewContextSimpleNative(results), context: sqlData}
}
