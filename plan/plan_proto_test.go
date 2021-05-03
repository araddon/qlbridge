package plan_test

import (
	"testing"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/datasource"
	td "github.com/lytics/qlbridge/datasource/mockcsvtestdata"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/plan"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/lytics/qlbridge/vm"
)

var sqlStatements = []string{
	"SELECT count(*), sum(stuff) AS sumostuff FROM orders WHERE age > 20 GROUP BY category HAVING sumostuff > 10;",
	"SELECT AVG(CHAR_LENGTH(CAST(`title` AS CHAR))) as title_avg from orders WITH distributed=true, node_ct=2",
	// this one tests a session_time that doesn't exist in table schema
	"SELECT session_time FROM orders",
	// Test order by
	"SELECT name, order_id FROM orders ORDER BY name ASC;",
	`
		SELECT a.language, a.template, Count(*) AS count
		FROM 
			(Select Distinct language, template FROM content) AS a
			Left Join users AS b
				On b.language = a.language AND b.template = b.template
		GROUP BY a.language, a.template`,
}

var sqlNonSelect = []string{
	// mutations
	`INSERT INTO mytable (id, str) values (0, "a");`,
	`UPSERT INTO mytable (id, str) values (0, "a");`,
	`UPDATE users SET name = "was_updated", [deleted] = true WHERE id = "user815"`,
	`DELETE from users where employee = false;`,
	// show
	`DESCRIBE mytable`,
	`show tables`,
	`show tables LIKE "user%";`,
	`show databases`,
	"SHOW FULL COLUMNS FROM `tablex` FROM `dbx` LIKE '%';",
	`SHOW VARIABLES`,
	`SHOW GLOBAL VARIABLES like '%'`,
	"show keys from `appearances` from `baseball`",
	"show indexes from `appearances` from `baseball`",

	// set
	`SET @@local.sort_buffer_size=10000;`,

	// DDL
	`
	CREATE TABLE articles 
		 (
		  ID int(11) NOT NULL AUTO_INCREMENT,
		  Email char(150) NOT NULL DEFAULT '' COMMENT "email hello",
		  PRIMARY KEY (ID),
		  CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email) COMMENT "hello constraint"
		) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8
	WITH stuff = "hello";`,
}

var sqlStatementsx = []string{
	`SELECT name FROM orders WHERE name = "bob";`,
}

func selectPlan(t *testing.T, ctx *plan.Context) *plan.Select {
	pln := planStmt(t, ctx)

	sp, ok := pln.(*plan.Select)
	assert.True(t, ok, "must be *plan.Select")
	return sp
}

func planStmt(t *testing.T, ctx *plan.Context) plan.Task {
	stmt, err := rel.ParseSql(ctx.Raw)
	assert.True(t, err == nil, "Must parse but got %v", err)
	ctx.Stmt = stmt

	planner := plan.NewPlanner(ctx)
	pln, _ := plan.WalkStmt(ctx, stmt, planner)
	//assert.True(t, err == nil) // since the FROM doesn't exist it errors
	assert.True(t, pln != nil, "must have plan")
	return pln
}

func TestSqlPlans(t *testing.T) {
	for _, sqlStatement := range append(sqlStatements, sqlNonSelect...) {
		ctx := td.TestContext(sqlStatement)
		u.Infof("running for pb check on: %s", sqlStatement)
		p := planStmt(t, ctx)
		assert.True(t, p != nil)
	}
}

func TestSelectSerialization(t *testing.T) {
	// Should have error on invalid plan
	_, err := plan.SelectPlanFromPbBytes([]byte("hello"), td.SchemaLoader)
	assert.NotEqual(t, nil, err)

	for _, sqlStatement := range sqlStatements {
		ctx := td.TestContext(sqlStatement)
		u.Infof("running for pb check on: %s", sqlStatement)
		p := selectPlan(t, ctx)
		assert.True(t, p != nil)
		pb, err := p.Marshal()
		assert.True(t, err == nil, "expected no error but got %v", err)
		assert.True(t, len(pb) > 10, string(pb))
		p2, err := plan.SelectPlanFromPbBytes(pb, td.SchemaLoader)
		assert.True(t, err == nil, "expected no error but got %v", err)
		assert.True(t, p2 != nil)
		assert.True(t, p2.PlanBase != nil, "Has plan Base")
		assert.True(t, p2.Stmt.Raw == p.Stmt.Raw)
		assert.True(t, p.Equal(p2), "Should be equal plans")
	}
}

var (
	_ = u.EMPTY

	st1, _ = dateparse.ParseAny("12/18/2014")
	st2, _ = dateparse.ParseAny("12/18/2019")

	// This is the message context which will be added to all tests below
	//  and be available to the VM runtime for evaluation by using
	//  key's such as "int5" or "user_id"
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
	sqlTestsX = []sqlTest{
		// Date math
		st(`select int5 FROM mycontext WHERE created < "now-1M"`, map[string]interface{}{"int5": 5}),
	}
	// list of tests
	sqlTests = []sqlTest{
		st(`select toint(str5) as sv`, map[string]interface{}{"sv": 5}),
	}
)

type sqlTest struct {
	sql     string
	context expr.ContextReader
	result  *datasource.ContextSimple // ?? what is this?
	rowct   int                       // expected row count
}

func st(sql string, results map[string]interface{}) sqlTest {
	return sqlTest{sql: sql, result: datasource.NewContextSimpleNative(results), context: sqlData}
}

func TestRunProtoTests(t *testing.T) {

	for _, test := range sqlTests {

		ctx := td.TestContext(test.sql)
		p := selectPlan(t, ctx)
		assert.True(t, p != nil)
		pb, err := p.Marshal()
		assert.True(t, err == nil, "expected no error but got %v", err)

		selPlan, err := plan.SelectPlanFromPbBytes(pb, td.SchemaLoader)
		assert.True(t, err == nil, "expected no error but got %v", err)

		assert.True(t, selPlan.Stmt != nil, "must have stmt")

		writeContext := datasource.NewContextSimple()
		_, err = vm.EvalSql(selPlan.Stmt, writeContext, test.context)
		assert.True(t, err == nil, "expected no error but got ", err, " for ", test.sql)

		for key, v := range test.result.Data {
			v2, ok := writeContext.Get(key)
			assert.True(t, ok, "Expected ok for get %s output: %#v", key, writeContext.Data)
			assert.Equal(t, v2.Value(), v.Value(), "?? %s  %v!=%v %T %T", key, v.Value(), v2.Value(), v.Value(), v2.Value())
		}
	}
}
