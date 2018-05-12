package testutil

import (
	"database/sql/driver"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource/membtree"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/schema"
)

var _ = u.EMPTY

func init() {
	Setup()
	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()
	exec.RegisterSqlDriver()
	exec.DisableRecover()
	static := membtree.NewStaticDataSource("users", 0, nil, []string{"user_id", "name", "email", "created", "roles"})
	schema.RegisterSourceType("inmem_testsuite", static)
}

// RunDDLTests run the DDL (CREATE SCHEMA, TABLE, alter) test harness suite.
func RunDDLTests(t TestingT) {
	// DDL
	TestExec(t, `CREATE SOURCE x WITH { "type":"inmem_testsuite" };`)
	TestExec(t, `DROP SOURCE x;`)
}

// RunTestSuite run the normal DML SQL test suite.
func RunTestSuite(t TestingT) {

	// Literal Queries
	TestSelect(t, `select 1;`,
		[][]driver.Value{{int64(1)}},
	)
	TestSelect(t, `select 1, "hello";`,
		[][]driver.Value{{int64(1), "hello"}},
	)
	TestSelect(t, `select exists(email), email FROM users WHERE yy(reg_date) > 10;`,
		[][]driver.Value{{true, "aaron@email.com"}},
	)

	// Slightly different test method allows source
	TestSqlSelect(t, "mockcsv", `select exists(email), email FROM users WHERE yy(reg_date) > 10;`,
		[][]driver.Value{{true, "aaron@email.com"}},
	)

	// - yy func evaluates
	// - projection (user_id, email)
	TestSelect(t, `select user_id, email FROM users WHERE yy(reg_date) > 10;`,
		[][]driver.Value{{"9Ip1aKbeZe2njCDM", "aaron@email.com"}},
	)
	// - ensure we can evaluate against "NULL"
	// - extra paren in where
	// - `db`.`col` syntax
	TestSelect(t, "SELECT user_id FROM users WHERE (`users.user_id` != NULL)",
		[][]driver.Value{{"hT2impsabc345c"}, {"9Ip1aKbeZe2njCDM"}, {"hT2impsOPUREcVPc"}},
	)
	TestSelect(t, "SELECT email FROM users WHERE interests != NULL)",
		[][]driver.Value{{"aaron@email.com"}, {"bob@email.com"}},
	)
	TestSelect(t, "SELECT email FROM users WHERE (`users`.`email` like \"%aaron%\");",
		[][]driver.Value{{"aaron@email.com"}},
	)

	// Mixed *, literal, fields
	TestSelect(t, "SELECT *, emaildomain(email), contains(email,\"aaron\"), 5 FROM users WHERE email = \"aaron@email.com\"",
		[][]driver.Value{{"9Ip1aKbeZe2njCDM", "aaron@email.com", "fishing", "2012-10-17T17:29:39.738Z", "82",
			`{"name":"bob"}`, "email.com", true, int64(5)}},
	)

	// - user_id != NULL (on string column)
	// - as well as count(*)
	TestSelect(t, "SELECT COUNT(*) AS count FROM users WHERE (`users.user_id` != NULL)",
		[][]driver.Value{{int64(3)}},
	)

	// Aliasing columns in group by
	TestSelect(t, "select `users`.`user_id` AS userids FROM users WHERE email = \"aaron@email.com\" GROUP BY `users`.`user_id`;",
		[][]driver.Value{{"9Ip1aKbeZe2njCDM"}},
	)

	// nested functions in aggregations
	// - note also lack of group-by ie determine Is Agg query in rel ast
	TestSelect(t, "SELECT AVG(CHAR_LENGTH(CAST(`email` AS CHAR))) AS `len` FROM `users`",
		[][]driver.Value{{float64(14.0)}}, // aaron@email.combob@email.comnot_an_email_2 = 42 characters / 3 = 14
	)

	// Distinct keyword
	TestSelect(t, "SELECT COUNT(DISTINCT(`users.email`)) AS cd FROM users",
		[][]driver.Value{{int64(0)}},
	)

	TestSelect(t, "SELECT email FROM users ORDER BY email DESC",
		[][]driver.Value{{"not_an_email_2"}, {"bob@email.com"}, {"aaron@email.com"}},
	)
	TestSelect(t, "SELECT email FROM users ORDER BY email ASC",
		[][]driver.Value{{"aaron@email.com"}, {"bob@email.com"}, {"not_an_email_2"}},
	)

	// This is an error because we have schema on this table, and this column
	// doesn't exist.
	TestSelectErr(t, "SELECT email, non_existent_field FROM users ORDER BY email ASC", nil)

	/*
		// TODO: #56 DISTINCT inside count()
		testutil.TestSelect(t, "SELECT COUNT(DISTINCT(`users.user_id`)) AS cd FROM users",
			[][]driver.Value{{int64(3)}},
		)

		// TODO: #56 this doesn't work because ordering is non-deterministic coming out of group by currently
		//  which technically don't think there is any sql expectation of ordering, but there is for this test harness
		testutil.TestSelect(t, "select `users`.`user_id` AS userids FROM users GROUP BY `users`.`user_id`;",
			[][]driver.Value{{"hT2impsabc345c"}, {"9Ip1aKbeZe2njCDM"}, {"hT2impsOPUREcVPc"}},
		)
	*/
}

// RunSimpleSuite run the normal DML SQL test suite.
func RunSimpleSuite(t TestingT) {

	// Literal Queries
	TestSelect(t, `select 1;`,
		[][]driver.Value{{int64(1)}},
	)
	TestSelect(t, `select 1, "hello";`,
		[][]driver.Value{{int64(1), "hello"}},
	)

	// - ensure we can evaluate against "NULL"
	// - extra paren in where
	// - `db`.`col` syntax
	TestSelect(t, "SELECT user_id FROM users WHERE (`users.user_id` != NULL)",
		[][]driver.Value{{"hT2impsabc345c"}, {"9Ip1aKbeZe2njCDM"}, {"hT2impsOPUREcVPc"}},
	)
	TestSelect(t, "SELECT email FROM users WHERE interests != NULL)",
		[][]driver.Value{{"aaron@email.com"}, {"bob@email.com"}},
	)

	TestSelect(t, "SELECT email FROM users WHERE (`users`.`email` like \"%aaron%\");",
		[][]driver.Value{{"aaron@email.com"}},
	)

	// - user_id != NULL (on string column)
	// - as well as count(*)
	TestSelect(t, "SELECT COUNT(*) AS count FROM users WHERE (`users.user_id` != NULL)",
		[][]driver.Value{{int64(3)}},
	)

	// Aliasing columns in group by
	TestSelect(t, "select `users`.`user_id` AS userids FROM users WHERE email = \"aaron@email.com\" GROUP BY `users`.`user_id`;",
		[][]driver.Value{{"9Ip1aKbeZe2njCDM"}},
	)

	// nested functions in aggregations
	// - note also lack of group-by ie determine Is Agg query in rel ast
	TestSelect(t, "SELECT AVG(CHAR_LENGTH(CAST(`email` AS CHAR))) AS `len` FROM `users`",
		[][]driver.Value{{float64(14.0)}}, // aaron@email.combob@email.comnot_an_email_2 = 42 characters / 3 = 14
	)

	// Distinct keyword
	TestSelect(t, "SELECT COUNT(DISTINCT(`users`.`email`)) AS cd FROM users",
		[][]driver.Value{{int64(0)}},
	)
	return
	TestSelect(t, "SELECT email FROM users ORDER BY email DESC",
		[][]driver.Value{{"not_an_email_2"}, {"bob@email.com"}, {"aaron@email.com"}},
	)
	TestSelect(t, "SELECT email FROM users ORDER BY email ASC",
		[][]driver.Value{{"aaron@email.com"}, {"bob@email.com"}, {"not_an_email_2"}},
	)

	// This is an error because we have schema on this table, and this column
	// doesn't exist.
	TestSelectErr(t, "SELECT email, non_existent_field FROM users ORDER BY email ASC", nil)

	/*
		// TODO: #56 DISTINCT inside count()
		testutil.TestSelect(t, "SELECT COUNT(DISTINCT(`users.user_id`)) AS cd FROM users",
			[][]driver.Value{{int64(3)}},
		)

		// TODO: #56 this doesn't work because ordering is non-deterministic coming out of group by currently
		//  which technically don't think there is any sql expectation of ordering, but there is for this test harness
		testutil.TestSelect(t, "select `users`.`user_id` AS userids FROM users GROUP BY `users`.`user_id`;",
			[][]driver.Value{{"hT2impsabc345c"}, {"9Ip1aKbeZe2njCDM"}, {"hT2impsOPUREcVPc"}},
		)
	*/
}
