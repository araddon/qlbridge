package datasource_test

import (
	"database/sql/driver"
	"testing"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/testutil"
)

func init() {
	testutil.Setup()
	// Register our Datasources in registry
	datasource.Register(datasource.SchemaDbSourceType, datasource.NewSchemaDb(td.MockSchema))
}

var _ = u.EMPTY

func TestSchemaShowStatements(t *testing.T) {

	// TODO:  this test needs the "databases" ie system-schema not current-info-schema
	testutil.TestSelect(t, `show databases;`,
		[][]driver.Value{{"mockcsv"}},
	)
	// - rewrite show tables -> "use schema; select name from schema.tables;"
	testutil.TestSelect(t, `show tables;`,
		[][]driver.Value{{"orders"}, {"users"}},
	)
	testutil.TestSelect(t, `show tables from mockcsv;`,
		[][]driver.Value{{"orders"}, {"users"}},
	)
	testutil.TestSelect(t, `show tables in mockcsv;`,
		[][]driver.Value{{"orders"}, {"users"}},
	)

	// TODO:  we need to detect other schemas? and error on non-existent schemas?
	//testutil.TestSelectErr(t, `show tables from non_existent;`, nil)

	// show table create
	createStmt := "CREATE TABLE `users` (\n" +
		"    `user_id` varchar(255) DEFAULT NULL,\n" +
		"    `email` varchar(255) DEFAULT NULL,\n" +
		"    `interests` varchar(255) DEFAULT NULL,\n" +
		"    `reg_date` datetime DEFAULT NULL,\n" +
		"    `referral_count` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	testutil.TestSelect(t, `show create table users;`,
		[][]driver.Value{{"users", createStmt}},
	)

	// - rewrite show tables -> "use schema; select Table, Table_Type from schema.tables;"
	testutil.TestSelect(t, `show full tables;`,
		[][]driver.Value{{"orders", "BASE TABLE"}, {"users", "BASE TABLE"}},
	)
	testutil.TestSelect(t, `show tables like "us%";`,
		[][]driver.Value{{"users"}},
	)
	testutil.TestSelect(t, `show full tables like "us%";`,
		[][]driver.Value{{"users", "BASE TABLE"}},
	)
	testutil.TestSelect(t, `show full tables from mockcsv like "us%";`,
		[][]driver.Value{{"users", "BASE TABLE"}},
	)

	// SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
	testutil.TestSelect(t, `show columns from users;`,
		[][]driver.Value{
			{"user_id", "string", "", "", "", ""},
			{"email", "string", "", "", "", ""},
			{"interests", "string", "", "", "", ""},
			{"reg_date", "time", "", "", "", ""},
			{"referral_count", "int", "", "", "", ""},
		},
	)
	testutil.TestSelect(t, `show columns FROM users FROM mockcsv;`,
		[][]driver.Value{
			{"user_id", "string", "", "", "", ""},
			{"email", "string", "", "", "", ""},
			{"interests", "string", "", "", "", ""},
			{"reg_date", "time", "", "", "", ""},
			{"referral_count", "int", "", "", "", ""},
		},
	)
	testutil.TestSelect(t, `show columns from users WHERE Field Like "email";`,
		[][]driver.Value{
			{"email", "string", "", "", "", ""},
		},
	)
	testutil.TestSelect(t, `show full columns from users WHERE Field Like "email";`,
		[][]driver.Value{
			{"email", "string", "", "", "", "", "", "", ""},
		},
	)
	testutil.TestSelect(t, `show columns from users Like "user%";`,
		[][]driver.Value{
			{"user_id", "string", "", "", "", ""},
		},
	)
	// VARIABLES
	testutil.TestSelect(t, `show global variables like 'max_allowed*';`,
		[][]driver.Value{
			{"max_allowed_packet", int64(datasource.MaxAllowedPacket)},
		},
	)

	// DESCRIBE
	testutil.TestSelect(t, `describe users;`,
		[][]driver.Value{
			{"user_id", "string", "", "", "", ""},
			{"email", "string", "", "", "", ""},
			{"interests", "string", "", "", "", ""},
			{"reg_date", "time", "", "", "", ""},
			{"referral_count", "int", "", "", "", ""},
		},
	)

}
