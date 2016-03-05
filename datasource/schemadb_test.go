package datasource_test

import (
	"database/sql/driver"
	"testing"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/testutil"
	//"github.com/araddon/qlbridge/schema"
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

	// TODO:  we need to detect other schemas?
	//testutil.TestSelectErr(t, `show tables from non_existent;`, nil)

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

	// columns
	// SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
	//| Field | Type         | Null | Key | Default | Extra |
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
