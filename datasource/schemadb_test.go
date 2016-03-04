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
	testutil.TestSelect(t, `show columns from users;`,
		[][]driver.Value{{"users", "BASE TABLE"}},
	)

	return
	// DESCRIBE
	testutil.TestSelect(t, `describe users;`,
		[][]driver.Value{{"users", "BASE TABLE"}},
	)
}
