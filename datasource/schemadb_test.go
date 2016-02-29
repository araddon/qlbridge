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
	// - rewrite show tables -> "use schema; select name from schema.tables;"
	testutil.TestSelect(t, `show tables;`,
		[][]driver.Value{{"orders"}, {"users"}},
	)
	testutil.TestSelect(t, `show tables like "us%";`,
		[][]driver.Value{{"users"}},
	)
}
