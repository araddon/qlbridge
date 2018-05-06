package sqlite_test

import (
	"testing"

	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	_ "github.com/mattn/go-sqlite3"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

var (
	sch *schema.Schema
)

func init() {
	testutil.Setup()
	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()
}

func planContext(query string) *plan.Context {
	ctx := plan.NewContext(query)
	ctx.DisableRecover = true
	ctx.Schema = sch
	ctx.Session = datasource.NewMySqlSessionVars()
	return ctx
}

func TestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}
