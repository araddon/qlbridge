package sqlite_test

import (
	"testing"

	u "github.com/araddon/gou"
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

	for _, tablename := range td.MockSchema.Tables() {
		tbl, _ := td.MockSchema.Table(tablename)
		if tbl == nil {
			panic("missing table " + tablename)
		}
		u.Infof("found schema for %s \n%s", tablename, tbl.String())
	}

	td.TestContext = func(query string) *plan.Context {
		ctx := plan.NewContext(query)
		ctx.DisableRecover = true
		//ctx.Schema = MockSchema
		ctx.Session = datasource.NewMySqlSessionVars()
		return ctx
	}
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
