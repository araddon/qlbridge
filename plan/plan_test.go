package plan_test

import (
	"testing"

	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/testutil"
)

func init() {
	testutil.Setup()
	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()
}

func TestRunTestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}
