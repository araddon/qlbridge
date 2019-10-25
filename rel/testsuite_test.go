package rel_test

import (
	"os"
	"testing"

	"github.com/araddon/qlbridge/testutil"
)

func TestMain(m *testing.M) {
	testutil.Setup() // will call flag.Parse()

	// Now run the actual Tests
	os.Exit(m.Run())
}
func TestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}
