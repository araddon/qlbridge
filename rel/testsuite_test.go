package rel_test

import (
	"testing"

	"github.com/araddon/qlbridge/testutil"
)

func init() {
	testutil.Setup()
}
func TestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}
