package rel

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
)

var (
	_ = u.EMPTY
)

func filterEqual(t *testing.T, ql1, ql2 string) {
	u.Debugf("before: %s", ql1)
	f1, err := ParseFilterQL(ql1)
	assert.Equal(t, nil, err)
	f2, err := ParseFilterQL(ql2)
	assert.Equal(t, nil, err)
	assert.Tf(t, f1.EqualLogic(f2), "f1 should equal f2:  %s", ql1)
}

func TestFilterEquality(t *testing.T) {
	t.Parallel()

	filterEqual(t, `FILTER OR (x == "y")`, `FILTER x == "y"`)
	filterEqual(t, `FILTER NOT OR (x == "y")`, `FILTER x != "y"`)
	filterEqual(t, `FILTER NOT AND (x == "y")`, `FILTER x != "y"`)
	filterEqual(t, `FILTER AND (x == "y" , AND ( stuff == x ))`, `FILTER AND (x == "y" , stuff == x )`)
}
