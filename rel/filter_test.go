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
	assert.Tf(t, f1.Equal(f2), "Should Equal: \nf1:%s   %s \nf2:%s  %s", ql1, f1, ql2, f2.String())
}

func TestFilterEquality(t *testing.T) {
	t.Parallel()

	filterEqual(t, `FILTER OR (x == "y")`, `FILTER x == "y"`)
	filterEqual(t, `FILTER NOT OR (x == "y")`, `FILTER NOT (x == "y")`)
	filterEqual(t, `FILTER NOT AND (x == "y")`, `FILTER NOT (x == "y")`)
	filterEqual(t, `FILTER AND (x == "y" , AND ( stuff == x ))`, `FILTER AND (x == "y" , stuff == x )`)
}
