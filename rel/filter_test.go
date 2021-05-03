package rel_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/rel"
)

func filterEqual(t *testing.T, ql1, ql2 string) {
	u.Debugf("before: %s", ql1)
	f1, err := rel.ParseFilterQL(ql1)
	assert.Equal(t, nil, err)
	f2, err := rel.ParseFilterQL(ql2)
	assert.Equal(t, nil, err)
	assert.True(t, f1.Equal(f2), "Should Equal: \nf1:%s   %s \nf2:%s  %s", ql1, f1, ql2, f2.String())
	assert.Equal(t, f1.String(), f2.String())
}

func filterSelEqual(t *testing.T, ql1, ql2 string) {
	u.Debugf("before: %s", ql1)
	f1, err := rel.ParseFilterSelect(ql1)
	assert.Equal(t, nil, err)
	f2, err := rel.ParseFilterSelect(ql2)
	assert.Equal(t, nil, err)
	assert.True(t, f1.Equal(f2), "Should Equal: \nf1:%s   %s \nf2:%s  %s", ql1, f1, ql2, f2.String())
	assert.Equal(t, f1.String(), f2.String())
}

func TestFilterEquality(t *testing.T) {
	t.Parallel()

	filterEqual(t, `FILTER OR (x == "y")`, `FILTER x == "y"`)
	filterEqual(t, `FILTER NOT OR (x == "y")`, `FILTER NOT (x == "y")`)
	filterEqual(t, `FILTER NOT AND (x == "y")`, `FILTER NOT (x == "y")`)
	filterEqual(t, `FILTER AND (x == "y" , AND ( stuff == x ))`, `FILTER AND (x == "y" , stuff == x )`)
	filterSelEqual(t, `SELECT x, y FROM user FILTER NOT AND (x == "y")`, `SELECT x, y FROM user FILTER NOT (x == "y")`)
	filterSelEqual(t, `SELECT x, y FROM user FILTER NOT AND (x == "y") LIMIT 10 ALIAS bob`, `SELECT x, y FROM user FILTER NOT (x == "y") LIMIT 10 ALIAS bob`)

	rfs := rel.NewFilterSelect()
	assert.NotEqual(t, nil, rfs)

	// Some Manipulations to force un-equal compare
	fs1, _ := rel.ParseFilterSelect(`SELECT a FROM user FILTER NOT AND (x == "y")`)
	fs1b, _ := rel.ParseFilterSelect(`SELECT a FROM user FILTER NOT AND (x == "y")`)
	var fs2, fs3 *rel.FilterSelect

	assert.True(t, fs1.Equal(fs1b))
	assert.True(t, fs3.Equal(fs2))
	assert.True(t, !fs2.Equal(fs1))
	assert.True(t, !fs1.Equal(fs2))
	assert.Equal(t, "", fs2.String())

	// Some Manipulations to force un-equal compare
	f1 := rel.MustParseFilter(`FILTER NOT AND (x == "y")`)
	f1b := rel.MustParseFilter(`FILTER NOT AND (x == "y")`)
	var f2, f3 *rel.FilterStatement

	assert.True(t, f1.Equal(f1b))
	assert.True(t, f3.Equal(f2))
	assert.True(t, !f2.Equal(f1))
	assert.True(t, !f1.Equal(f2))
	assert.Equal(t, "", f3.String())

	tests := [][]string{
		{`FILTER OR (x == "y")`, `--description
		FILTER OR (x == "y")`},
		{`FILTER OR (x == "y")`, `FILTER OR (x == "8")`},
		{`FILTER OR (x == "y") FROM user`, `FILTER OR (x == "y")`},
		{`FILTER OR (x == "y") LIMIT 10`, `FILTER OR (x == "y")`},
		{`FILTER NOT AND (x == "y") ALIAS xyz`, `FILTER NOT AND (x == "y");`},
		{`FILTER NOT AND (x == "y") WITH conf_key = 22;`, `FILTER NOT AND (x == "y");`},
		{`FILTER NOT AND (x == "y") WITH conf_key = 22;`, `FILTER NOT AND (x == "y") WITH conf_key = 25;`},
	}

	for _, fl := range tests {
		ft1 := rel.MustParseFilter(fl[0])
		ft2 := rel.MustParseFilter(fl[1])
		assert.True(t, !ft1.Equal(ft2))
	}

	tests = [][]string{
		{`SELECT x, y FROM user FILTER NOT AND (x == "y") LIMIT 10 ALIAS bob`, `SELECT x, y, z FROM user FILTER NOT AND (x == "y") LIMIT 10 ALIAS bob`},
		{`SELECT x, y FROM user FILTER NOT AND (x == "y") LIMIT 10 ALIAS bob`, `SELECT x, y FROM user FILTER NOT AND (x == "18") LIMIT 10 ALIAS bob`},
	}

	for _, fl := range tests {
		fts1, _ := rel.ParseFilterSelect(fl[0])
		fts2, _ := rel.ParseFilterSelect(fl[1])
		assert.True(t, !fts1.Equal(fts2))
	}
}
