package rel_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/rel"
)

var pbTests = []string{
	"SELECT hash(a) AS id, `z` FROM nothing;",
	`SELECT name FROM orders WHERE name = "bob";`,
}

func TestPb(t *testing.T) {
	t.Parallel()
	for _, sql := range pbTests {
		s, err := rel.ParseSql(sql)
		assert.True(t, err == nil, "Should not error on parse sql but got [%v] for %s", err, sql)
		ss := s.(*rel.SqlSelect)
		pb := ss.ToPbStatement()
		assert.True(t, pb != nil, "was nil PB: %#v", ss)
		pbBytes, err := proto.Marshal(pb)
		assert.True(t, err == nil, "Should not error on proto.Marshal but got [%v] for %s pb:%#v", err, sql, pb)
		ss2, err := rel.SqlFromPb(pbBytes)
		assert.True(t, err == nil, "Should not error from pb but got [%v] for %s ", err, sql)
		assert.True(t, ss.Equal(ss2), "Equal?")
		u.Infof("pre/post: \n\t%s\n\t%s", ss, ss2)
	}
}
