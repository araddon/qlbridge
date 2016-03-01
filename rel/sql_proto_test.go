package rel

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/gogo/protobuf/proto"
)

var pbTests = []string{
	"SELECT hash(a) AS id, `z` FROM nothing;",
}

func TestPb(t *testing.T) {
	t.Parallel()
	for _, sql := range pbTests {
		s, err := ParseSql(sql)
		assert.Tf(t, err == nil, "Should not error on parse sql but got [%v] for %s", err, sql)
		ss := s.(*SqlSelect)
		pb := ss.ToPbStatement()
		assert.Tf(t, pb != nil, "was nil PB: %#v", ss)
		pbBytes, err := proto.Marshal(pb)
		assert.Tf(t, err == nil, "Should not error on proto.Marshal but got [%v] for %s pb:%#v", err, sql, pb)
		ss2, err := SqlFromPb(pbBytes)
		assert.Tf(t, err == nil, "Should not error from pb but got [%v] for %s ", err, sql)
		assert.T(t, ss.Equal(ss2), "Equal?")
		u.Infof("pre/post: \n\t%s\n\t%s", ss, ss2)
	}
}

var _ = u.EMPTY
