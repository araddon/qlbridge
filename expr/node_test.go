package expr_test

import (
	"encoding/json"
	"testing"

	u "github.com/araddon/gou"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/expr"
)

var pbTests = []string{
	`eq(event,"stuff") OR ge(party, 1)`,
	`"Portland" IN ("ohio")`,
	`"xyz" BETWEEN todate("1/1/2015") AND 50`,
	`name == "bob"`,
	`name = 'bob'`,
	`AND ( EXISTS x, EXISTS y )`,
	`AND ( EXISTS x, INCLUDE ref_name )`,
	`company = "Toys R"" Us"`,
}

func TestNodePb(t *testing.T) {
	t.Parallel()
	for _, exprText := range pbTests {
		exp, err := expr.ParseExpression(exprText)
		assert.Equal(t, err, nil, "Should not error parse expr but got ", err, "for ", exprText)
		pb := exp.NodePb()
		assert.True(t, pb != nil, "was nil PB: %#v", exp)
		pbBytes, err := proto.Marshal(pb)
		assert.True(t, err == nil, "Should not error on proto.Marshal but got [%v] for %s pb:%#v", err, exprText, pb)
		n2, err := expr.NodeFromPb(pbBytes)
		assert.True(t, err == nil, "Should not error from pb but got ", err, "for ", exprText)
		assert.True(t, exp.Equal(n2), "Equal?  %v  %v", exp, n2)
		u.Infof("pre/post: \n\t%s\n\t%s", exp, n2)
	}
}

func TestExprRoundTrip(t *testing.T) {
	t.Parallel()
	for _, et := range exprTests {
		exp, err := expr.ParseExpression(et.qlText)
		if et.ok {
			assert.Equal(t, err, nil, "Should not error parse expr but got %v for %s", err, et.qlText)
			by, err := json.MarshalIndent(exp.Expr(), "", "  ")
			assert.Equal(t, err, nil)
			u.Debugf("%s", string(by))
			en := &expr.Expr{}
			err = json.Unmarshal(by, en)
			assert.Equal(t, err, nil)
			_, err = expr.NodeFromExpr(en)
			assert.Equal(t, err, nil, et.qlText)

			// by, _ = json.MarshalIndent(nn.Expr(), "", "  ")
			// u.Debugf("%s", string(by))

			// TODO: Fixme
			// u.Debugf("%s", nn)
			// assert.True(t, nn.Equal(exp), "%s  doesn't match %s", et.qlText, nn.String())

		} else {
			assert.NotEqual(t, nil, err)
		}

	}
}

func TestNodeJson(t *testing.T) {
	t.Parallel()
	for _, exprText := range pbTests {
		exp, err := expr.ParseExpression(exprText)
		assert.Equal(t, err, nil, "Should not error parse expr but got ", err, "for ", exprText)
		by, err := json.MarshalIndent(exp.Expr(), "", "  ")
		assert.Equal(t, err, nil)
		u.Debugf("%s", string(by))
	}
}

var _ = u.EMPTY
