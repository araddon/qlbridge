package expr

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/bmizerany/assert"
)

var _ = u.EMPTY

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()

	// change quotes marks to NOT include double-quotes so we can use for values
	lex.IdentityQuoting = []byte{'[', '`'}
}

type testBuiltins struct {
	expr string
}

var builtinTests = []testBuiltins{
	{`count(nonfield)`},
}

func TestBuiltins(t *testing.T) {
	for _, biTest := range builtinTests {
		node, err := ParseExpression(biTest.expr)
		assert.Tf(t, err == nil, "nil err: %v", err)
		assert.Tf(t, node != nil, "has node: %v", node)
	}
}
