package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	//"github.com/araddon/qlbridge/plan"
)

func TestContext(t *testing.T) {
	c := &Context{}
	var planNil *Context
	planNil.Recover() // make sure we don't panic
	assert.Equal(t, true, planNil.Equal(nil))
	assert.Equal(t, false, planNil.Equal(c))
	assert.Equal(t, false, c.Equal(nil))

	selQuery := "Select 1;"
	c1 := NewContext(selQuery)
	c2 := NewContext(selQuery)
	// Should NOT be equal because the id is not the same
	assert.Equal(t, false, c1.Equal(c2))

	c1pb := c1.ToPB()
	c1FromPb := NewContextFromPb(c1pb)
	// Should be equal
	assert.Equal(t, true, c1.Equal(c1FromPb))
	c1FromPb.SchemaName = "what"
	assert.Equal(t, false, c1.Equal(c1FromPb))
	c1FromPb.fingerprint = 88 //
	assert.Equal(t, false, c1.Equal(c1FromPb))
}
