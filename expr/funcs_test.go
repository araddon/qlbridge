package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFuncsRegistry(t *testing.T) {
	t.Parallel()

	_, ok := EmptyEvalFunc(nil, nil)
	assert.Equal(t, false, ok)
}
