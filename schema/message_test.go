package schema_test

import (
	"database/sql/driver"
	"testing"

	"github.com/lytics/qlbridge/schema"
	"github.com/stretchr/testify/assert"
)

func TestMessage(t *testing.T) {
	k := schema.NewKeyUint(uint64(7))
	assert.Equal(t, driver.Value(uint64(7)), k.Key())
}
