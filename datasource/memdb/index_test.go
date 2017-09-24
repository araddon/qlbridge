package memdb

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
)

func TestIndex(t *testing.T) {
	assert.Equal(t, uint64(264), makeId(driver.Value(int64(264))))

	assert.Equal(t, uint64(0), makeId(driver.Value(nil)))

	assert.Equal(t, uint64(2404974478441873708), makeId([]byte("hello")), " %v", makeId([]byte("hello")))

	assert.Equal(t, uint64(2404974478441873708), makeId("hello"), " %v", makeId("hello"))

	v := datasource.KeyCol{Val: 264}

	assert.Equal(t, uint64(264), makeId(v))
}
