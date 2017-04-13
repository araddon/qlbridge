package files

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
)

var _ = u.EMPTY

func TestFileTableNames(t *testing.T) {

	assert.Equal(t, "players", TableFromFileAndPath("", "tables/players.csv"))
	assert.Equal(t, "players", TableFromFileAndPath("", "tables/players/2017.csv"))

	assert.Equal(t, "players", TableFromFileAndPath("baseball/", "baseball/tables/players.csv"))
	assert.Equal(t, "players", TableFromFileAndPath("baseball/", "baseball/tables/players/2017.csv"))

	assert.Equal(t, "players", TableFromFileAndPath("baseball", "baseball/tables/players.csv"))
	assert.Equal(t, "players", TableFromFileAndPath("baseball", "baseball/tables/players/2017.csv"))
}
