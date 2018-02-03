package files

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileTableNames(t *testing.T) {

	assert.Equal(t, "players", TableFromFileAndPath("", "tables/players.csv"))
	assert.Equal(t, "players", TableFromFileAndPath("", "tables/players/2017.csv"))

	assert.Equal(t, "players", TableFromFileAndPath("baseball/", "baseball/tables/players.csv"))
	assert.Equal(t, "players", TableFromFileAndPath("baseball/", "baseball/tables/players/2017.csv"))

	assert.Equal(t, "players", TableFromFileAndPath("baseball", "baseball/tables/players.csv"))
	assert.Equal(t, "players", TableFromFileAndPath("baseball", "baseball/tables/players/2017.csv"))

	// Cannot interpret this
	assert.Equal(t, "", TableFromFileAndPath("baseball", "baseball/tables/players/partition1/2017.csv"))
}

func TestFileInfo(t *testing.T) {

	fi := &FileInfo{}
	assert.NotEqual(t, "", fi.String())

}
