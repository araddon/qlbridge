package files_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource/files"
)

func TestFileHandlerRegister(t *testing.T) {
	assert.True(t, registryNilPanic(), "Should have paniced")
	assert.True(t, registryForPanic(), "Should have paniced")
}
func registryNilPanic() (paniced bool) {
	defer func() {
		if r := recover(); r != nil {
			paniced = true
		}
	}()
	files.RegisterFileHandler("testnil1", nil)
	return paniced
}

func registryForPanic() (paniced bool) {
	defer func() {
		if r := recover(); r != nil {
			paniced = true
		}
	}()
	files.RegisterFileHandler("test1", files.NewJsonHandlerTables(lineParser, []string{"issues"}))
	files.RegisterFileHandler("test1", files.NewJsonHandlerTables(lineParser, []string{"issues"}))
	return paniced
}
