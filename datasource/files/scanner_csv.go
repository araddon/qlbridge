package files

import (
	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensuure our csv handler implements FileHandler interface
	_ FileHandler = (*csvFiles)(nil)
)

func init() {
	RegisterFileHandler("csv", &csvFiles{})
}

// the built in csv filehandler
type csvFiles struct {
	appendcols []string
}

func (m *csvFiles) Init(store FileStore, ss *schema.Schema) error { return nil }
func (m *csvFiles) FileAppendColumns() []string                   { return m.appendcols }
func (m *csvFiles) File(path string, obj cloudstorage.Object) *FileInfo {
	return FileInfoFromCloudObject(path, obj)
}
func (m *csvFiles) Scanner(store cloudstorage.StoreReader, fr *FileReader) (schema.ConnScanner, error) {
	csv, err := datasource.NewCsvSource(fr.Table, 0, fr.F, fr.Exit)
	if err != nil {
		u.Errorf("Could not open file for csv reading %v", err)
		return nil, err
	}
	return csv, nil
}
