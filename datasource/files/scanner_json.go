package files

import (
	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensuure our json handler implements FileHandler interface
	_ FileHandler = (*jsonHandler)(nil)
)

func init() {
	RegisterFileScanner("json", &jsonHandler{})
}

// the built in json filehandler
type jsonHandler struct {
	parser datasource.FileLineHandler
}

// NewJsonHandler creates a json file handler for paging new-line
// delimited rows of json file
func NewJsonHandler(lh datasource.FileLineHandler) FileHandler {
	return &jsonHandler{lh}
}

func (m *jsonHandler) FileAppendColumns() []string { return nil }
func (m *jsonHandler) File(path string, obj cloudstorage.Object) *FileInfo {
	return FileInfoFromCloudObject(path, obj)
}
func (m *jsonHandler) Scanner(store cloudstorage.StoreReader, fr *FileReader) (schema.ConnScanner, error) {
	js, err := datasource.NewJsonSource(fr.Table, fr.F, fr.Exit, m.parser)
	if err != nil {
		u.Errorf("Could not open file for json reading %v", err)
		return nil, err
	}
	return js, nil
}
