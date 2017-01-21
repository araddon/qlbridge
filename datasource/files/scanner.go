// Package files is a cloud (gcs, s3) and local file datasource that translates
// json, csv, files into appropriate interface for qlbridge DataSource
// so we can run queries.  Provides FileHandler interface to allow
// custom file type handling
package files

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// the global file-scanners registry mutex
	registryMu sync.Mutex
	scanners   = make(map[string]FileHandler)

	// ensuure our csv handler implements FileHandler interface
	_ FileHandler = (*csvFiles)(nil)
)

func init() {
	RegisterFileScanner("csv", &csvFiles{})
}

// FileHandler defines a file-type/format, each format such as
//  csv, json, or a custom-protobuf file type of your choosing
//  would have its on filehandler that knows how to read, parse, scan
//  a file type.
//
// The File Reading, Opening, Listing is a separate layer, see FileSource
//  for the Cloudstorage layer.
//
// So it is a a factory to create Scanners for a speciffic format type such as csv, json
type FileHandler interface {
	// Each time the underlying FileStore layer finds a new file it hands it off
	// to filehandler to determine if it is File or not, and to to extract any
	// metadata such as partition, and parse out fields that may exist in File/Folder path
	File(path string, obj cloudstorage.Object) *FileInfo
	// Create a scanner for particiular file
	Scanner(store cloudstorage.Store, fr *FileReader) (schema.ConnScanner, error)
	// FileAppendColumns provides an method that this file-handler is going to provide additional
	// columns to the files list table, ie we are going to extract column info from the
	// folder paths, file-names which is common.
	// Optional:  may be nil
	FileAppendColumns() []string
	// A file handler may optionally implement schema.SourceSchema  ie, provide table/schema info
}

// RegisterFileScanner Register a file scanner maker available by the provided @scannerType
func RegisterFileScanner(scannerType string, fh FileHandler) {
	if fh == nil {
		panic("File scanners must not be nil")
	}
	scannerType = strings.ToLower(scannerType)
	u.Debugf("global FileHandler register: %v %T FileHandler:%p", scannerType, fh, fh)
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dupe := scanners[scannerType]; dupe {
		panic("Register called twice for FileHandler type " + scannerType)
	}
	scanners[scannerType] = fh
}

func scannerGet(scannerType string) (FileHandler, bool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	scannerType = strings.ToLower(scannerType)
	scanner, ok := scanners[scannerType]
	return scanner, ok
}

// the built in csv filehandler
type csvFiles struct {
}

func (m *csvFiles) FileAppendColumns() []string { return nil }
func (m *csvFiles) File(path string, obj cloudstorage.Object) *FileInfo {
	return fileInterpret(path, obj)
}
func (m *csvFiles) Scanner(store cloudstorage.Store, fr *FileReader) (schema.ConnScanner, error) {
	csv, err := datasource.NewCsvSource(fr.Table, 0, fr.F, fr.Exit)
	if err != nil {
		u.Errorf("Could not open file for csv reading %v", err)
		return nil, err
	}
	return csv, nil
}
