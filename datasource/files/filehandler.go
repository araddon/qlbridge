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

	"github.com/araddon/qlbridge/schema"
)

var (
	// the global file-scanners registry mutex
	registryMu sync.Mutex
	scanners   = make(map[string]FileHandler)
)

// FileHandler defines a file-type/format, each format such as
// csv, json, or a custom-protobuf file type of your choosing
// would have its on filehandler that knows how to read, parse, scan
// a file type.
//
// The File Reading, Opening, Listing is a separate layer, see FileSource
// for the Cloudstorage layer.
//
// So it is a a factory to create Scanners for a speciffic format type such as csv, json
type FileHandler interface {
	// Each time the underlying FileStore finds a new file it hands it off
	// to filehandler to determine if it is File or not (directory?), and to to extract any
	// metadata such as partition, and parse out fields that may exist in File/Folder path
	File(path string, obj cloudstorage.Object) *FileInfo
	// Create a scanner for particiular file
	Scanner(store cloudstorage.StoreReader, fr *FileReader) (schema.ConnScanner, error)
	// FileAppendColumns provides a method that this file-handler is going to provide additional
	// columns to the files list table, ie we are going to extract column info from the
	// folder paths, file-names.
	// For example:   `tables/appearances/2017/appearances.csv may extract the "2017" as "year"
	// and append that as column to all rows.
	// Optional:  may be nil
	FileAppendColumns() []string
}

// RegisterFileScanner Register a FileHandler available by the provided @scannerType
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
