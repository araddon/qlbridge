package files

import (
	"database/sql/driver"
	"io"
	"strings"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
)

var (
	// FileColumns are the default columns for the "file" table
	FileColumns = []string{"file", "table", "path", "size", "partition", "updated", "deleted", "filetype"}
)

// FileInfo describes a single file
type FileInfo struct {
	obj        cloudstorage.Object
	Name       string         // Name, Path of file
	Table      string         // Table name this file participates in
	FileType   string         // csv, json, etc
	Partition  int            // which partition
	Size       int            // Content-Length size in bytes
	AppendCols []driver.Value // Additional Column info extracted from file name/folder path
}

// FileReader file info and access to file to supply to ScannerMakers
type FileReader struct {
	*FileInfo
	F    io.ReadCloser // Actual file reader
	Exit chan bool     // exit channel to shutdown reader
}

// Values as as slice, create a row of values describing this file
// for use in sql listing of files
func (m *FileInfo) Values() []driver.Value {
	cols := []driver.Value{
		m.Name,
		m.Table,
		"",
		m.Size,
		m.Partition,
		m.obj.Updated(),
		false,
		m.FileType,
	}
	cols = append(cols, m.AppendCols...)
	return cols
}

// Convert a cloudstorage object to a File
func FileInfoFromCloudObject(path string, obj cloudstorage.Object) *FileInfo {

	tableName := obj.Name()
	if strings.HasPrefix(tableName, "tables") {
		tableName = strings.Replace(tableName, "tables/", "", 1)
	}
	//u.Debugf("tableName: %q  path:%v", tableName, path)
	if !strings.HasPrefix(tableName, path) {
		parts := strings.Split(tableName, path)
		if len(parts) == 2 {
			tableName = parts[1]
		} else {
			u.Warnf("could not get parts? %v", tableName)
		}
	} else {
		// .tables/appearances/appearances.csv
		tableName = strings.Replace(tableName, path+"/", "", 1)
	}

	fi := &FileInfo{Name: obj.Name()}
	//u.Debugf("table:%q  path:%v", tableName, path)

	// Look for Folders
	parts := strings.Split(tableName, "/")
	if len(parts) > 1 {
		fi.Table = parts[0]
	} else {
		parts = strings.Split(tableName, ".")
		if len(parts) > 1 {
			fi.Table = strings.ToLower(parts[0])
		} else {
			u.Errorf("table not readable from filename %q  %#v", tableName, obj)
			return nil
		}
	}

	return fi
}
