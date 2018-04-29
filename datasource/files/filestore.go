package files

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/google"
	"github.com/lytics/cloudstorage/localfs"

	"github.com/araddon/qlbridge/schema"
)

var (
	// TODO:   move to test files
	localFilesConfig = cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "./tables",
		TmpDir:     "/tmp/localcache",
	}

	// TODO:   complete manufacture this from config
	gcsConfig = cloudstorage.Config{
		Type:       google.StoreType,
		AuthMethod: google.AuthGCEDefaultOAuthToken,
		Project:    "lytics-dev",
		Bucket:     "lytics-dataux-tests",
		TmpDir:     "/tmp/localcache",
	}
)

var (
	// the global filestore registry mutex
	fileStoreMu sync.Mutex
	fileStores  = make(map[string]FileStoreCreator)
)

func init() {
	RegisterFileStore("gcs", createGCSFileStore)
	RegisterFileStore("localfs", createLocalFileStore)
}

// FileStoreLoader defines the interface for loading files
func FileStoreLoader(ss *schema.Schema) (cloudstorage.StoreReader, error) {
	if ss == nil || ss.Conf == nil {
		return nil, fmt.Errorf("No config info for files source for %v", ss)
	}

	//u.Debugf("json conf:\n%s", ss.Conf.Settings.PrettyJson())
	storeType := ss.Conf.Settings.String("type")
	if storeType == "" {
		return nil, fmt.Errorf("Expected 'type' in File Store definition conf")
	}

	fileStoreMu.Lock()
	storeType = strings.ToLower(storeType)
	fs, ok := fileStores[storeType]
	fileStoreMu.Unlock()

	if !ok {
		return nil, fmt.Errorf("Unrecognized filestore type %q expected [gcs,localfs]", storeType)
	}

	return fs(ss)
}

// FileStoreCreator defines a Factory type for creating FileStore
type FileStoreCreator func(*schema.Schema) (FileStore, error)

// FileStore Defines handler for reading Files, understanding
// folders and how to create scanners/formatters for files.
// Created by FileStoreCreator
//
// FileStoreCreator(schema) -> FileStore
//           FileStore.Objects() -> File
//                    FileHandler(File) -> FileScanner
//                         FileScanner.Next() ->  Row
type FileStore interface {
	cloudstorage.StoreReader
}

// RegisterFileStore global registry for Registering
// implementations of FileStore factories of the provided @storeType
func RegisterFileStore(storeType string, fs FileStoreCreator) {
	if fs == nil {
		panic("FileStore must not be nil")
	}
	storeType = strings.ToLower(storeType)
	u.Debugf("global FileStore register: %v %T FileStore:%p", storeType, fs, fs)
	fileStoreMu.Lock()
	defer fileStoreMu.Unlock()
	if _, dupe := fileStores[storeType]; dupe {
		panic("Register called twice for FileStore type " + storeType)
	}
	fileStores[storeType] = fs
}

func createGCSFileStore(ss *schema.Schema) (FileStore, error) {

	conf := ss.Conf.Settings

	c := gcsConfig
	if proj := conf.String("project"); proj != "" {
		c.Project = proj
	}
	if bkt := conf.String("bucket"); bkt != "" {
		bktl := strings.ToLower(bkt)
		// We don't actually need the gs:// because cloudstore does it
		if strings.HasPrefix(bktl, "gs://") && len(bkt) > 5 {
			bkt = bkt[5:]
		}
		c.Bucket = bkt
	}
	if jwt := conf.String("jwt"); jwt != "" {
		c.JwtFile = jwt
	}
	return cloudstorage.NewStore(&c)
}

func createLocalFileStore(ss *schema.Schema) (FileStore, error) {

	conf := ss.Conf.Settings

	localPath := conf.String("localpath")
	if localPath == "" {
		localPath = "./tables/"
	}
	c := cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    localPath,
		TmpDir:     "/tmp/localcache",
	}
	if c.LocalFS == "" {
		return nil, fmt.Errorf(`"localfs" filestore requires a {"settings":{"localpath":"/path/to/files"}} to local files`)
	}
	return cloudstorage.NewStore(&c)
}
