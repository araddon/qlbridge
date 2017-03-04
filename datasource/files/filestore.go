package files

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"

	"github.com/araddon/qlbridge/schema"
)

var (
	// TODO:   move to test files
	localFilesConfig = cloudstorage.CloudStoreContext{
		LogggingContext: "qlbridge",
		TokenSource:     cloudstorage.LocalFileSource,
		LocalFS:         "./tables",
		TmpDir:          "/tmp/localcache",
	}

	// TODO:   complete manufacture this from config
	gcsConfig = cloudstorage.CloudStoreContext{
		LogggingContext: "qlbridge",
		TokenSource:     cloudstorage.GCEDefaultOAuthToken,
		Project:         "lytics-dev",
		Bucket:          "lytics-dataux-tests",
		TmpDir:          "/tmp/localcache",
	}
)

var (
	// the global filestore registry mutex
	fileStoreMu sync.Mutex
	fileStores  = make(map[string]FileStore)
)

func init() {
	RegisterFileStore("gcs", createGCSFileStore)
	RegisterFileStore("localfs", createLocalFileStore)
}

// FileStoreLoader defines the interface for loading files
func FileStoreLoader(ss *schema.SchemaSource) (cloudstorage.StoreReader, error) {
	return createFileStore(ss)
}

// FileStore defines a Factory type for creating StoreReaders
//
// FileStore creates StoreReader, StoreReader reads lists of files, and opens files.
//
// FileStore.Open() ->
//           StoreReader.Objects() -> File
//                    FileHandler(File) -> FileScanner
//                         FileScanner.Next() ->  Row
type FileStore func(*schema.SchemaSource) (cloudstorage.StoreReader, error)

// RegisterFileStore global registry for Registering
// implementations of FileStore factories of the provided @storeType
func RegisterFileStore(storeType string, fs FileStore) {
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

func createFileStore(ss *schema.SchemaSource) (cloudstorage.StoreReader, error) {

	if ss == nil || ss.Conf == nil {
		return nil, fmt.Errorf("No config info for files source")
	}

	u.Debugf("json conf:\n%s", ss.Conf.Settings.PrettyJson())
	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	storeType := ss.Conf.Settings.String("type")
	if storeType == "" {
		return nil, fmt.Errorf("Expected 'type' in File Store defintion conf")
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

func createGCSFileStore(ss *schema.SchemaSource) (cloudstorage.StoreReader, error) {

	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

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

func createLocalFileStore(ss *schema.SchemaSource) (cloudstorage.StoreReader, error) {

	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	conf := ss.Conf.Settings

	localPath := conf.String("localpath")
	if localPath == "" {
		localPath = "./tables/"
	}
	c := cloudstorage.CloudStoreContext{
		LogggingContext: "localfiles",
		TokenSource:     cloudstorage.LocalFileSource,
		LocalFS:         localPath,
		TmpDir:          "/tmp/localcache",
	}
	if c.LocalFS == "" {
		return nil, fmt.Errorf(`"localfs" filestore requires a {"settings":{"localpath":"/path/to/files"}} to local files`)
	}
	//os.RemoveAll("/tmp/localcache")

	return cloudstorage.NewStore(&c)
}
