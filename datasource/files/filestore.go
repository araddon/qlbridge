package files

import (
	"fmt"
	"strings"

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

	// FileStoreLoader defines the interface for loading files
	FileStoreLoader func(ss *schema.SchemaSource) (cloudstorage.StoreReader, error)
)

func init() {
	FileStoreLoader = createConfStore
}

func createConfStore(ss *schema.SchemaSource) (cloudstorage.StoreReader, error) {

	if ss == nil || ss.Conf == nil {
		return nil, fmt.Errorf("No config info for files source")
	}

	u.Debugf("json conf:\n%s", ss.Conf.Settings.PrettyJson())
	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	var config *cloudstorage.CloudStoreContext
	conf := ss.Conf.Settings
	storeType := ss.Conf.Settings.String("type")
	switch storeType {
	case "gcs":
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
		config = &c
	case "localfs", "":
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

		config = &c
	default:
		return nil, fmt.Errorf("Unrecognized filestore type %q expected [gcs,localfs]", storeType)
	}
	u.Debugf("creating cloudstore from %#v", config)
	return cloudstorage.NewStore(config)
}
