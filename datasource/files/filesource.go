// Package files implements Cloud Files logic for getting, readding, and converting
// files into databases.   It reads cloud(or local) files, gets lists of tables,
// and can scan through them using distributed query engine.
package files

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure we implement interfaces
	_ schema.Source = (*FileSource)(nil)

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

	schemaRefreshInterval = time.Minute * 5

	// FileStoreLoader defines the interface for loading files
	FileStoreLoader func(ss *schema.SchemaSource) (cloudstorage.Store, error)
)

const (
	// SourceType is the registered Source name in the qlbridge source registry
	SourceType = "cloudstore"
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(SourceType, NewFileSource())
	FileStoreLoader = createConfStore
}

// PartitionedFileReader defines a file source that can page through files
// getting next file from partition
type PartitionedFileReader interface {
	// NextFile returns io.EOF on last file
	NextFile() (*FileReader, error)
}

// FileSource Source for reading files, and scanning them allowing
//  the contents to be treated as a database, like doing a full
//  table scan in mysql.  But, you can partition across files.
//
// - readers:      s3, gcs, local-fs
// - tablesource:  translate lists of files into tables.  Normally we would have
//                 multiple files per table (ie partitioned, per-day, etc)
// - scanners:     responsible for file-specific
// - files table:  a "table" of all the files from this cloud source
//
type FileSource struct {
	ss             *schema.SchemaSource
	lastLoad       time.Time
	store          cloudstorage.Store
	fh             FileHandler
	fdbcols        []string
	fdb            schema.SourceAll
	fc             schema.ConnAll
	filesTable     string
	tablenames     []string
	tables         map[string]*schema.Table
	files          map[string][]*FileInfo
	path           string
	tablePerFolder bool
	fileType       string // csv, json, proto, customname
	Partitioner    string // random, ??  (date, keyed?)
}

// NewFileSource provides a singleton manager for a particular
// Source Schema, and File-Handler to read/manage all files from
// a source such as gcs folder x, s3 folder y
func NewFileSource() *FileSource {
	m := FileSource{
		tables:     make(map[string]*schema.Table),
		files:      make(map[string][]*FileInfo),
		tablenames: make([]string, 0),
	}
	return &m
}

func (m *FileSource) Init() {}

// Setup the filesource with schema info
func (m *FileSource) Setup(ss *schema.SchemaSource) error {
	m.ss = ss
	if err := m.init(); err != nil {
		return err
	}
	if m.lastLoad.Before(time.Now().Add(-schemaRefreshInterval)) {
		m.lastLoad = time.Now()
		m.loadSchema()
	}
	return nil
}

// Open a connection to given table, partition of Source interface
func (m *FileSource) Open(tableName string) (schema.Conn, error) {
	if tableName == m.filesTable {
		return m.fdb.Open(tableName)
	}
	pg, err := m.createPager(tableName, 0)
	if err != nil {
		u.Errorf("could not get pager: %v", err)
		return nil, err
	}
	pg.RunFetcher()
	return pg, nil
}

// Close this File Source manager
func (m *FileSource) Close() error { return nil }

// Tables for this file-source
func (m *FileSource) Tables() []string { return m.tablenames }
func (m *FileSource) init() error {
	if m.store == nil {

		conf := m.ss.Conf.Settings
		if tablePath := conf.String("path"); tablePath != "" {
			m.path = tablePath
		}
		if fileType := conf.String("format"); fileType != "" {
			m.fileType = fileType
		} else {
			m.fileType = "csv"
		}
		if partitioner := conf.String("partitioner"); partitioner != "" {
			m.Partitioner = partitioner
		}

		// TODO:   if no m.fileType inspect file name?
		fileHandler, exists := scannerGet(m.fileType)
		if !exists || fileHandler == nil {
			return fmt.Errorf("Could not find scanner for filetype %q", m.fileType)
		}
		m.fh = fileHandler

		// ensure any additional columns are added
		m.fdbcols = append(FileColumns, m.fh.FileAppendColumns()...)

		store, err := FileStoreLoader(m.ss)
		if err != nil {
			u.Errorf("Could not create cloudstore %v", err)
			return err
		}
		m.store = store

		m.filesTable = fmt.Sprintf("%s_files", m.ss.Name)
		m.tablenames = append(m.tablenames, m.filesTable)

		// We are going to create a DB/Store to be allow the
		// entire list of files to be shown as a meta-table of sorts
		db, err := memdb.NewMemDbForSchema(m.filesTable, m.fdbcols)
		if err != nil {
			u.Errorf("could not create db %v", err)
			return err
		}
		m.fdb = db
		c, err := db.Open(m.filesTable)
		if err != nil {
			u.Errorf("Could not create db %v", err)
			return err
		}
		ca, ok := c.(schema.ConnAll)
		if !ok {
			u.Warnf("Crap, wrong conn type: %T", c)
			return fmt.Errorf("Expected ConnAll but got %T", c)
		}
		m.fc = ca

	}
	return nil
}

func fileInterpret(path string, obj cloudstorage.Object) *FileInfo {

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

	//u.Debugf("table:%q  path:%v", tableName, path)

	// Look for Folders
	parts := strings.Split(tableName, "/")
	if len(parts) > 1 {
		return &FileInfo{Table: parts[0], Name: obj.Name()}
	}
	parts = strings.Split(tableName, ".")
	if len(parts) > 1 {
		tableName := strings.ToLower(parts[0])
		return &FileInfo{Table: tableName, Name: obj.Name()}
	}
	u.Errorf("table not readable from filename %q  %#v", tableName, obj)
	return nil
}

func (m *FileSource) loadSchema() {

	u.Infof("%p  load schema path:%q   %#v", m, m.path, m.ss.Conf)

	q := cloudstorage.Query{Prefix: m.path}
	q.Sorted() // We need to sort this by reverse to go back to front?
	objs, err := m.store.List(q)
	if err != nil {
		u.Errorf("could not open list err=%v", err)
		return
	}
	nextPartId := 0

	u.Infof("how many files? %v", len(objs))

	for _, obj := range objs {
		u.Debugf("obj %#v", obj)
		fi := m.fh.File(m.path, obj)
		if fi == nil || fi.Name == "" {
			continue
		}
		fi.obj = obj
		fi.FileType = m.fileType

		if _, tableExists := m.files[fi.Table]; !tableExists {
			u.Debugf("%p found new table: %q", m, fi.Table)
			m.files[fi.Table] = make([]*FileInfo, 0)
			m.tablenames = append(m.tablenames, fi.Table)
			nextPartId = 0
		}
		if fi.Partition == 0 && m.ss.Conf.PartitionCt > 0 {
			// assign a partition
			fi.Partition = nextPartId
			//u.Debugf("%d found file part:%d  %s", len(m.files[fi.Table]), fi.Partition, fi.Name)
			nextPartId++
			if nextPartId >= m.ss.Conf.PartitionCt {
				nextPartId = 0
			}
		}
		m.addFile(fi)
	}
}

func (m *FileSource) addFile(fi *FileInfo) {
	m.files[fi.Table] = append(m.files[fi.Table], fi)
	_, err := m.fc.Put(nil, nil, fi.Values())
	if err != nil {
		u.Warnf("could not register file")
	}
}

// Table satisfys Source Schema interface to get table schema for given table
func (m *FileSource) Table(tableName string) (*schema.Table, error) {

	// We have a special table that is the list of all files
	if m.filesTable == tableName {
		return m.fdb.Table(tableName)
	}

	var err error
	//u.Debugf("%p Table(%q)", m, tableName)
	t, ok := m.tables[tableName]
	if ok {
		return t, nil
	}

	// Its possible that the file handle implements schema handling
	if schemaSource, hasSchema := m.fh.(schema.SourceTableSchema); hasSchema {

		t, err = schemaSource.Table(tableName)
		if err != nil {
			u.Errorf("could not get %T P:%p table %q %v", schemaSource, schemaSource, tableName, err)
			return nil, err
		}

	} else {

		// Source doesn't implement Schema Handling so we are going to get
		//  a scanner and introspect some rows
		t, err = m.buildTable(tableName)
		if err != nil {
			return nil, err
		}

	}

	if t == nil {
		u.Warnf("No table found? %q", tableName)
		return nil, fmt.Errorf("Missing table for %q", tableName)
	}

	m.tables[tableName] = t
	//u.Debugf("%p Table(%q)", m, tableName)
	return t, nil
}

func (m *FileSource) buildTable(tableName string) (*schema.Table, error) {

	// Since we don't have a table schema, lets create one via introspection
	u.Debugf("introspecting file-table %q for schema type=%q", tableName, m.fileType)
	pager, err := m.createPager(tableName, 0)
	if err != nil {
		u.Errorf("could not find scanner for table %q table err:%v", tableName, err)
		return nil, err
	}

	// Since we aren't calling WalkExec, we need to get the data
	pager.RunFetcher()

	scanner, err := pager.NextScanner()
	if err != nil {
		u.Errorf("what, no scanner? table=%q  err=%v", tableName, err)
		return nil, err
	}

	colScanner, hasColumns := scanner.(schema.ConnColumns)
	if !hasColumns {
		return nil, fmt.Errorf("Must have Columns to Introspect Tables")
	}

	t := schema.NewTable(tableName)
	t.SetColumns(colScanner.Columns())

	// we are going to look at ~10 rows to create schema for it
	if err = datasource.IntrospectTable(t, scanner); err != nil {
		u.Errorf("Could not introspect schema %v", err)
		return nil, err
	}

	return t, nil
}

func (m *FileSource) createPager(tableName string, partition int) (*FilePager, error) {

	// Read the object from cloud storage
	files := m.files[tableName]
	if len(files) == 0 {
		return nil, schema.ErrNotFound
	}

	//u.Debugf("getting file pager %q", tableName)
	pg := NewFilePager(tableName, m)
	pg.files = files
	return pg, nil
}

func createConfStore(ss *schema.SchemaSource) (cloudstorage.Store, error) {

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
