// Package files implements Cloud Files logic for getting, reading, and converting
// files into databases.   It reads cloud(or local) files, gets lists of tables,
// and can scan through them using distributed query engine.
package files

import (
	"fmt"
	"path"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/lytics/cloudstorage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure we implement interfaces
	_ schema.Source = (*FileSource)(nil)

	schemaRefreshInterval = time.Minute * 5
)

const (
	// SourceType is the registered Source name in the qlbridge source registry
	SourceType = "cloudstore"
)

func init() {
	// We need to register our DataSource provider here
	schema.RegisterSourceType(SourceType, NewFileSource())
}

// FileReaderIterator defines a file source that can page through files
// getting next file from partition
type FileReaderIterator interface {
	// NextFile returns io.EOF on last file
	NextFile() (*FileReader, error)
}

type Partitioner func(uint64, *FileInfo) int

func SipPartitioner(partitionCt uint64, fi *FileInfo) int {
	hashU64 := siphash.Hash(0, 1, []byte(fi.Name))
	return int(hashU64 % partitionCt)
}

// FileSource Source for reading files, and scanning them allowing
// the contents to be treated as a database, like doing a full
// table scan in mysql.  But, you can partition across files.
//
// - readers:      gcs, local-fs
// - tablesource:  translate lists of files into tables.  Normally we would have
//                 multiple files per table (ie partitioned, per-day, etc)
// - scanners:     responsible for file-specific
// - files table:  a "table" of all the files from this cloud source
//
type FileSource struct {
	ss             *schema.Schema
	lastLoad       time.Time
	store          cloudstorage.StoreReader
	fh             FileHandler
	fdbcols        []string
	fdbcolidx      map[string]int
	fdb            schema.Source
	filesTable     string
	tablenames     []string
	tableSchemas   map[string]*schema.Table
	tables         map[string]*FileTable
	path           string
	tablePerFolder bool
	fileType       string // csv, json, proto, customname
	Partitioner    string // random, ??  (date, keyed?)
	partitionFunc  Partitioner
	partitionCt    uint64
}

// NewFileSource provides a singleton manager for a particular
// Source Schema, and File-Handler to read/manage all files from
// a source such as gcs folder x, s3 folder y
func NewFileSource() *FileSource {
	m := FileSource{
		tableSchemas:  make(map[string]*schema.Table),
		tables:        make(map[string]*FileTable),
		tablenames:    make([]string, 0),
		partitionFunc: SipPartitioner,
	}
	return &m
}

func (m *FileSource) Init() {}

// Setup the filesource with schema info
func (m *FileSource) Setup(ss *schema.Schema) error {
	m.ss = ss
	if err := m.init(); err != nil {
		return err
	}
	if m.lastLoad.Before(time.Now().Add(-schemaRefreshInterval)) {
		m.lastLoad = time.Now()
	}
	m.partitionCt = uint64(m.ss.Conf.PartitionCt)

	m.partitionFunc = SipPartitioner
	return nil
}

// Open a connection to given table, partition of Source interface
func (m *FileSource) Open(tableName string) (schema.Conn, error) {
	//u.Debugf("Open(%q)", tableName)
	if tableName == m.filesTable {
		return m.fdb.Open(tableName)
	}
	pg, err := m.createPager(tableName, 0, 0)
	if err != nil {
		u.Errorf("could not get pager: %v", err)
		return nil, err
	}
	return pg, nil
}

// Close this File Source manager
func (m *FileSource) Close() error { return nil }

// Tables for this file-source
func (m *FileSource) Tables() []string { return m.tablenames }
func (m *FileSource) init() error {
	if m.store == nil {

		// u.Debugf("File init %v", string(m.ss.Conf.Settings.PrettyJson()))

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

		store, err := FileStoreLoader(m.ss)
		if err != nil {
			u.Errorf("Could not create filestore for source %s err=%v", m.ss.Name, err)
			return err
		}
		m.store = store

		fileHandler, exists := scannerGet(m.fileType)
		if !exists || fileHandler == nil {
			return fmt.Errorf("Could not find scanner for filetype %q", m.fileType)
		}
		if err := fileHandler.Init(store, m.ss); err != nil {
			u.Errorf("Could not create filehandler for %s type=%q err=%v", m.ss.Name, m.fileType, err)
			return err
		}
		m.fh = fileHandler
		// u.Debugf("got fh: %T", m.fh)

		// ensure any additional columns are added
		m.fdbcols = append(FileColumns, m.fh.FileAppendColumns()...)

		m.fdbcolidx = make(map[string]int, len(m.fdbcols))
		for i, col := range m.fdbcols {
			m.fdbcolidx[col] = i
		}

		m.findTables()

		m.filesTable = fmt.Sprintf("%s_files", m.ss.Name)
		m.tablenames = append(m.tablenames, m.filesTable)

		// We are going to create a DB/Store to be allow the
		// entire list of files to be shown as a meta-table
		db, err := newStoreSource(m.filesTable, m)
		if err != nil {
			u.Errorf("could not create db %v", err)
			return err
		}
		m.fdb = db
	}
	return nil
}

func (m *FileSource) File(o cloudstorage.Object) *FileInfo {
	fi := m.fh.File(m.path, o)
	if fi == nil {
		// u.Debugf("ignoring file, path:%v  %q  is nil", m.path, o.Name())
		return nil
	}
	if m.partitionCt > 0 {
		fi.Partition = m.partitionFunc(m.partitionCt, fi)
	}
	//u.Debugf("File(%q)  path=%q", o.Name(), m.path)
	return fi
}

func (m *FileSource) findTables() error {

	// FileHandlers may optionally provide their own
	// list of files, as deciphering folder, file structure
	// isn't always obvious
	if th, ok := m.fh.(FileHandlerTables); ok {
		tables := th.Tables()
		for _, tbl := range tables {
			m.tablenames = append(m.tablenames, tbl.Table)
			m.tables[tbl.Table] = tbl
		}
		return nil
	}

	q := cloudstorage.Query{Delimiter: "/", Prefix: m.path}
	q.Sorted()
	folders, err := m.store.Folders(context.Background(), q)
	if err != nil {
		u.Errorf("could not read files %v", err)
		return err
	}
	if len(folders) == 0 {
		return m.findTablesFromFileNames()
	}

	// u.Debugf("from path=%q  folders: %v  err=%v", m.path, folders, err)
	for _, table := range folders {
		table = path.Base(table)
		table = strings.ToLower(table)
		m.tables[table] = &FileTable{Table: table, PartialPath: table}
		m.tablenames = append(m.tablenames, table)
	}

	return nil
}

func (m *FileSource) findTablesFromFileNames() error {

	q := cloudstorage.Query{Delimiter: "", Prefix: m.path}
	q.Sorted()
	ctx := context.Background()
	iter, err := m.store.Objects(ctx, q)
	if err != nil {
		return err
	}

	u.Infof("findTablesFromFileNames  from path=%q", m.path)

	tables := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			// If has been closed
			return ctx.Err()
		default:
			o, err := iter.Next()
			if err == iterator.Done {
				return nil
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}

			fi := m.fh.File(m.path, o)
			if fi == nil || fi.Name == "" {
				u.Warnf("no file?? %#v", o)
				continue
			}

			// u.Debugf("File %s", fi)
			if fi.Table != "" {
				if _, exists := tables[fi.Table]; !exists {
					tables[fi.Table] = true
					u.Warnf("found new table path=%q table=%q pp=%q name=%q", m.path, fi.Table, fi.PartialPath, fi.Name)
					m.tables[fi.Table] = &FileTable{Table: fi.Table, PartialPath: fi.PartialPath}
					m.tablenames = append(m.tablenames, fi.Table)
				}
			}
		}
	}
}

// Table satisfys SourceSchema interface to get table schema for given table
func (m *FileSource) Table(tableName string) (*schema.Table, error) {

	//u.Debugf("Table(%q) path:%v  %#v", tableName, m.path, m.ss.Conf)
	// We have a special table that is the list of all files
	if m.filesTable == tableName {
		return m.fdb.Table(tableName)
	}

	// Check cache for this table
	t, ok := m.tableSchemas[tableName]
	if ok {
		return t, nil
	}

	var err error
	// Its possible that the file handle implements schema handling
	if schemaSource, hasSchema := m.fh.(schema.SourceTableSchema); hasSchema {
		t, err = schemaSource.Table(tableName)
		if err != nil {
			u.Errorf("could not get %T table %q %v", schemaSource, tableName, err)
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
		return nil, fmt.Errorf("Missing table for %q", tableName)
	}

	m.tableSchemas[tableName] = t
	//u.Debugf("%p Table(%q) cols=%v", m, tableName, t.Columns())
	return t, nil
}

func (m *FileSource) buildTable(tableName string) (*schema.Table, error) {

	// Since we don't have a table schema, lets create one via introspection
	//u.Debugf("introspecting file-table %q for schema type=%q path=%s", tableName, m.fileType, m.path)
	pager, err := m.createPager(tableName, 0, 1)
	if err != nil {
		u.Errorf("could not find scanner for table %q table err:%v", tableName, err)
		return nil, err
	}

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
	//u.Infof("built table %v %v", tableName, t.Columns())
	return t, nil
}

func (m *FileSource) createPager(tableName string, partition, limit int) (*FilePager, error) {

	pg := NewFilePager(tableName, m)
	pg.Limit = limit
	pg.RunFetcher()
	return pg, nil
}
