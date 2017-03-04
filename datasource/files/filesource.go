// Package files implements Cloud Files logic for getting, reading, and converting
// files into databases.   It reads cloud(or local) files, gets lists of tables,
// and can scan through them using distributed query engine.
package files

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"golang.org/x/net/context"

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
	datasource.Register(SourceType, NewFileSource())
}

// FileReaderIterator defines a file source that can page through files
// getting next file from partition
type FileReaderIterator interface {
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
	store          cloudstorage.StoreReader
	fh             FileHandler
	fdbcols        []string
	fdbcolidx      map[string]int
	fdb            schema.Source
	filesTable     string
	tablenames     []string
	tables         map[string]*schema.Table
	files          map[string][]*FileInfo
	path           string
	tablePerFolder bool
	fileType       string // csv, json, proto, customname
	Partitioner    string // random, ??  (date, keyed?)
	PartitionCt    uint64
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
	}
	return nil
}

// Open a connection to given table, partition of Source interface
func (m *FileSource) Open(tableName string) (schema.Conn, error) {
	u.Debugf("Open(%q)", tableName)
	if tableName == m.filesTable {
		return m.fdb.Open(tableName)
	}
	pg, err := m.createPager(tableName, 0)
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

		//u.Debugf("File init %v", string(m.ss.Conf.Settings.PrettyJson()))

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
		u.Debugf("got fh: %T", m.fh)

		// ensure any additional columns are added
		m.fdbcols = append(FileColumns, m.fh.FileAppendColumns()...)

		m.fdbcolidx = make(map[string]int, len(m.fdbcols))
		for i, col := range m.fdbcols {
			m.fdbcolidx[col] = i
		}

		store, err := FileStoreLoader(m.ss)
		if err != nil {
			u.Errorf("Could not create cloudstore %v", err)
			return err
		}
		m.store = store

		m.findTables()

		m.filesTable = fmt.Sprintf("%s_files", m.ss.Name)
		m.tablenames = append(m.tablenames, m.filesTable)

		// We are going to create a DB/Store to be allow the
		// entire list of files to be shown as a meta-table of sorts
		db, err := newStoreSource(m.filesTable, m)
		if err != nil {
			u.Errorf("could not create db %v", err)
			return err
		}
		m.fdb = db

		// for _, table := range m.tablenames {
		// 	_, err := m.Table(table)
		// 	if err != nil {
		// 		u.Errorf("could not create table? %v", err)
		// 	}
		// }
	}
	return nil
}

func (m *FileSource) findTables() {
	q := cloudstorage.Query{"/", m.path, nil}
	q.Sorted()
	folders, err := m.store.Folders(context.Background(), q)
	u.Debugf("folders: %v  err=%v", folders, err)

	for _, folder := range folders {
		parts := strings.Split(strings.ToLower(folder), ".")
		u.Debugf("found table  %q", parts[0])
		m.tablenames = append(m.tablenames, parts[0])
	}
}

// Table satisfys SourceSchema interface to get table schema for given table
func (m *FileSource) Table(tableName string) (*schema.Table, error) {

	u.Debugf("Table(%q) path:%v  %#v", tableName, m.path, m.ss.Conf)
	// We have a special table that is the list of all files
	if m.filesTable == tableName {
		//u.WarnT(10)
		u.Debugf("Table(%q)  %T", tableName, m.fdb)
		return m.fdb.Table(tableName)
	}

	// Check cache for this table
	t, ok := m.tables[tableName]
	if ok {
		return t, nil
	}

	var err error
	// Its possible that the file handle implements schema handling
	if schemaSource, hasSchema := m.fh.(schema.SourceTableSchema); hasSchema {

		t, err = schemaSource.Table(tableName)
		if err != nil {
			u.Errorf("could not get %T P:%p table %q %v", schemaSource, schemaSource, tableName, err)
			return nil, err
		}

	} else {

		u.Infof("call build table")
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
	u.Debugf("introspecting file-table %q for schema type=%q path=%s", tableName, m.fileType, m.path)
	pager, err := m.createPager(tableName, 0)
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

	return t, nil
}

func (m *FileSource) createPager(tableName string, partition int) (*FilePager, error) {

	//u.Debugf("getting file pager %q", tableName)
	pg := NewFilePager(tableName, m)

	pg.RunFetcher()
	return pg, nil
}
