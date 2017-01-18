package files

import (
	"io"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/lytics/cloudstorage"
)

var (
	// Our file-pager wraps our file-scanners to move onto next file
	_ PartitionedFileReader = (*FilePager)(nil)
	_ schema.ConnScanner    = (*FilePager)(nil)
	_ exec.ExecutorSource   = (*FilePager)(nil)
	//_ exec.RequiresContext  = (*FilePager)(nil)
)

// FilePager acts like a Partitionied Data Source Conn, wrapping underlying FileSource
// and paging through list of files and only scanning those that match
// this pagers partition
// - by default the partition is -1 which means no partitioning
type FilePager struct {
	cursor    int
	rowct     int64
	table     string
	exit      chan bool
	closed    bool
	fs        *FileSource
	files     []*FileInfo
	partition *schema.Partition
	partid    int
	tbl       *schema.Table
	p         *plan.Source
	schema.ConnScanner
}

// NewFilePager creates default new FilePager
func NewFilePager(tableName string, fs *FileSource) *FilePager {
	return &FilePager{
		fs:     fs,
		table:  tableName,
		exit:   make(chan bool),
		partid: -1,
	}
}

// func (m *FilePager) SetContext(ctx *plan.Context) {
// 	m.ctx = ctx
// }

// WalkExecSource Provide ability to implement a source plan for execution
func (m *FilePager) WalkExecSource(p *plan.Source) (exec.Task, error) {

	if m.p == nil {
		m.p = p
		//u.Debugf("%p custom? %v", m, p.Custom)
		if partitionId, ok := p.Custom.IntSafe("partition"); ok {
			m.partid = partitionId
		}
	} else {
		u.Warnf("not nil?  custom? %v", p.Custom)
	}

	//u.Debugf("WalkExecSource():  %T  %#v", p, p)
	return exec.NewSource(p.Context(), p)
}

// Columns part of Conn interface for providing columns for this table/conn
func (m *FilePager) Columns() []string {
	if m.tbl == nil {
		t, err := m.fs.Table(m.table)
		if err != nil {
			u.Warnf("error getting table? %v", err)
			return nil
		}
		m.tbl = t
	}
	return m.tbl.Columns()
}

// NextScanner provides the next scanner assuming that each scanner
// representas different file, and multiple files for single source
func (m *FilePager) NextScanner() (schema.ConnScanner, error) {

	fr, err := m.NextFile()
	//u.Debugf("%p next file? fr:%+v  err=%v", m, fr, err)
	if err == io.EOF {
		return nil, err
	}

	//u.Debugf("%p next file partid:%d  custom:%v %v", m, m.partid, m.p.Custom, fr.Name)
	scanner, err := m.fs.fh.Scanner(m.fs.store, fr)
	if err != nil {
		u.Errorf("Could not open file scanner %v err=%v", m.fs.fileType, err)
		return nil, err
	}
	m.ConnScanner = scanner
	return scanner, err
}

// NextFile gets next file
func (m *FilePager) NextFile() (*FileReader, error) {

	var fi *FileInfo
	//u.Debugf("%p file ct=%d partid:%d", m, len(m.files), m.partid)
	for {
		if m.cursor >= len(m.files) {
			return nil, io.EOF
		}
		fi = m.files[m.cursor]
		m.cursor++
		// partid = -1 means we are not partitioning
		if m.partid < 0 || fi.Partition == m.partid {
			break
		}
	}

	//u.Debugf("%p opening: partition:%v desiredpart:%v file: %q ", m, fi.Partition, m.partid, fi.Name)
	obj, err := m.fs.store.Get(fi.Name)
	if err != nil {
		u.Errorf("could not read %q table %v", err)
		return nil, err
	}

	f, err := obj.Open(cloudstorage.ReadOnly)
	if err != nil {
		u.Errorf("could not read %q table %v", m.table, err)
		return nil, err
	}
	//u.Infof("found file: %s   %p", obj.Name(), f)

	fr := &FileReader{
		F:        f,
		Exit:     make(chan bool),
		FileInfo: fi,
	}
	return fr, nil
}

// Next iterator for next message, wraps the file Scanner, Next file abstractions
func (m *FilePager) Next() schema.Message {
	if m.ConnScanner == nil {
		m.NextScanner()
	}
	for {
		if m.closed {
			return nil
		}
		msg := m.ConnScanner.Next()
		if msg == nil {
			// Kind of crap api, side-effect method? uck
			_, err := m.NextScanner()
			if err != nil && err == io.EOF {
				// Truly was last file in partition
				return nil
			} else if err != nil {
				u.Errorf("unexpected end of scan %v", err)
				return nil
			}
			// now that we have a new scanner, lets try again
			if m.ConnScanner != nil {
				//u.Debugf("next page")
				msg = m.ConnScanner.Next()
			}
		}

		m.rowct++
		return msg
	}
}

// Close this connection/pager
func (m *FilePager) Close() error {
	m.closed = true
	return nil
}
