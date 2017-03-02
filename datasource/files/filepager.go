package files

import (
	"io"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/lytics/cloudstorage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	// Our file-pager wraps our file-scanners to move onto next file
	_ FileReaderIterator  = (*FilePager)(nil)
	_ schema.ConnScanner  = (*FilePager)(nil)
	_ exec.ExecutorSource = (*FilePager)(nil)

	// Default file queue size to buffer by pager
	FileBufferSize = 5
)

type Partitioner func(uint64, string) int

func SipPartitioner(partitionCt uint64, name string) int {
	hashU64 := siphash.Hash(0, 1, []byte(name))
	return int(hashU64 % partitionCt)
}

// FilePager acts like a Partitionied Data Source Conn, wrapping underlying FileSource
// and paging through list of files and only scanning those that match
// this pagers partition
// - by default the partition is -1 which means no partitioning
type FilePager struct {
	rowct       int64
	table       string
	exit        chan bool
	err         error
	closed      bool
	fs          *FileSource
	readers     chan (*FileReader)
	partition   *schema.Partition
	partid      int
	tbl         *schema.Table
	p           *plan.Source
	partitioner Partitioner
	schema.ConnScanner
}

// NewFilePager creates default new FilePager
func NewFilePager(tableName string, fs *FileSource) *FilePager {
	fp := &FilePager{
		fs:          fs,
		table:       tableName,
		exit:        make(chan bool),
		readers:     make(chan *FileReader, FileBufferSize),
		partid:      -1,
		partitioner: SipPartitioner,
	}
	return fp
}

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

	select {
	case <-m.exit:
		// See if exit was called
		return nil, iterator.Done
	case fr := <-m.readers:
		if fr == nil {
			return nil, iterator.Done
		}
		return fr, nil
	}
}

func (m *FilePager) RunFetcher() {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("panic in fetcher %v", r)
		}
	}()
	go m.fetcher()
}

// fetcher process run in a go-routine to pre-fetch files
// assuming we should keep n in buffer
func (m *FilePager) fetcher() {

	q := cloudstorage.Query{"", m.fs.path, nil}
	q.Sorted()
	ctx := context.Background()
	iter := m.fs.store.Objects(ctx, q)

	for {
		select {
		case <-m.exit:
			// was closed
			return
		case <-ctx.Done():
			// If has been closed
			return
		default:
			o, err := iter.Next()
			if err == iterator.Done {
				return
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				// Return to user
				return
			}
			m.rowct++

			fi := m.fs.fh.File(m.fs.path, o)
			if fi == nil || fi.Name == "" {
				continue
			}
			if m.fs.PartitionCt >= 0 {
				fi.Partition = m.partitioner(m.fs.PartitionCt, fi.Name)
			}

			filePath := fi.Name
			if strings.HasPrefix(fi.Name, "tables") {
				filePath = strings.Replace(fi.Name, "tables/", "", 1)
			}
			//u.Debugf("%p opening: partition:%v desiredpart:%v file: %q ", m, fi.Partition, m.partid, fi.Name)
			obj, err := m.fs.store.Get(filePath)
			if err != nil {
				filePath := fi.Name
				if strings.HasPrefix(fi.Name, "tables") {
					filePath = strings.Replace(fi.Name, "tables/", "", 1)
				}
				obj, err = m.fs.store.Get(filePath)
				if err != nil {
					u.Errorf("could not read %q err=%v", fi.Name, err)
					return
				}
			}

			start := time.Now()
			f, err := obj.Open(cloudstorage.ReadOnly)
			if err != nil {
				u.Errorf("could not read %q table %v", m.table, err)
				return
			}
			u.Debugf("found file: %s   took:%vms", obj.Name(), time.Now().Sub(start).Nanoseconds()/1e6)

			fr := &FileReader{
				F:        f,
				Exit:     make(chan bool),
				FileInfo: fi,
			}

			// This will back-pressure after we reach our queue size
			m.readers <- fr

			// partid = -1 means we are not partitioning
			if m.partid < 0 || fi.Partition == m.partid {
				break
			}
		}
	}

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
	//close(m.exit)
	return nil
}
