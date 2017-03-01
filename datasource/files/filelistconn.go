package files

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"os"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	_ schema.Source      = (*fileListConn)(nil)
	_ schema.Conn        = (*fileListConn)(nil)
	_ schema.ConnScanner = (*fileListConn)(nil)
)

// fileListConn Source for reading lists of files/names/metadata of files
//
// - readers:      s3, gcs, local-fs
type fileListConn struct {
	f        *FileSource
	table    string
	tbl      *schema.Table
	exit     <-chan bool
	complete bool
	err      error
	rowct    uint64
	columns  []string
	colindex map[string]int
	indexCol int
	filter   expr.Node
}

// NewFileListSource reader
func newFileListConn(table string) (*fileListConn, error) {
	s := &fileListConn{
		table: table,
		exit:  make(<-chan bool, 1),
	}
	return s, nil
}

func (m *fileListConn) Init()                           {}
func (m *fileListConn) Tables() []string                { return []string{m.table} }
func (m *fileListConn) Columns() []string               { return m.columns }
func (m *fileListConn) CreateIterator() schema.Iterator { return m }
func (m *fileListConn) Table(tableName string) (*schema.Table, error) {
	if m.tbl != nil {
		return m.tbl, nil
	}
	return nil, schema.ErrNotFound
}
func (m *fileListConn) loadTable() error {
	tbl := schema.NewTable(strings.ToLower(m.table))
	columns := m.Columns()
	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
		tbl.AddField(schema.NewFieldBase(columns[i], value.StringType, 64, "string"))
	}
	tbl.SetColumns(columns)
	m.tbl = tbl
	return nil
}
func (m *fileListConn) Open(connInfo string) (schema.Conn, error) {
	return newFileListConn(connInfo, f, exit, m.lhSpec)
}

func (m *fileListConn) Close() error {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("close error: %v", r)
		}
	}()
	return nil
}

func (m *fileListConn) MesgChan() <-chan schema.Message {
	iter := m.CreateIterator()
	return SourceIterChannel(iter, m.exit)
}

func (m *fileListConn) Next() schema.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			line, err := m.r.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					m.complete = true
				} else {
					m.err = err
					return nil
				}
			}
			if len(line) == 0 {
				return nil
			}

			m.rowct++

			msg, err := m.lh(line)
			if err != nil {
				m.err = err
				return nil
			}
			return msg
		}
	}
}

func (m *fileListConn) jsonDefaultLine(line []byte) (schema.Message, error) {
	jm := make(map[string]interface{})
	err := json.Unmarshal(line, &jm)
	if err != nil {
		u.Warnf("could not read json line: %v  %s", err, string(line))
		return nil, err
	}
	vals := make([]driver.Value, len(jm))
	keys := make(map[string]int, len(jm))
	i := 0
	for k, val := range jm {
		vals[i] = val
		keys[k] = i
		i++
	}
	u.Debugf("json data: %#v \n%#v", keys, vals)
	return NewSqlDriverMessageMap(m.rowct, vals, keys), nil
}
