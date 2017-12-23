package datasource

import (
	"bufio"
	"bytes"
	"compress/gzip"
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
	_ schema.Source      = (*JsonSource)(nil)
	_ schema.Conn        = (*JsonSource)(nil)
	_ schema.ConnScanner = (*JsonSource)(nil)
)

type FileLineHandler func(line []byte) (schema.Message, error)

// JsonSource implements qlbridge schema DataSource, SourceConn, Scanner
// to allow new line delimited json files to be full featured databases.
// - very, very naive scanner, forward only single pass
// - can open a file with .Open()
// - not thread-safe
// - does not implement write operations
type JsonSource struct {
	table    string
	tbl      *schema.Table
	exit     <-chan bool
	complete bool
	err      error
	r        *bufio.Reader
	gz       *gzip.Reader
	rc       io.ReadCloser
	rowct    uint64
	lhSpec   FileLineHandler
	lh       FileLineHandler
	columns  []string
	colindex map[string]int
	indexCol int
	filter   expr.Node
}

// NewJsonSource reader assumes we are getting NEW LINE delimted json file
// - optionally may be gzipped
func NewJsonSource(table string, rc io.ReadCloser, exit <-chan bool, lh FileLineHandler) (*JsonSource, error) {

	js := &JsonSource{
		table:  table,
		exit:   exit,
		rc:     rc,
		lhSpec: lh,
		lh:     lh,
	}

	buf := bufio.NewReader(rc)
	first2, err := buf.Peek(2)
	if err != nil {
		u.Warnf("error opening bufio.peek for json reader: %v", err)
		return nil, err
	}

	// Gzip Files have these 2 byte prefix
	if err == nil && len(first2) == 2 && bytes.Equal(first2, []byte{'\x1F', '\x8B'}) {
		gr, err := gzip.NewReader(buf)
		if err != nil {
			u.Warnf("could not open gzip reader: %v", err)
			return nil, err
		}
		js.gz = gr
		js.r = bufio.NewReader(gr)
	} else {
		js.r = buf
	}

	if lh == nil {
		js.lh = js.jsonDefaultLine
	}

	//m.loadTable()
	return js, nil
}

func (m *JsonSource) Init()                           {}
func (m *JsonSource) Setup(*schema.Schema) error      { return nil }
func (m *JsonSource) Tables() []string                { return []string{m.table} }
func (m *JsonSource) Columns() []string               { return m.columns }
func (m *JsonSource) CreateIterator() schema.Iterator { return m }
func (m *JsonSource) Table(tableName string) (*schema.Table, error) {
	if m.tbl != nil {
		return m.tbl, nil
	}
	return nil, schema.ErrNotFound
}
func (m *JsonSource) loadTable() error {
	tbl := schema.NewTable(strings.ToLower(m.table))
	columns := m.Columns()
	for i := range columns {
		columns[i] = strings.ToLower(columns[i])
		tbl.AddField(schema.NewFieldBase(columns[i], value.StringType, 64, "string"))
	}
	tbl.SetColumns(columns)
	m.tbl = tbl
	return nil
}
func (m *JsonSource) Open(connInfo string) (schema.Conn, error) {
	if connInfo == "stdio" || connInfo == "stdin" {
		connInfo = "/dev/stdin"
	}
	f, err := os.Open(connInfo)
	if err != nil {
		return nil, err
	}
	exit := make(<-chan bool, 1)
	return NewJsonSource(connInfo, f, exit, m.lhSpec)
}

func (m *JsonSource) Close() error {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("close error: %v", r)
		}
	}()
	if m.gz != nil {
		m.gz.Close()
	}
	if m.rc != nil {
		m.rc.Close()
	}
	return nil
}

func (m *JsonSource) Next() schema.Message {
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

func (m *JsonSource) jsonDefaultLine(line []byte) (schema.Message, error) {
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
