package datasource

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql/driver"
	"encoding/csv"
	"io"
	"os"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	_ schema.Source      = (*CsvDataSource)(nil)
	_ schema.Conn        = (*CsvDataSource)(nil)
	_ schema.ConnScanner = (*CsvDataSource)(nil)
)

// Csv DataSource, implements qlbridge schema DataSource, SourceConn, Scanner
//   to allow csv files to be full featured databases.
//   - very, very naive scanner, forward only single pass
//   - can open a file with .Open()
//   - assumes comma delimited
//   - not thread-safe
//   - does not implement write operations
type CsvDataSource struct {
	table    string
	tbl      *schema.Table
	exit     <-chan bool
	csvr     *csv.Reader
	gz       *gzip.Reader
	rc       io.ReadCloser
	rowct    uint64
	headers  []string
	colindex map[string]int
	indexCol int
	filter   expr.Node
}

// NewCsvSource reader assumes we are getting first row as headers
// - optionally may be gzipped
func NewCsvSource(table string, indexCol int, ior io.Reader, exit <-chan bool) (*CsvDataSource, error) {

	m := CsvDataSource{table: table, indexCol: indexCol}
	if rc, ok := ior.(io.ReadCloser); ok {
		m.rc = rc
	}

	buf := bufio.NewReader(ior)

	first2, err := buf.Peek(2)
	if err != nil {
		u.Errorf("Error opening bufio.peek for csv reader %v", err)
		return nil, err
	}

	// TODO:  move this compression to the file-reader not here
	if err == nil && len(first2) == 2 && bytes.Equal(first2, []byte{'\x1F', '\x8B'}) {
		gr, err := gzip.NewReader(buf)
		if err != nil {
			u.Errorf("Could not open reader? %v", err)
			return nil, err
		}
		m.gz = gr
		m.csvr = csv.NewReader(gr)
	} else {
		m.csvr = csv.NewReader(buf)
	}

	m.csvr.TrailingComma = true // allow empty fields
	// if flagCsvDelimiter == "|" {
	// 	m.csvr.Comma = '|'
	// } else if flagCsvDelimiter == "\t" || flagCsvDelimiter == "t" {
	// 	m.csvr.Comma = '\t'
	// }
	headers, err := m.csvr.Read()
	if err != nil {
		u.Warnf("err csv %v", err)
		return nil, err
	}
	//u.Debugf("headers: %v", headers)
	m.headers = headers
	m.colindex = make(map[string]int, len(headers))
	for i, key := range headers {
		key = strings.ToLower(key)
		m.colindex[key] = i
		m.headers[i] = key
	}
	m.loadTable()
	//u.Infof("csv headers: %v colIndex: %v", headers, m.colindex)
	return &m, nil
}

func (m *CsvDataSource) Init()                      {}
func (m *CsvDataSource) Setup(*schema.Schema) error { return nil }
func (m *CsvDataSource) Tables() []string           { return []string{m.table} }
func (m *CsvDataSource) Columns() []string          { return m.headers }
func (m *CsvDataSource) Table(tableName string) (*schema.Table, error) {
	if m.tbl != nil {
		return m.tbl, nil
	}
	return nil, schema.ErrNotFound
}
func (m *CsvDataSource) loadTable() error {
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
func (m *CsvDataSource) Open(connInfo string) (schema.Conn, error) {
	if connInfo == "stdio" || connInfo == "stdin" {
		connInfo = "/dev/stdin"
	}
	f, err := os.Open(connInfo)
	if err != nil {
		return nil, err
	}
	exit := make(<-chan bool, 1)
	return NewCsvSource(connInfo, 0, f, exit)
}

func (m *CsvDataSource) Close() error {
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

func (m *CsvDataSource) Next() schema.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			row, err := m.csvr.Read()

			if err != nil {
				if err == io.EOF {
					return nil
				}
				u.Warnf("could not read row? %v", err)
				continue
			}
			m.rowct++
			if len(row) != len(m.headers) {
				u.Warnf("headers/cols dont match, dropping expected:%d got:%d   vals=", len(m.headers), len(row), row)
				continue
			}
			vals := make([]driver.Value, len(row))
			for i, val := range row {
				vals[i] = val
			}
			//u.Debugf("headers: %#v \n\trows:  %#v", m.headers, row)
			return NewSqlDriverMessageMap(m.rowct, vals, m.colindex)
		}
	}
}
