package datasource

import (
	"encoding/csv"
	"io"
	"net/url"
	"os"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
)

func init() {
	//datasource.Register("csv", &datasource.CsvDataSource{})
}

var (
	_ DataSource = (*CsvDataSource)(nil)
	_ Scanner    = (*CsvDataSource)(nil)
)

// Csv DataStoure, implements qlbridge DataSource to scan through data
//   see interfaces possible but they are
//
type CsvDataSource struct {
	exit    <-chan bool
	csvr    *csv.Reader
	rowct   uint64
	headers []string
	rc      io.ReadCloser
}

func NewCsvSource(ior io.Reader, exit <-chan bool) (*CsvDataSource, error) {
	m := CsvDataSource{}
	if rc, ok := ior.(io.ReadCloser); ok {
		m.rc = rc
	}
	m.csvr = csv.NewReader(ior)
	m.csvr.TrailingComma = true // allow empty fields
	// if flagCsvDelimiter == "|" {
	// 	m.csvr.Comma = '|'
	// } else if flagCsvDelimiter == "\t" || flagCsvDelimiter == "t" {
	// 	m.csvr.Comma = '\t'
	// }
	headers, err := m.csvr.Read()
	if err != nil {
		return nil, err
	}
	m.headers = headers
	return &m, nil
}

func (m *CsvDataSource) Open(connInfo string) (DataSource, error) {
	f, err := os.Open(connInfo)
	if err != nil {
		return nil, err
	}
	exit := make(<-chan bool, 1)
	return NewCsvSource(f, exit)
}

func (m *CsvDataSource) Close() error {
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("close error: %v", r)
		}
	}()
	if m.rc != nil {
		m.rc.Close()
	}
	return nil
}

func (m *CsvDataSource) CreateIterator(filter expr.Node) Iterator {
	return m
}

func (m *CsvDataSource) Next() Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			row, err := m.csvr.Read()
			//u.Debugf("row:   %v   %v", row, err)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				u.Warnf("could not read row? %v", err)
				continue
			}
			m.rowct++
			v := make(url.Values)

			// If values exist for desired indexes, set them.
			for idx, fieldName := range m.headers {
				if idx <= len(row)-1 {
					v.Set(fieldName, strings.TrimSpace(row[idx]))
				}
			}

			return &UrlValuesMsg{id: m.rowct, body: NewContextUrlValues(v)}
		}

	}

}
