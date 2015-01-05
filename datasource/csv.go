package datasource

import (
	"encoding/csv"
	"io"
	"net/url"
	"os"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/ast"
)

func init() {
	Register("csv", &CsvProducer{})
}

type CsvProducer struct {
	exit    <-chan bool
	csvr    *csv.Reader
	rowct   uint64
	headers []string
}

func NewCsvSource(ior io.Reader, exit <-chan bool) (*CsvProducer, error) {
	m := CsvProducer{}
	//m.csvr = csv.NewReader(os.Stdin)
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

func (m *CsvProducer) Open(connInfo string) (DataSource, error) {
	f, err := os.Open(connInfo)
	if err != nil {
		return nil, err
	}
	exit := make(<-chan bool, 1)
	return NewCsvSource(f, exit)
}
func (m *CsvProducer) CreateIterator(filter *ast.Tree) Iterator {
	return m
}

func (m *CsvProducer) Next() Message {
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
			v := make(url.Values)

			// If values exist for desired indexes, set them.
			for idx, fieldName := range m.headers {
				if idx <= len(row)-1 {
					v.Set(fieldName, strings.TrimSpace(row[idx]))
				}
			}

			return &UrlValuesMsg{id: m.rowct, body: v}
		}

	}

}

/*
func CsvProducer(msgChan chan url.Values, quit chan bool) {
	defer func() {
		quit <- true
	}()

	csvr.TrailingComma = true // allow empty fields
	if flagCsvDelimiter == "|" {
		csvr.Comma = '|'
	} else if flagCsvDelimiter == "\t" || flagCsvDelimiter == "t" {
		csvr.Comma = '\t'
	}
	headers, err := csvr.Read()
	if err != nil {
		panic(err.Error())
	}
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			continue
		}

		v := make(url.Values)

		// If values exist for desired indexes, set them.
		for idx, fieldName := range headers {
			if idx <= len(row)-1 {
				v.Set(fieldName, strings.TrimSpace(row[idx]))
			}
		}

		msgChan <- v

	}
}
*/
