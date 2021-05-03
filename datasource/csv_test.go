package datasource_test

import (
	"fmt"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/schema"
)

var (
	_        = u.EMPTY
	testData = map[string]string{
		"user.csv": `user_id,email,interests,reg_date,item_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",82
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`,
	}

	csvSource       schema.Source = &datasource.CsvDataSource{}
	csvStringSource schema.Source = &csvStaticSource{testData: testData}
)

type csvStaticSource struct {
	*datasource.CsvDataSource
	testData map[string]string
}

func (m *csvStaticSource) Open(connInfo string) (schema.Conn, error) {
	if data, ok := m.testData[connInfo]; ok {
		sr := strings.NewReader(data)
		return datasource.NewCsvSource(connInfo, 0, sr, make(<-chan bool, 1))
	}
	return nil, fmt.Errorf("not found")
}

func TestCsvDataSource(t *testing.T) {
	csvIn, err := csvStringSource.Open("user.csv")
	assert.True(t, err == nil, "should not have error: %v", err)
	csvIter, ok := csvIn.(schema.ConnScanner)
	assert.True(t, ok)
	iterCt := 0
	for msg := csvIter.Next(); msg != nil; msg = csvIter.Next() {
		iterCt++
		u.Infof("row:  %v", msg.Body())
	}
	assert.Equal(t, 3, iterCt)
	err = csvIn.Close()
	assert.Equal(t, nil, err)

	sr := strings.NewReader(testData["user.csv"])
	csvIn, err = datasource.NewCsvSource("user.csv", 0, sr, make(<-chan bool, 1))
	assert.Equal(t, nil, err)
	csvIn.Close()
}
