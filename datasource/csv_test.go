package datasource

import (
	"fmt"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"strings"
	"testing"
)

var testData = map[string]string{
	"user.csv": `user_id,email,interests,reg_date,item_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",82
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`,
}

func init() {
	// Register our Datasources in registry
	Register("csv", &CsvDataSource{})
	Register("csvtest", &csvStringSource{testData: testData})

	u.SetupLogging("debug")
	u.SetColorOutput()
}

type csvStringSource struct {
	*CsvDataSource
	testData map[string]string
}

func (m *csvStringSource) Open(connInfo string) (DataSource, error) {
	if data, ok := m.testData[connInfo]; ok {
		sr := strings.NewReader(data)
		return NewCsvSource(sr, make(<-chan bool, 1))
	}
	return nil, fmt.Errorf("not found")
}

func TestCsvDatasource(t *testing.T) {
	// register some test data
	// Create a csv data source from stdin
	csvIn, err := Open("csvtest", "user.csv")
	assert.Tf(t, err == nil, "should not have error: %v", err)
	csvIter, ok := csvIn.(Scanner)
	assert.T(t, ok)

	iter := csvIter.CreateIterator(nil)
	iterCt := 0
	for msg := iter.Next(); msg != nil; msg = iter.Next() {
		iterCt++
		u.Infof("row:  %v", msg.Body())
	}
	assert.Tf(t, iterCt == 3, "should have 3 rows: %v", iterCt)
}
