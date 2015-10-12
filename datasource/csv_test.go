package datasource

import (
	"flag"
	"fmt"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
	_                  = u.EMPTY

	testData = map[string]string{
		"user.csv": `user_id,email,interests,reg_date,item_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",82
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`,
	}

	csvSource       DataSource = &CsvDataSource{}
	csvStringSource DataSource = &csvStaticSource{testData: testData}
)

func init() {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
	} else {
		u.SetupLogging("info")
	}

	u.SetColorOutput()

	// Register our Datasources in registry
	Register("csv", csvSource)
	Register("csvtest", csvStringSource)
}

type csvStaticSource struct {
	*CsvDataSource
	testData map[string]string
}

func (m *csvStaticSource) Open(connInfo string) (SourceConn, error) {
	if data, ok := m.testData[connInfo]; ok {
		sr := strings.NewReader(data)
		return NewCsvSource(connInfo, 0, sr, make(<-chan bool, 1))
	}
	return nil, fmt.Errorf("not found")
}

func TestCsvDatasource(t *testing.T) {
	// register some test data
	// Create a csv data source from stdin
	//csvIn, err := OpenConn("csvtest", "user.csv")
	csvIn, err := csvStringSource.Open("user.csv")
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
