package mockcsv

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

var MockData = map[string]string{
	"users.csv": `user_id,email,interests,reg_date,item_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",82
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`,
}

func init() {
	datasource.Register("mockcsv", &MockCsvSource{data: MockData})
}

type MockCsvSource struct {
	*datasource.CsvDataSource
	data map[string]string
}

func (m *MockCsvSource) Open(connInfo string) (datasource.SourceConn, error) {
	if data, ok := m.data[connInfo]; ok {
		sr := strings.NewReader(data)
		u.Debugf("open mockcsv: %v", connInfo)
		return datasource.NewCsvSource(sr, make(<-chan bool, 1))
	}
	u.Errorf("not found?  %v", connInfo)
	return nil, fmt.Errorf("not found")
}
func (m *MockCsvSource) Tables() []string {
	tbls := make([]string, 0, len(m.data))
	for _, tbl := range m.data {
		tbls = append(tbls, tbl)
	}
	return tbls
}
