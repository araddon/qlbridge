package datasource_test

import (
	"io/ioutil"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

var (
	_            = u.EMPTY
	testJsonData = map[string]string{
		"user.json": `{"user_id": "9Ip1aKbeZe2njCDM" ,"email":"aaron@email.com","interests":"fishing","reg_date":"2012-10-17T17:29:39.738Z","item_count":82}
{"user_id": "hT2impsOPUREcVPc" ,"email":"bob@email.com","interests":"swimming","reg_date":"2009-12-11T19:53:31.547Z","item_count":12}
{"user_id": "hT2impsabc345c" ,"email":"not_an_email","interests":"swimming","reg_date":"2009-12-11T19:53:31.547Z","item_count":12}`}

	jsonSource       schema.Source = &datasource.JsonSource{}
	jsonStringSource schema.Source = &jsonStaticSource{files: testJsonData}
)

func init() {
	testutil.Setup()
}

type jsonStaticSource struct {
	*datasource.JsonSource
	files map[string]string
}

func (m *jsonStaticSource) Open(connInfo string) (schema.Conn, error) {
	if data, ok := m.files[connInfo]; ok {
		sr := strings.NewReader(data)
		return datasource.NewJsonSource(connInfo, ioutil.NopCloser(sr), make(<-chan bool, 1), nil)
	}
	return nil, schema.ErrNotFound
}

func TestJsonDataSource(t *testing.T) {
	jsonIn, err := jsonStringSource.Open("user.json")
	assert.Tf(t, err == nil, "should not have error: %v", err)
	iter, ok := jsonIn.(schema.ConnScanner)
	assert.T(t, ok)
	iterCt := 0
	for msg := iter.Next(); msg != nil; msg = iter.Next() {
		iterCt++
		u.Infof("row:  %v", msg.Body())
	}
	assert.Equalf(t, 3, iterCt, "should have 3 rows: %v", iterCt)
}
