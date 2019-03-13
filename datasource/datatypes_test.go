package datasource_test

import (
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"

	"github.com/araddon/dateparse"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	tu "github.com/araddon/qlbridge/testutil"
)

var (
	cats   = []string{"sports", "politics", "worldnews"}
	catstr = strings.Join(cats, ",")
	jo     = json.RawMessage([]byte(`{"name":"bob"}`))
	t1     = dateparse.MustParse("2014-01-01")
)

func init() {
	tu.Setup()
	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()
}

type dtData struct {
	Categories string                          `db:"categories"`
	Cat        datasource.StringArray          `db:"cat"`
	JsonCat    datasource.StringArray          `db:"json_cats"`
	Json1      *datasource.JsonWrapper         `db:"j1"`
	Json2      *datasource.JsonHelperScannable `db:"j2"`
	Id         string                          `db:"user_id"`
	T1         *datasource.TimeValue           `db:"t1"`
}

func TestDataTypes(t *testing.T) {

	// Load in a "csv file" into our mock data store
	mockcsv.CreateCsvTable(td.MockSchema.Name, "typestest", `user_id,categories,json_obj,json_cats,t1
9Ip1aKbeZe2njCDM,"sports,politics,worldnews","{""name"":""bob""}","[""sports"",""politics"",""worldnews""]","2014-01-01"`)

	data := dtData{}
	tu.ExecSqlSpec(t, &tu.QuerySpec{
		Source: td.MockSchema.Name,
		Sql: `SELECT
			user_id, categories, categories AS cat, json_cats, t1, json_obj as j1, json_obj as j2
			FROM typestest;`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%#v", data)
			assert.Equal(t, catstr, data.Categories)
			assert.Equal(t, cats, []string(data.Cat))
			assert.Equal(t, cats, []string(data.JsonCat))
			assert.Equal(t, t1, data.T1.Time())
			assert.Equal(t, jo, json.RawMessage(*data.Json1))
			j2, _ := json.Marshal(data.Json2)
			assert.Equal(t, string(jo), string(j2))
		},
		RowData: &data,
	})
	by, err := json.Marshal(data)
	assert.Equal(t, nil, err)
	var data2 dtData
	err = json.Unmarshal(by, &data2)
	u.Infof("by %s", string(by))
	assert.Equal(t, nil, err)
	assert.Equal(t, catstr, data2.Categories)
	assert.Equal(t, cats, []string(data2.Cat))
	assert.Equal(t, cats, []string(data2.JsonCat))
	assert.Equal(t, t1, data2.T1.Time())
	assert.Equal(t, jo, json.RawMessage(*data2.Json1))
	j2, _ := json.Marshal(data2.Json2)
	assert.Equal(t, string(jo), string(j2))

	tu.ExecSqlSpec(t, &tu.QuerySpec{
		Source:      td.MockSchema.Name,
		Exec:        "DROP TABLE typestest;",
		ExpectRowCt: -1,
	})
	tu.TestSelect(t, `show tables;`,
		[][]driver.Value{{"orders"}, {"users"}},
	)
}
