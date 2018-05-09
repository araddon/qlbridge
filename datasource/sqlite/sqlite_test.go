package sqlite_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"sync"
	"testing"

	u "github.com/araddon/gou"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/datasource/sqlite"
	"github.com/bmizerany/assert"
	_ "github.com/mattn/go-sqlite3"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

/*
TODO:
- the schema doesn't exists/isn't getting loaded.

*/
var (
	testFile = "./test.db"
	sch      *schema.Schema
	loadData sync.Once
)

func exitIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}
func LoadTestDataOnce(t *testing.T) {
	loadData.Do(func() {
		testutil.Setup()
		// load our mock data sources "users", "orders"
		td.LoadTestDataOnce()

		os.Remove(testFile)

		// It will be created if it doesn't exist.
		db, err := sql.Open("sqlite3", testFile)
		exitIfErr(err)
		err = db.Ping()
		exitIfErr(err)

		reg := schema.DefaultRegistry()
		by := []byte(`{
			"name": "sqlite_test",
			"type": "sqlite",
			"settings" : {
			  "file" : "test.db"
			}
		}`)

		conf := &schema.ConfigSource{}
		err = json.Unmarshal(by, conf)
		assert.Equal(t, nil, err)

		// Create Sqlite db schema
		for _, tablename := range td.MockSchema.Tables() {
			tbl, _ := td.MockSchema.Table(tablename)
			if tbl == nil {
				panic("missing table " + tablename)
			}
			//u.Infof("found schema for %s \n%s", tablename, tbl.String())
			// for _, col := range tbl.Fields {
			// 	u.Debugf("%+v", col)
			// }
			u.Debugf("\n%s", sqlite.TableToString(tbl))
			res, err := db.Exec(sqlite.TableToString(tbl))
			assert.Equal(t, nil, err)
			assert.NotEqual(t, nil, res)
			//u.Infof("err=%v  res=%+v", err, res)
		}
		db.Close()

		// From config, create schema
		err = reg.SchemaAddFromConfig(conf)
		assert.Equal(t, nil, err)

		// Get the Schema we just created
		s, ok := reg.Schema("sqlite_test")
		assert.Equal(t, true, ok)
		assert.NotEqual(t, nil, s)

		// Copy, populate db
		for _, tablename := range td.MockSchema.Tables() {
			// Now go through the MockDB and copy data over
			baseConn, err := td.MockSchema.OpenConn(tablename)
			exitIfErr(err)
			conn := baseConn.(schema.ConnScanner)
			for {
				msg := conn.Next()
				if msg == nil {
					break
				}
				sm := msg.(*datasource.SqlDriverMessageMap)
				u.Debugf("msg %#v", sm.Vals)
			}
		}

		sdbConn, err := s.OpenConn("users")
		assert.Equal(t, nil, err)
		assert.NotEqual(t, nil, sdbConn)

		td.TestContext = planContext
	})
}

func planContext(query string) *plan.Context {
	ctx := plan.NewContext(query)
	ctx.DisableRecover = true
	ctx.Schema = sch
	ctx.Session = datasource.NewMySqlSessionVars()
	return ctx
}

func TestSuite(t *testing.T) {
	defer func() {
		td.SetContextToMockCsv()
	}()
	LoadTestDataOnce(t)
	//testutil.RunTestSuite(t)
}
