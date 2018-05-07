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
	testFile   = "./.test.db"
	sch        *schema.Schema
	loadData   sync.Once
	oldContext func(query string) *plan.Context
)

func LoadTestDataOnce(t *testing.T) {
	loadData.Do(func() {
		testutil.Setup()
		// load our mock data sources "users", "articles"
		td.LoadTestDataOnce()
		oldContext = td.TestContext

		os.Remove(testFile)

		// It will be created if it doesn't exist.
		//   "./source.enriched.db"
		db, err := sql.Open("sqlite3", testFile)
		if err != nil {
			panic(err.Error())
		}
		err = db.Ping()
		if err != nil {
			panic(err.Error())
		}

		reg := schema.DefaultRegistry()
		by := []byte(`{
			"name": "sqlite_test",
			"type": "sqlite",
			"settings" : {
			  "file" : "./test.db"
			}
		}`)

		conf := &schema.ConfigSource{}
		err = json.Unmarshal(by, conf)
		assert.Equal(t, nil, err)
		err = reg.SchemaAddFromConfig(conf)
		assert.Equal(t, nil, err)

		s, ok := reg.Schema("sqlite_test")
		assert.Equal(t, true, ok)
		assert.NotEqual(t, nil, s)
		sch = s

		for _, tablename := range td.MockSchema.Tables() {
			tbl, _ := td.MockSchema.Table(tablename)
			if tbl == nil {
				panic("missing table " + tablename)
			}
			u.Infof("found schema for %s \n%s", tablename, tbl.String())
			for _, col := range tbl.Fields {
				u.Debugf("%+v", col)
			}
			u.Debugf("create \n%s", sqlite.TableToString(tbl))
			res, err := db.Exec(sqlite.TableToString(tbl))
			u.Infof("err=%v  res=%+v", err, res)
			baseConn, err := td.MockSchema.OpenConn(tablename)
			if err != nil {
				panic(err.Error())
			}
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
	LoadTestDataOnce(t)
	testutil.RunTestSuite(t)
	td.TestContext = oldContext
}
