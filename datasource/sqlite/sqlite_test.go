package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sync"
	"testing"

	u "github.com/araddon/gou"
	td "github.com/lytics/qlbridge/datasource/mockcsvtestdata"
	"github.com/lytics/qlbridge/datasource/sqlite"
	"github.com/stretchr/testify/assert"

	// Ensure we import sqlite driver
	_ "github.com/mattn/go-sqlite3"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/plan"
	"github.com/lytics/qlbridge/schema"
	"github.com/lytics/qlbridge/testutil"
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
			// Create table schema
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

		// set global to schema for text context
		sch = s

		// Copy, populate db
		for _, tablename := range td.MockSchema.Tables() {
			// Now go through the MockDB and copy data over
			baseConn, err := td.MockSchema.OpenConn(tablename)
			exitIfErr(err)

			sdbConn, err := s.OpenConn(tablename)
			assert.Equal(t, nil, err)
			assert.NotEqual(t, nil, sdbConn)
			userConn := sdbConn.(schema.ConnUpsert)
			cctx := context.Background()

			conn := baseConn.(schema.ConnScanner)
			for {
				msg := conn.Next()
				if msg == nil {
					break
				}
				sm := msg.(*datasource.SqlDriverMessageMap)
				k := sqlite.NewKey(sqlite.MakeId(sm.Vals[0]))
				if _, err := userConn.Put(cctx, k, sm.Vals); err != nil {
					u.Errorf("could not insert %v  %#v", err, sm.Vals)
				} else {
					//u.Infof("inserted %v %#v", key, sm.Vals)
				}
			}

			err = sdbConn.Close()
			assert.Equal(t, nil, err)
		}

		td.TestContext = planContext
	})
}
func TestMain(m *testing.M) {
	testutil.Setup() // will call flag.Parse()

	// Now run the actual Tests
	os.Exit(m.Run())
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
	testutil.RunSimpleSuite(t)
}
