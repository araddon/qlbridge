// Test only package for harness to load, implement SQL tests
package testutil

import (
	"database/sql/driver"
	"flag"
	"log"
	"os"
	"sync"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/araddon/qlbridge/datasource"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/schema"
)

var (
	verbose   *bool
	setupOnce = sync.Once{}
)

// SetupLogging enables -vv verbose logging or sends logs to /dev/null
// env var VERBOSELOGS=true was added to support verbose logging with alltests
func Setup() {
	setupOnce.Do(func() {

		if flag.CommandLine.Lookup("vv") == nil {
			verbose = flag.Bool("vv", false, "Verbose Logging?")
		}

		flag.Parse()
		logger := u.GetLogger()
		if logger != nil {
			// don't re-setup
		} else {
			if (verbose != nil && *verbose == true) || os.Getenv("VERBOSELOGS") != "" {
				u.SetupLogging("debug")
				u.SetColorOutput()
			} else {
				// make sure logging is always non-nil
				dn, _ := os.Open(os.DevNull)
				u.SetLogger(log.New(dn, "", 0), "error")
			}
		}

		builtins.LoadAllBuiltins()

	})
}

type QuerySpec struct {
	Source          string // Db source
	Sql             string
	Exec            string
	HasErr          bool
	Cols            []string
	ValidateRow     func([]interface{})
	ExpectRowCt     int
	ExpectColCt     int
	RowData         interface{}
	Expect          [][]driver.Value
	ValidateRowData func()
}

func ExecSpec(t *testing.T, q *QuerySpec) {
	ctx := td.TestContext(q.Sql)
	job, err := exec.BuildSqlJob(ctx)
	if !q.HasErr {
		assert.Tf(t, err == nil, "expected no error but got %v for %s", err, q.Sql)
	} else {
		assert.Tf(t, err != nil, "expected error but got %v for %s", err, q.Sql)
		return
	}

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	if !q.HasErr {

	}
	assert.T(t, err == nil)
	err = job.Run()
	//time.Sleep(time.Millisecond * 1)
	assert.Tf(t, err == nil, "got err=%v for sql=%s", err, q.Sql)
	assert.Tf(t, len(msgs) == q.ExpectRowCt, "expected %d rows but got %v for %s", q.ExpectRowCt, len(msgs), q.Sql)
	for rowi, msg := range msgs {
		row := msg.(*datasource.SqlDriverMessageMap).Values()
		expect := q.Expect[rowi]
		//u.Debugf("msg?  %#v", msg)
		assert.Tf(t, len(row) == len(expect), "expects %d cols but got %v for sql=%s", len(expect), len(row), q.Sql)
		for i, v := range row {
			assert.Equalf(t, expect[i], v, "Comparing values, col:%d expected %v:%T got %v:%T for sql=%s",
				i, expect[i], expect[i], v, v, q.Sql)
		}
	}
}
func ExecSqlSpec(t *testing.T, q *QuerySpec) {

	dbx, err := sqlx.Connect("qlbridge", q.Source)

	assert.Equalf(t, nil, err, "no error: %v", err)
	assert.NotEqual(t, nil, dbx, "has conn: ", dbx)

	defer func() {
		if err := dbx.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	switch {
	case len(q.Exec) > 0:
		result, err := dbx.Exec(q.Exec)
		assert.Equalf(t, nil, err, "%v", err)
		u.Infof("result: %#v", result)
		if q.ExpectRowCt > -1 {
			affected, err := result.RowsAffected()
			assert.Tf(t, err == nil, "%v", err)
			assert.Tf(t, affected == int64(q.ExpectRowCt), "expected %v affected but got %v for %s", q.ExpectRowCt, affected, q.Exec)
		}

	case len(q.Sql) > 0:
		u.Debugf("----ABOUT TO QUERY  %s", q.Sql)
		rows, err := dbx.Queryx(q.Sql)
		assert.Equalf(t, nil, err, "%v", err)
		defer rows.Close()

		cols, err := rows.Columns()
		assert.Equalf(t, nil, err, "%v", err)
		if len(q.Cols) > 0 {
			for _, expectCol := range q.Cols {
				found := false
				for _, colName := range cols {
					if colName == expectCol {
						found = true
					}
				}
				assert.Tf(t, found, "Should have found column: %v", expectCol)
			}
		}
		rowCt := 0
		for rows.Next() {
			if q.RowData != nil {
				err = rows.StructScan(q.RowData)
				//u.Infof("%#v rowVals: %#v", rows.Rows., q.RowData)
				assert.Tf(t, err == nil, "data:%+v   err=%v", q.RowData, err)

				if q.ValidateRowData != nil {
					q.ValidateRowData()
				}
			} else if len(q.Expect) > 0 {
				// rowVals is an []interface{} of all of the column results
				row, err := rows.SliceScan()
				//u.Debugf("rowVals: %#v", row)
				assert.Equalf(t, nil, err, "%v", err)
				for i, v := range row {
					expect := q.Expect[rowCt]
					assert.Equalf(t, expect[i], v, "Comparing values, col:%d expected %v:%T got %v:%T for sql=%s",
						i, expect[i], expect[i], v, v, q.Sql)
				}

			} else {
				// rowVals is an []interface{} of all of the column results
				rowVals, err := rows.SliceScan()
				//u.Infof("rowVals: %#v", rowVals)
				assert.Equalf(t, nil, err, "%v", err)
				if q.ExpectColCt > 0 {
					assert.Tf(t, len(rowVals) == q.ExpectColCt, "wanted %d cols but got %v", q.ExpectColCt, len(rowVals))
				}
				rowCt++
				if q.ValidateRow != nil {
					q.ValidateRow(rowVals)
				}
			}
			rowCt++
		}

		if q.ExpectRowCt > -1 {
			assert.Tf(t, rowCt == q.ExpectRowCt, "expected %v rows but got %v", q.ExpectRowCt, rowCt)
		}

		assert.T(t, rows.Err() == nil)
		//u.Infof("rows: %v", cols)
	}
}
func TestSelect(t *testing.T, sql string, expects [][]driver.Value) {
	ExecSpec(t, &QuerySpec{
		Sql:         sql,
		ExpectRowCt: len(expects),
		Expect:      expects,
	})
}

// TestSqlSelect tests using the database/sql driver
func TestSqlSelect(t *testing.T, source, sql string, expects [][]driver.Value) {
	ExecSqlSpec(t, &QuerySpec{
		Source:      source,
		Sql:         sql,
		ExpectRowCt: len(expects),
		Expect:      expects,
	})
}
func TestSelectErr(t *testing.T, sql string, expects [][]driver.Value) {
	ExecSpec(t, &QuerySpec{
		Sql:         sql,
		HasErr:      true,
		ExpectRowCt: len(expects),
		Expect:      expects,
	})
}
