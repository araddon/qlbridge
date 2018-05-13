// Package testutil a Test only package for harness to load, implement SQL tests.
package testutil

import (
	"database/sql/driver"
	"flag"
	"log"
	"os"
	"sync"

	u "github.com/araddon/gou"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

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

// TestingT is an interface wrapper around *testing.T so when we import
// this go dep, govendor don't import "testing"
type TestingT interface {
	Errorf(format string, args ...interface{})
}

// Setup enables -vv verbose logging or sends logs to /dev/null
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

// QuerySpec a test harness
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

// ExecSpec execute a queryspec test
func ExecSpec(t TestingT, q *QuerySpec) {
	sql := q.Sql
	if sql == "" && q.Exec != "" {
		sql = q.Exec
	}
	ctx := td.TestContext(sql)
	u.Debugf("running sql %v%v", q.Sql, q.Exec)

	job, err := exec.BuildSqlJob(ctx)
	if !q.HasErr {
		assert.Equal(t, nil, err, "expected no error but got %v for %s", err, q.Sql)
	} else {
		assert.NotEqual(t, nil, err, "expected error but got %v for %s", err, q.Sql)
		return
	}

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	if !q.HasErr {

	}
	assert.Equal(t, nil, err)
	err = job.Run()
	assert.Equal(t, nil, err)
	assert.Equal(t, q.ExpectRowCt, len(msgs), "expected %d rows but got %v for %s", q.ExpectRowCt, len(msgs), q.Sql)
	for rowi, msg := range msgs {
		row := msg.(*datasource.SqlDriverMessageMap).Values()
		assert.True(t, len(q.Expect) > rowi-1, "Expect count doesn't match row count %v vs %v", len(q.Expect), rowi)
		if len(q.Expect) > rowi-1 {
			expect := q.Expect[rowi]
			assert.True(t, len(row) == len(expect), "expects %d cols but got %v for sql=%s", len(expect), len(row), q.Sql)
			if len(row) == len(expect) {
				for i, v := range row {
					assert.Equal(t, expect[i], v, "Comparing values, col:%d expected %v:%T got %v:%T for sql=%s",
						i, expect[i], expect[i], v, v, q.Sql)
				}
			}
		}

	}
}

// ExecSqlSpec execute a test harness of QuerySpec
func ExecSqlSpec(t TestingT, q *QuerySpec) {

	dbx, err := sqlx.Connect("qlbridge", q.Source)

	u.Debugf("running sql %v%v", q.Sql, q.Exec)

	assert.Equal(t, nil, err, "no error: %v", err)
	assert.NotEqual(t, nil, dbx, "has conn: ", dbx)

	defer func() {
		if err := dbx.Close(); err != nil {
			t.Errorf("Should not error on close: %v", err)
		}
	}()

	switch {
	case len(q.Exec) > 0:
		result, err := dbx.Exec(q.Exec)
		assert.Equal(t, nil, err)
		if q.ExpectRowCt > -1 && result != nil {
			affected, err := result.RowsAffected()
			assert.Equal(t, nil, err)
			assert.Equal(t, int64(q.ExpectRowCt), affected, "expected %v affected but got %v for %s", q.ExpectRowCt, affected, q.Exec)
		}

	case len(q.Sql) > 0:
		u.Debugf("----ABOUT TO QUERY  %s", q.Sql)
		rows, err := dbx.Queryx(q.Sql)
		assert.Equal(t, nil, err)
		defer rows.Close()

		cols, err := rows.Columns()
		assert.Equal(t, nil, err)
		if len(q.Cols) > 0 {
			for _, expectCol := range q.Cols {
				found := false
				for _, colName := range cols {
					if colName == expectCol {
						found = true
					}
				}
				assert.True(t, found, "Should have found column: %v", expectCol)
			}
		}
		rowCt := 0
		for rows.Next() {
			if q.RowData != nil {
				err = rows.StructScan(q.RowData)
				assert.Equal(t, nil, err)
				if q.ValidateRowData != nil {
					q.ValidateRowData()
				}
			} else if len(q.Expect) > 0 {
				// rowVals is an []interface{} of all of the column results
				row, err := rows.SliceScan()
				assert.Equal(t, nil, err)
				if rowCt >= len(q.Expect) {
					t.Errorf("Too many rows? rowCt=%v  len(expect)=%d", rowCt, len(q.Expect))
					continue
				}
				for i, v := range row {
					expect := q.Expect[rowCt]
					assert.Equal(t, expect[i], v, "Comparing values, col:%d expected %v:%T got %v:%T for sql=%s",
						i, expect[i], expect[i], v, v, q.Sql)
				}

			} else {
				// rowVals is an []interface{} of all of the column results
				rowVals, err := rows.SliceScan()
				assert.Equal(t, nil, err, "%v", err)
				if q.ExpectColCt > 0 {
					assert.True(t, len(rowVals) == q.ExpectColCt, "wanted %d cols but got %v", q.ExpectColCt, len(rowVals))
				}
				rowCt++
				if q.ValidateRow != nil {
					q.ValidateRow(rowVals)
				}
			}
			rowCt++
		}

		if q.ExpectRowCt > -1 {
			assert.True(t, rowCt == q.ExpectRowCt, "expected %v rows but got %v", q.ExpectRowCt, rowCt)
		}

		assert.Equal(t, nil, rows.Err())
	}
}
func TestSelect(t TestingT, sql string, expects [][]driver.Value) {
	ExecSpec(t, &QuerySpec{
		Sql:         sql,
		ExpectRowCt: len(expects),
		Expect:      expects,
	})
}
func TestExec(t TestingT, sql string) {
	ExecSpec(t, &QuerySpec{
		Exec: sql,
	})
}

// TestSqlSelect tests using the database/sql driver
func TestSqlSelect(t TestingT, source, sql string, expects [][]driver.Value) {
	ExecSqlSpec(t, &QuerySpec{
		Source:      source,
		Sql:         sql,
		ExpectRowCt: len(expects),
		Expect:      expects,
	})
}
func TestSelectErr(t TestingT, sql string, expects [][]driver.Value) {
	ExecSpec(t, &QuerySpec{
		Sql:         sql,
		HasErr:      true,
		ExpectRowCt: len(expects),
		Expect:      expects,
	})
}
