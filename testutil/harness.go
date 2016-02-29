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
		if *verbose || os.Getenv("VERBOSELOGS") != "" {
			u.SetupLogging("debug")
			u.SetColorOutput()
		} else {
			// make sure logging is always non-nil
			dn, _ := os.Open(os.DevNull)
			u.SetLogger(log.New(dn, "", 0), "error")
		}

		builtins.LoadAllBuiltins()

	})
}

type QuerySpec struct {
	sql    string
	rowct  int
	haserr bool
	expect [][]driver.Value
}

func ExecSpec(t *testing.T, q *QuerySpec) {
	ctx := td.TestContext(q.sql)
	job, err := exec.BuildSqlJob(ctx)
	if !q.haserr {
		assert.Tf(t, err == nil, "expected no error but got %v for %s", err, q.sql)
	} else {
		assert.Tf(t, err != nil, "expected error but got %v for %s", err, q.sql)
	}

	msgs := make([]schema.Message, 0)
	resultWriter := exec.NewResultBuffer(ctx, &msgs)
	job.RootTask.Add(resultWriter)

	err = job.Setup()
	assert.T(t, err == nil)
	err = job.Run()
	//time.Sleep(time.Millisecond * 1)
	assert.Tf(t, err == nil, "got err=%v for sql=%s", err, q.sql)
	assert.Tf(t, len(msgs) == q.rowct, "expected %d rows but got %v for %s", q.rowct, len(msgs), q.sql)
	for rowi, msg := range msgs {
		row := msg.(*datasource.SqlDriverMessageMap).Values()
		expect := q.expect[rowi]
		u.Debugf("msg?  %#v", msg)
		assert.Tf(t, len(row) == len(expect), "expects %d cols but got %v for sql=%s", len(expect), len(row), q.sql)
		for i, v := range row {
			assert.Equalf(t, expect[i], v, "Comparing values, col:%d expected %v got %v for sql=%s", i, expect[i], v, q.sql)
		}
	}
}
func TestSelect(t *testing.T, sql string, expects [][]driver.Value) {
	ExecSpec(t, &QuerySpec{
		sql:    sql,
		rowct:  len(expects),
		expect: expects,
	})
}
