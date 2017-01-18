package files_test

import (
	"database/sql"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/lytics/cloudstorage"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/files"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/schema"
)

var localconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "tables/",
	TmpDir:          "/tmp/localcache",
}

func init() {
	if testing.Verbose() {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}

	builtins.LoadAllBuiltins()

	datasource.Register("testcsvs", newSource())
	exec.RegisterSqlDriver()
	exec.DisableRecover()
}

type testSource struct {
	*files.FileSource
}

func newSource() schema.Source {
	return &testSource{files.NewFileSource()}
}

// Setup the filesource with schema info
func (m *testSource) Setup(ss *schema.SchemaSource) error {

	settings := u.JsonHelper(map[string]interface{}{
		"path":     "appearances",
		"filetype": "csv",
	})
	ss.Conf = &schema.ConfigSource{
		Name:       "testcsvs",
		SourceType: "testcsvs",
		Settings:   settings,
	}
	return m.FileSource.Setup(ss)
}

type player struct {
	PlayerId string
	YearId   string
	TeamId   string
}

// go test -bench="BenchFile" --run="BenchFile"
//
// go test -bench="BenchFile" --run="BenchFile" -cpuprofile cpu.out
// go tool pprof files.test cpu.out
func TestFileSelectSimple(t *testing.T) {

	sqlText := `SELECT playerid, yearid, teamid 
	FROM appearances 
	WHERE playerid = "barnero01" AND yearid = "1871";
	`
	db, err := sql.Open("qlbridge", "testcsvs")
	assert.Equalf(t, nil, err, "no error: %v", err)
	assert.NotEqual(t, nil, db, "has conn: ", db)

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	rows, err := db.Query(sqlText)
	assert.Tf(t, err == nil, "no error: %v", err)
	defer rows.Close()
	assert.Tf(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 3, "3 cols: %v", cols)
	players := make([]player, 0)
	for rows.Next() {
		var p player
		err = rows.Scan(&p.PlayerId, &p.YearId, &p.TeamId)
		assert.Tf(t, err == nil, "no error: %v", err)
		u.Debugf("player=%+v", p)
		players = append(players, p)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(players) == 1, "has 1 players row: %+v", players)

	p1 := players[0]
	assert.T(t, p1.PlayerId == "barnero01")
	assert.T(t, p1.YearId == "1871")
	assert.T(t, p1.TeamId == "BS1")
}

// go test -bench="BenchFileSqlWhere" --run="BenchFileSqlWhere"
//
// go test -bench="BenchFileSqlWhere" --run="BenchFileSqlWhere" -cpuprofile cpu.out
// go tool pprof files.test cpu.out
func BenchmarkBenchFileSqlWhere(b *testing.B) {

	sqlText := `SELECT playerid, yearid, teamid 
	FROM appearances 
	WHERE playerid = "barnero01" AND yearid = "1871";
	`
	db, _ := sql.Open("qlbridge", "testcsvs")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(sqlText)
		players := make([]player, 0)
		for rows.Next() {
			var p player
			rows.Scan(&p.PlayerId, &p.YearId, &p.TeamId)
			players = append(players, p)
		}
		rows.Close()
		if len(players) != 1 {
			b.Fail()
		}
	}
}

/*

bench_april_2016

BenchmarkBenchFileIter-4   	       3	 356800349 ns/op
BenchmarkBenchFileIter-4   	       3	 335035981 ns/op
BenchmarkBenchFileIter-4   	       3	 337403907 ns/op


bench_master Jan 17 2017
BenchmarkBenchFileSqlWhere-4   	       3	 441293018 ns/op
BenchmarkBenchFileSqlWhere-4   	       3	 478329135 ns/op
BenchmarkBenchFileSqlWhere-4   	       3	 444717045 ns/op


1.31 times as long.  Bad but not 10x.

*/

// go test -bench="BenchFileIter" --run="BenchFileIter"
//
// go test -bench="BenchFileIter" --run="BenchFileIter" -cpuprofile cpu.out
// go tool pprof files.test cpu.out
func BenchmarkBenchFileIter(b *testing.B) {

	fs, _ := datasource.DataSourcesRegistry().Schema("testcsvs")
	b.StartTimer()

	for i := 0; i < b.N; i++ {

		conn, _ := fs.Open("appearances")
		scanner := conn.(schema.Iterator)

		for {
			msg := scanner.Next()
			if msg == nil {
				break
			}
		}
	}
}

/*

bench_april_2016  code from april 2nd 2016
BenchmarkBenchFileIter	3	 362930983 ns/op

bench_master  Jan 17 2017
BenchmarkBenchFileIter	3	 357590068 ns/op



*/
