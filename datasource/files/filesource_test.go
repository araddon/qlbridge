package files_test

import (
	"database/sql"
	"flag"
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
	flag.Parse()
	if testing.Verbose() {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}

	builtins.LoadAllBuiltins()

	datasource.Register("testcsvs", newCsvTestSource())
	exec.RegisterSqlDriver()
	exec.DisableRecover()
}

type testSource struct {
	*files.FileSource
}

func newCsvTestSource() schema.Source {
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

// go test -bench="FileSqlWhere" --run="FileSqlWhere"
//
// go test -bench="FileSqlWhere" --run="FileSqlWhere" -cpuprofile cpu.out
// go tool pprof files.test cpu.out
func BenchmarkFileSqlWhere(b *testing.B) {

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

BenchmarkFileSqlWhere-4   	       3	 441965974 ns/op
BenchmarkFileSqlWhere-4   	       3	 461789398 ns/op
BenchmarkFileSqlWhere-4   	       3	 454068259 ns/op

bench_master Jan 17 2017
BenchmarkFileSqlWhere-4   	       3	 441293018 ns/op
BenchmarkFileSqlWhere-4   	       3	 478329135 ns/op
BenchmarkFileSqlWhere-4   	       3	 444717045 ns/op


faster, look-elsewhere!!

*/

// go test -bench="FileIter" --run="FileIter"
//
// go test -bench="FileIter" --run="FileIter" -cpuprofile cpu.out
// go tool pprof files.test cpu.out
func BenchmarkFileIter(b *testing.B) {

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
BenchmarkFileIter-4   	       3	 356800349 ns/op
BenchmarkFileIter-4   	       3	 335035981 ns/op
BenchmarkFileIter-4   	       3	 337403907 ns/op

bench_master  Jan 17 2017
BenchmarkFileIter-4   	       3	 335869429 ns/op
BenchmarkFileIter-4   	       3	 336145414 ns/op
BenchmarkFileIter-4   	       3	 347501696 ns/op

No difference
*/
