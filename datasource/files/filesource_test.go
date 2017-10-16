package files_test

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/files"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
)

var localconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "tables/",
	TmpDir:          "/tmp/localcache",
}

func init() {
	testutil.Setup()
	time.Sleep(time.Second * 1)
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

	fileStore := "localfs"
	if os.Getenv("FILESTORE") != "" {
		fileStore = os.Getenv("FILESTORE")
	}

	settings := u.JsonHelper(map[string]interface{}{
		"path":     "baseball",
		"filetype": "csv",
		"type":     fileStore,
	})
	ss.Conf = &schema.ConfigSource{
		Name:       "testcsvs",
		SourceType: "testcsvs",
		Settings:   settings,
	}
	return m.FileSource.Setup(ss)
}

func TestFileList(t *testing.T) {
	// TODO:  fix schema to have consistent sort, currently
	// it uses map[string]schema
	testutil.TestSqlSelect(t, "testcsvs", `show databases;`,
		[][]driver.Value{
			{"mockcsv"},
			{"testcsvs"},
		},
	)
	testutil.TestSqlSelect(t, "testcsvs", `show tables;`,
		[][]driver.Value{
			{"appearances"},
			{"testcsvs_files"},
		},
	)
	testutil.TestSqlSelect(t, "testcsvs", `SELECT * FROM testcsvs_files;`,
		[][]driver.Value{
			{"baseball/appearances/appearances.csv"},
		},
	)
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
	assert.Equal(t, nil, err, "no error: %v", err)
	assert.NotEqual(t, nil, db, "has conn: ", db)

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	rows, err := db.Query(sqlText)
	assert.True(t, err == nil, "no error: %v", err)
	defer rows.Close()
	assert.True(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.True(t, err == nil, "no error: %v", err)
	assert.True(t, len(cols) == 3, "3 cols: %v", cols)
	players := make([]player, 0)
	for rows.Next() {
		var p player
		err = rows.Scan(&p.PlayerId, &p.YearId, &p.TeamId)
		assert.True(t, err == nil, "no error: %v", err)
		u.Debugf("player=%+v", p)
		players = append(players, p)
	}
	assert.True(t, rows.Err() == nil, "no error: %v", err)
	assert.True(t, len(players) == 1, "has 1 players row: %+v", players)

	p1 := players[0]
	assert.True(t, p1.PlayerId == "barnero01")
	assert.True(t, p1.YearId == "1871")
	assert.True(t, p1.TeamId == "BS1")

	// r := datasource.DataSourcesRegistry()
	// u.Debugf("tables:  %v", r.Tables())
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
