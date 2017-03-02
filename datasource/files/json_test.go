package files_test

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/files"
	"github.com/araddon/qlbridge/schema"
)

// http://data.githubarchive.org/%d-%02d-%02d-%d.json.gz
// http://data.githubarchive.org/2015-01-01-15.json.gz
func init() {

	files.RegisterFileScanner("github_json", files.NewJsonHandler(lineParser))
	datasource.Register("testjson", newJsonTestSource())
}

type jsonTestSource struct {
	*files.FileSource
}

func newJsonTestSource() schema.Source {
	return &jsonTestSource{files.NewFileSource()}
}

// Setup the filesource with schema info
func (m *jsonTestSource) Setup(ss *schema.SchemaSource) error {

	settings := u.JsonHelper(map[string]interface{}{
		"path":     "github",
		"filetype": "json",
		"format":   "github_json",
	})
	ss.Conf = &schema.ConfigSource{
		Name:       "testjson",
		SourceType: "testjson",
		Settings:   settings,
	}
	return m.FileSource.Setup(ss)
}

func lineParser(line []byte) (schema.Message, error) {
	gh := make(u.JsonHelper)

	d := json.NewDecoder(bytes.NewBuffer(line))
	d.UseNumber()
	err := d.Decode(&gh)
	if err != nil {
		u.Warnf("could not read json line: %v  %s", err, string(line))
		return nil, err
	}
	payload := gh.Helper("payload.issue")

	vals := make([]driver.Value, len(payload))
	keys := make(map[string]int, len(payload))
	i := 0
	for k, val := range payload {
		vals[i] = val
		keys[k] = i
		i++
	}
	//u.Debugf("json data: \n%#v \n%#v \n%s", keys, vals, string(payload.PrettyJson()))
	return datasource.NewSqlDriverMessageMap(0, vals, keys), nil
}

type ghissue struct {
	Id     int64
	State  string
	Number int
	Title  *string
}

func TestJsonSelectSimple(t *testing.T) {

	sqlText := `SELECT number, id, state, title 
	FROM issues 
	`
	db, err := sql.Open("qlbridge", "testjson")
	assert.Equalf(t, nil, err, "no error: %v", err)
	assert.NotEqual(t, nil, db, "has conn: ", db)

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	rows, err := db.Query(sqlText)
	assert.Equalf(t, nil, err, "no error: %v", err)
	defer rows.Close()
	assert.Tf(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.Equalf(t, nil, err, "no error: %v", err)
	assert.Equalf(t, 4, len(cols), "4 cols: %v", cols)
	issues := make([]ghissue, 0)
	for rows.Next() {
		var ghi ghissue
		err = rows.Scan(&ghi.Number, &ghi.Id, &ghi.State, &ghi.Title)
		if err == nil {
			u.Debugf("issue=%+v", ghi)
			issues = append(issues, ghi)
		} else {
			// TODO:  fix me.  Type issues in sql driver
			u.Debugf("hm %v", err)
		}
		//assert.Equalf(t, nil, err, "no error: %v", err)

	}
	assert.Equalf(t, nil, rows.Err(), "no error: %v", err)
	assert.Equalf(t, 11, len(issues), "has 11 issues row: %+v", issues)

	i1 := issues[0]
	assert.Equal(t, int64(53222517), i1.Id, i1)
	assert.Equal(t, 110, i1.Number)
	assert.Equal(t, "open", i1.State)
}

// go test -bench="JsonSqlWhere" --run="JsonSqlWhere"
//
// go test -bench="JsonSqlWhere" --run="JsonSqlWhere" -cpuprofile cpu.out
// go tool pprof files.test cpu.out
func BenchmarkJsonSqlWhere(b *testing.B) {

	sqlText := `SELECT playerid, yearid, teamid 
	FROM appearances 
	WHERE playerid = "barnero01" AND yearid = "1871";
	`
	db, _ := sql.Open("qlbridge", "testjson")

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