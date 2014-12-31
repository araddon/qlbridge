package ast

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/bmizerany/assert"
	"testing"
)

var (
	_ = u.EMPTY
)

func init() {
	lex.IDENTITY_CHARS = lex.IDENTITY_SQL_CHARS
}

func parseSqlTest(t *testing.T, sql string) {
	sqlRequest, err := ParseSql(sql)
	assert.Tf(t, err == nil && sqlRequest != nil, "Must parse: %s  %v", sql, err)
}

// http://dev.mysql.com/doc/refman/5.0/en/information-functions.html
func TestSqlLexOnly(t *testing.T) {
	parseSqlTest(t, `SELECT LAST_INSERT_ID();`)
	parseSqlTest(t, `SELECT CHARSET();`)
	parseSqlTest(t, `SELECT DATABASE()`)
	parseSqlTest(t, `select @@version_comment limit 1`)
	parseSqlTest(t, `insert into mytable (id, str) values (0, "a")`)
	parseSqlTest(t, `DESCRIBE mytable`)
	parseSqlTest(t, `show tables`)
	parseSqlTest(t, `select director, year from movies where director like "Quentin"`)
	// TODO
	//parseSqlTest(t, `select name from movies where director IN ("Quentin","copola")`)
}
