package lex_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/rel"
)

func parseSqlTest(t *testing.T, sql string) {
	u.Debugf("parsing sql: %s", sql)
	sqlRequest, err := rel.ParseSql(sql)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, sqlRequest, "Must parse: %s  \n\t%v", sql, err)
	if ss, ok := sqlRequest.(*rel.SqlSelect); ok {
		_, err2 := rel.ParseSqlSelect(sql)
		assert.Equal(t, nil, err2)
		pb := ss.ToPbStatement()
		pbb, err := pb.Marshal()
		assert.Equal(t, nil, err)
		ss2, err := rel.SqlFromPb(pbb)
		assert.Equal(t, nil, err)
		assert.True(t, ss.Equal(ss2))
	}
}
func parseSqlError(t *testing.T, sql string) {
	u.Debugf("parse looking for error sql: %s", sql)
	_, err := rel.ParseSql(sql)
	assert.NotEqual(t, nil, err, "Must error on parse: %s", sql)
}

func TestSqlParser(t *testing.T) {

	parseSqlError(t, `SELECT a FROM x LIMIT 1 NOTAWORD;`)

	parseSqlError(t, `SELECT a, tolower(b) AS b INTO newtable FROM FROM WHERE a != "hello";`)
	parseSqlTest(t, `
		SELECT a.language, a.template, Count(*) AS count
		FROM 
			(Select Distinct language, template FROM content WHERE language != "en" OFFSET 1) AS a
			Left Join users AS b
				On b.language = a.language AND b.template = b.template
		GROUP BY a.language, a.template`)

	// CREATE
	parseSqlTest(t, `CREATE CONTINUOUSVIEW viewx AS SELECT a FROM tbl;`)
	parseSqlError(t, `CREATE FAKEITEM viewx;`)

	// DROP
	parseSqlTest(t, `DROP CONTINUOUSVIEW viewx WITH stuff = "hello";`)
}
