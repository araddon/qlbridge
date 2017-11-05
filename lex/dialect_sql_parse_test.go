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

	parseSqlTest(t, `CREATE TABLE articles 
		--comment-here
		(
		 ID int(11) NOT NULL AUTO_INCREMENT,
		 Email char(150) NOT NULL DEFAULT '' UNIQUE COMMENT 'this-is-comment',
		 stuff varchar(150),
		 profile text,
		 PRIMARY KEY (ID),
		 visitct BIGINT,
		 CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email)
	   ) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8;`)
	/*
	      http://dev.mysql.com/doc/refman/5.7/en/create-table.html

	      create_definition:
	   	   col_name column_definition
	   	 | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
	   		 [index_option] ...
	   	 | {INDEX|KEY} [index_name] [index_type] (index_col_name,...)
	   		 [index_option] ...
	   	 | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY]
	   		 [index_name] [index_type] (index_col_name,...)
	   		 [index_option] ...
	   	 | {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (index_col_name,...)
	   		 [index_option] ...
	   	 | [CONSTRAINT [symbol]] FOREIGN KEY
	   		 [index_name] (index_col_name,...) reference_definition
	   	 | CHECK (expr)

	      column_definition:
	   	   data_type [NOT NULL | NULL] [DEFAULT default_value]
	   		 [AUTO_INCREMENT] [UNIQUE [KEY] | [PRIMARY] KEY]
	   		 [COMMENT 'string']
	   		 [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
	   		 [STORAGE {DISK|MEMORY|DEFAULT}]
	   		 [reference_definition]
	   	 | data_type [GENERATED ALWAYS] AS (expression)
	   		 [VIRTUAL | STORED] [UNIQUE [KEY]] [COMMENT comment]
	   		 [NOT NULL | NULL] [[PRIMARY] KEY]
	*/

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
	parseSqlTest(t, `CREATE OR REPLACE VIEW viewx 
		AS SELECT a, b FROM mydb.tbl 
		WITH stuff = "hello";`)

	// DROP
	parseSqlTest(t, `DROP CONTINUOUSVIEW viewx WITH stuff = "hello";`)
}
