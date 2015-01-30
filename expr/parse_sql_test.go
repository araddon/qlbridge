package expr

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
	assert.Tf(t, err == nil && sqlRequest != nil, "Must parse: %s  \n\t%v", sql, err)
}

func TestSqlLexOnly(t *testing.T) {

	parseSqlTest(t, `SELECT LAST_INSERT_ID();`)
	parseSqlTest(t, `SELECT CHARSET();`)
	parseSqlTest(t, `SELECT DATABASE()`)
	parseSqlTest(t, `select @@version_comment limit 1`)
	parseSqlTest(t, `insert into mytable (id, str) values (0, 'a')`)
	parseSqlTest(t, `DESCRIBE mytable`)
	parseSqlTest(t, `show tables`)

	parseSqlTest(t, `select director, year from movies where year BETWEEN 2000 AND 2010;`)
	parseSqlTest(t, `select director, year from movies where director like 'Quentin'`)

	parseSqlTest(t, `select count(*) from user;`)

	parseSqlTest(t, `select
	        user_id, email
	    FROM mockcsv.users
	    WHERE user_id in
	    	(select user_id from mockcsv.orders)`)

	parseSqlTest(t, `PREPARE stmt1 FROM 'SELECT toint(field) + 4 AS field FROM table1';`)
	parseSqlTest(t, `select name from movies where director IN ("Quentin","copola","Bay","another")`)

	/*
		SELECT    color, year, tags, price
		FROM      cars
		WHERE     QUERY IS "cool"
		AND       tags CONTAINS ALL ("cool", "hybrid") EXCEPT ("favorite")
		AND       color in ("red")
		ORDER BY  price desc
		LIMIT     0,10
		BROWSE BY color(true, 1, 10, hits), year(true, 1, 10, value), price
	*/
}

func TestSqlParse(t *testing.T) {

	sql := `
	SELECT terms(repository.description)
	FROM github_member
	GROUP BY repository.language, author
	`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, len(sel.GroupBy) == 2, "has 2 group by: %v", sel.GroupBy)
	gb := sel.GroupBy[0]
	assert.Tf(t, gb.Tree != nil && gb.Tree.Root != nil, "")
	n := gb.Tree.Root.(*IdentityNode)
	assert.Tf(t, n.String() == "repository.language", "%v", n)
	assert.Tf(t, n.String() == "repository.language", "%v", n)

	sql = `select @@version_comment limit 7`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, sel.Limit == 7, "has limit = 7: %v", sel.Limit)
	assert.Tf(t, len(sel.Columns) == 1, "has 1 col: %v", len(sel.Columns))
	assert.Tf(t, sel.Columns[0].As == "@@version_comment", "")

	sql = "select `repository.full_name` from `github_public` ORDER BY `respository.full_name` asc, TOINT(`fieldname`) DESC limit 100;"
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, sel.Limit == 100, "want limit = 100 but have %v", sel.Limit)
	assert.Tf(t, len(sel.OrderBy) == 2, "want 2 orderby but has %v", len(sel.OrderBy))
	assert.Tf(t, sel.OrderBy[0].Order == "ASC", "%v", sel.OrderBy[0].String())
	assert.Tf(t, sel.OrderBy[1].Order == "DESC", "%v", sel.OrderBy[1].String())

	sql = "select `actor.id`, `actor.login` from github_watch where `actor.id` < 1000"
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, len(sel.Columns) == 2, "want 2 Columns but has %v", len(sel.Columns))
	assert.Tf(t, sel.Where != nil && sel.Where.StringAST() == "actor.id < 1000", "is where: %v", sel.Where.StringAST())

	sql = `select repository.name, repository.stargazers from github_fork GROUP BY repository.name ORDER BY repository.stargazers DESC;`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)

	sql = `select repository.name, repository.stargazers 
		FROM github_fork 
		WHERE eq(repository.name,"dataux")
		GROUP BY repository.name 
		HAVING eq(repository.name,"dataux")
		ORDER BY repository.stargazers DESC
		limit 9;`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, sel.Limit == 9, "want limit = 9 but have %v", sel.Limit)
	assert.Tf(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
}
