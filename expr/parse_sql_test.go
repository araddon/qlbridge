package expr

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/bmizerany/assert"
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

	// TODO:
	//parseSqlTest(t, `INSERT INTO events (id,event_date,event) SELECT id,last_logon,"last_logon" FROM users;`)
	// parseSqlTest(t, `REPLACE INTO tbl_3 (id,lastname) SELECT id,lastname FROM tbl_1;`)
	parseSqlTest(t, `insert into mytable (id, str) values (0, 'a')`)
	parseSqlTest(t, `upsert into mytable (id, str) values (0, 'a')`)
	parseSqlTest(t, `insert into mytable (id, str) values (0, 'a'),(1,'b');`)

	parseSqlTest(t, `SELECT LAST_INSERT_ID();`)
	parseSqlTest(t, `SELECT CHARSET();`)
	parseSqlTest(t, `SELECT DATABASE()`)
	parseSqlTest(t, `select @@version_comment limit 1`)

	parseSqlTest(t, `DESCRIBE mytable`)
	parseSqlTest(t, `show tables`)

	parseSqlTest(t, `select director, year from movies where year BETWEEN 2000 AND 2010;`)
	parseSqlTest(t, `select director, year from movies where director like 'Quentin'`)
	parseSqlTest(t, `select director, year from movies where !exists(user_id) OR toint(not_a_field) > 21`)
	parseSqlTest(t, `select count(*) from user;   `)
	parseSqlTest(t, `select name from movies where director IN ("Quentin","copola","Bay","another")`)
	parseSqlTest(t, `select id, name from users LIMIT 100 OFFSET 1000`)
	parseSqlTest(t, `SELECT 
            lol AS notlol IF hey == 0
        FROM nothing
        WHERE this != that;`)
	parseSqlTest(t, `
		SELECT 
			t1.name, t2.salary
		FROM employee AS t1 
		INNER JOIN info AS t2 
		ON t1.name = t2.name;`)
	parseSqlTest(t, `
		SELECT 
			t1.name, t2.salary
		FROM employee AS t1 
		INNER JOIN info AS t2 
		ON t1.name = t2.name;`)
	parseSqlTest(t, `select
	        user_id, email
	    FROM mockcsv.users
	    WHERE user_id in
	    	(select user_id from mockcsv.orders)`)
	// Currently unsupported
	//parseSqlTest(t, `select user_id, email FROM mockcsv.users
	//    WHERE tolower(email) IN (select email from mockcsv.orders)`)

	parseSqlTest(t, `PREPARE stmt1 FROM 'SELECT toint(field) + 4 AS field FROM table1';`)

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

func TestSqlParseAstCheck(t *testing.T) {

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
	assert.Tf(t, gb.Expr != nil, "")
	n := gb.Expr.(*IdentityNode)
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
	assert.Tf(t, sel.Where != nil, "where not nil?: %v", sel.Where.String())
	assert.Tf(t, sel.Where.String() == "`actor.id` < 1000", "is where: %v", sel.Where.String())
	// We also need to ensure that the From[].Sources are populated?  Is this expected or needed?
	// assert.Tf(t, len(sel.From) == 1, "Has 1 from")
	// assert.Tf(t, len(sel.From[0].Columns) == 2, "wanted 2 columns in from: %#v", sel.From[0])

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
		ORDER BY ` + "`repository.stargazers`" + ` DESC
		limit 9;`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, sel.Limit == 9, "want limit = 9 but have %v", sel.Limit)
	assert.Tf(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	assert.Tf(t, sel.OrderBy[0].String() == "`repository.stargazers` DESC", "want orderby but has %v", sel.OrderBy[0].String())

	// Unknown keyword SORT
	sql = "select `repository.name` from github_fork SORT BY `repository.stargazers_count` DESC limit 3"
	_, err = ParseSql(sql)
	assert.Tf(t, err != nil, "Must fail parse: %v", err)
	//assert.Tf(t, reqNil == nil, "Must fail parse: %v", reqNil)

	sql = `select repository.name, respository.language, repository.stargazers 
		FROM github_fork 
		WHERE 
			eq(repository.name,"dataux") 
			AND repository.language = "go"
			AND repository.name NOT LIKE "docker"
	`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	//assert.Tf(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.Where.String())

	sql = `
		SELECT
			actor, repository.name, repository.stargazers_count, repository.language 
		FROM github_watch
		WHERE
				repository.language = "go" 
				AND repository.forks_count > 1000 
				AND repository.description NOT LIKE "docker";`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	//assert.Tf(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.Where.String())

	sql = `
		EXPLAIN EXTENDED SELECT
			actor, repository.name, repository.stargazers_count, repository.language 
		FROM github_watch
		WHERE
				repository.language = "go" 
				AND repository.forks_count > 1000 
				AND repository.description NOT LIKE "docker";`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	desc, ok := req.(*SqlDescribe)
	assert.Tf(t, ok, "is SqlDescribe: %T", req)
	sel, ok = desc.Stmt.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	u.Info(sel.Where.String())

	// Where In Sub-Query Clause
	sql = `select user_id, email
				FROM mockcsv.users
				WHERE user_id in
					(select user_id from mockcsv.orders)`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, len(sel.From) == 1, "has 1 from: %v", sel.From)
	assert.Tf(t, sel.Where != nil && sel.Where.NodeType() == SqlWhereNodeType, "has sub-select: %v", sel.Where)
	u.Infof("sel:  %#v", sel.Where)

}

func TestSqlParseFromTypes(t *testing.T) {

	sql := `select gh.repository.name, gh.id, gp.date 
		FROM github_fork as gh
		INNER JOIN github_push AS gp ON gp.repo_id = gh.repo_id
		WHERE 
			gh.repository.language = "go"
	`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	sel.Rewrite()
	assert.Tf(t, len(sel.From) == 2, "wanted 2 froms but got %v", len(sel.From))
	//assert.Tf(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.String())

	// Try compound join keys
	sql = `select u.fname, u.lname, u.userid, b.description
		FROM user as u
		INNER JOIN blog AS b ON b.first_name = u.fname AND b.last_name = u.lname
	`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel = req.(*SqlSelect)
	sel.Rewrite()
	user := sel.From[0]
	//u.Infof("join nodes:   %q", user.JoinExpr.String())
	//blog := sel.From[1]
	assert.Tf(t, user.Source != nil, "")
}

func TestSqlShow(t *testing.T) {
	/*
		SHOW [FULL] TABLES [{FROM | IN} db_name]
		[LIKE 'pattern' | WHERE expr]
	*/
	sql := `show tables`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok := req.(*SqlShow)
	assert.Tf(t, ok, "is SqlShow: %T", req)
	assert.Tf(t, cmd.Identity == "tables", "has SHOW kw: %#v", cmd)
	/*
		assert.Tf(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "autocommit", "has autocommit: %#v", cmd.Columns)

		sql = `SET @@local.sort_buffer_size=10000;`
		req, err = ParseSql(sql)
		assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
		cmd, ok = req.(*SqlCommand)
		assert.Tf(t, ok, "is SqlCommand: %T", req)
		assert.Tf(t, cmd.Keyword() == lex.TokenSet, "has SET kw: %#v", cmd)
		assert.Tf(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "@@local.sort_buffer_size", "has autocommit: %#v", cmd.Columns)

		sql = "USE `myschema`;"
		req, err = ParseSql(sql)
		assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
		cmd, ok = req.(*SqlCommand)
		assert.Tf(t, ok, "is SqlCommand: %T", req)
		assert.Tf(t, cmd.Keyword() == lex.TokenUse, "has USE kw: %#v", cmd)
		assert.Tf(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "myschema", "has myschema: %#v", cmd.Columns[0])
	*/
}

func TestSqlCommands(t *testing.T) {
	// Administrative commands
	sql := `set autocommit`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok := req.(*SqlCommand)
	assert.Tf(t, ok, "is SqlCommand: %T", req)
	assert.Tf(t, cmd.Keyword() == lex.TokenSet, "has SET kw: %#v", cmd)
	assert.Tf(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "autocommit", "has autocommit: %#v", cmd.Columns)

	sql = `SET @@local.sort_buffer_size=10000;`
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok = req.(*SqlCommand)
	assert.Tf(t, ok, "is SqlCommand: %T", req)
	assert.Tf(t, cmd.Keyword() == lex.TokenSet, "has SET kw: %#v", cmd)
	assert.Tf(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "@@local.sort_buffer_size", "has autocommit: %#v", cmd.Columns)

	sql = "USE `myschema`;"
	req, err = ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok = req.(*SqlCommand)
	assert.Tf(t, ok, "is SqlCommand: %T", req)
	assert.Tf(t, cmd.Keyword() == lex.TokenUse, "has USE kw: %#v", cmd)
	assert.Tf(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "myschema", "has myschema: %#v", cmd.Columns[0])
}

func TestSqlAlias(t *testing.T) {
	// This is obviously not exactly sql standard
	// but is an alternate syntax to prepared statement
	sql := `
		SELECT id, name FROM user
		ALIAS user_query
		`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, len(sel.From) == 1 && sel.From[0].Name == "user", "has 1 from: %v", sel.From)
	assert.Tf(t, sel.Alias == "user_query", "has alias: %v", sel.Alias)
}

func TestSqlUpsert(t *testing.T) {
	// This is obviously not exactly sql standard
	// but many key/value and other document stores support it
	sql := `upsert into users (id, str) values (0, 'a')`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	up, ok := req.(*SqlUpsert)
	assert.Tf(t, ok, "is SqlUpsert: %T", req)
	assert.Tf(t, up.Table == "users", "has users: %v", up.Table)
	//assert.Tf(t, sel.Alias == "user_query", "has alias: %v", sel.Alias)
}

func TestSqlUpdate(t *testing.T) {
	//
	sql := `UPDATE users SET name = "was_updated", [deleted] = true WHERE id = "user815"`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	up, ok := req.(*SqlUpdate)
	assert.Tf(t, ok, "is SqlUpdate: %T", req)
	assert.Tf(t, up.Table == "users", "has users: %v", up.Table)
	assert.Tf(t, len(up.Values) == 2, "%v", up)
}

func TestWithJson(t *testing.T) {
	// This is obviously not exactly sql standard
	// but is nice for proxy's
	sql := `
		SELECT id, name FROM user
		WITH {
			"key":"value2"
			,"keyint":45,
			"keyfloat":55.5, 
			"keybool": true,
			"keyarraymixed":["a",2,"b"],
			"keyarrayobj":[
				{"hello":"value","age":55},
				{"hello":"value","age":55}
			],
			"keyobj":{"hello":"value","age":55},
			"keyobjnested":{
				"hello":"value",
				"array":[
					"a",
					2,
					"b"
				]
			}
		}
		`
	req, err := ParseSql(sql)
	assert.Tf(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*SqlSelect)
	assert.Tf(t, ok, "is SqlSelect: %T", req)
	assert.Tf(t, len(sel.From) == 1 && sel.From[0].Name == "user", "has 1 from: %v", sel.From)
	assert.Tf(t, len(sel.With) == 8, "has with: %v", sel.With)
	assert.Tf(t, len(sel.With.Helper("keyobj")) == 2, "has 2obj keys: %v", sel.With.Helper("keyobj"))
	u.Infof("sel.With:  \n%s", sel.With.PrettyJson())
}
