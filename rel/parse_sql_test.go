package rel_test

import (
	"flag"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = u.EMPTY
)

func init() {
	lex.IDENTITY_CHARS = lex.IDENTITY_SQL_CHARS
	flag.Parse()
	builtins.LoadAllBuiltins()
}

func parseSqlTest(t *testing.T, sql string) {
	u.Debugf("parsing sql: %s", sql)
	sqlRequest, err := rel.ParseSql(sql)
	assert.Equal(t, nil, err, "%v", err)
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
func TestSqlShow(t *testing.T) {
	t.Parallel()
	parseSqlTest(t, "SHOW FULL TABLES FROM `temp_schema` LIKE '%'")
	parseSqlTest(t, "SHOW CREATE TABLE `temp_schema`.`users`")
	parseSqlTest(t, `show session status like "ssl_cipher"`)
}

func TestSqlKeywordEscape(t *testing.T) {
	sel, err := rel.ParseSql("SELECT form_track_form AS form_track_form, `from` AS `from` FROM user")
	assert.Equal(t, nil, err)
	parseSqlTest(t, sel.String())
}

func TestSqlParseFail(t *testing.T) {
	tests := []string{
		`--hello
		DELETE from users where user_id > 10;`,
	}
	for _, stmt := range tests {
		_, err := rel.ParseSqlSelect(stmt)
		assert.NotEqual(t, nil, err, "Expected err for %v", stmt)
	}

	_, err := rel.ParseSqlSelectResolver(`DELETE from users where user_id > 10;`, nil)
	assert.NotEqual(t, nil, err)
	_, err = rel.ParseSqlSelectResolver(`SELECT x, y FROM user LIMIT;`, nil)
	assert.NotEqual(t, nil, err)
	_, err = rel.ParseSqlSelectResolver("SELECT form_track_form AS form_track_form, `from` AS `from` FROM user", nil)
	assert.Equal(t, nil, err)

	tests = []string{
		`SELECT x, y 
		-- a comment
		FROM user LIMIT "hello";`,
		`SELECT "hello" LIMIT "5x"`,
	}
	for _, stmt := range tests {
		_, err = rel.ParseSql(stmt)
		assert.NotEqual(t, nil, err, "Expected err for %v", stmt)
		_, err = rel.ParseSqlSelect(stmt)
		assert.NotEqual(t, nil, err, "Expected err for %v", stmt)
		_, err = rel.ParseSqlStatements(stmt)
		assert.NotEqual(t, nil, err, "Expected err for %v", stmt)
	}
}

func TestSqlParseOnly(t *testing.T) {
	t.Parallel()

	parseSqlTest(t, `
	SELECT exists(firstname), x 
	-- lets use the user table
	-- another for good meausre
	FROM user
	-- a comment
	WHERE x = y;
	`)

	parseSqlTest(t, "SELECT exists(firstname), user_id FROM user")

	parseSqlTest(t, `
	SELECT exists(firstname), x 
	-- lets use the user table
	-- another for good meausre
	FROM user;
	`)

	parseSqlTest(t, `
	SELECT 
			event                           AS my_event
			                                            IF event != "stuff"
			                                            AND NOT(hasprefix(event,"gh."))
			                                            AND NOT(hasprefix(event,"software."))
			                                            AND NOT(hasprefix(event,"devstatus."))
			                                            AND NOT(hasprefix(event,"devstatus."))
			                                            AND NOT(hasprefix(event,"devsummary."))
			                                            AND NOT(hasprefix(event,"dvcsconnector."))
	FROM nothing`)
	parseSqlTest(t, `
		SELECT event FROM nothing
		WHERE
			(
				not(exists(@@whitelist)) 
				OR len(@@whitelist) == 0 
				OR host(url) IN hosts(@@whitelist)
			) 
			AND exists(version) 
			AND eq(version, 4)
	`)

	parseSqlTest(t, `
		SELECT a.language, a.template, Count(*) AS count
		FROM 
			(Select Distinct language, template FROM content) AS a
			Left Join users AS b
				On b.language = a.language AND b.template = b.template
		GROUP BY a.language, a.template`)

	parseSqlTest(t, `SELECT 
            lol AS notlol IF AND (
                    or (
                        event IN ("rq", "ab"),
                        NOT EXISTS event
                    )
                    product IN ("my", "app")
                )
        FROM nothing
        WHERE this != that;`)

	parseSqlError(t, "SELECT x FROM user WHERE ex(a,b")
	parseSqlError(t, "SELECT x FROM user GROUP BY ex(a,b")
	parseSqlError(t, "SELECT x FROM user GROUP BY x HAVING ct > count(x,;")
	parseSqlError(t, "SELECT x FROM user ORDER BY ex(a,;")
	parseSqlError(t, `SELECT x FROM user OFFSET "hello";`)
	parseSqlError(t, `SELECT x FROM user WITH "hello";`)
	parseSqlError(t, `SELECT x FROM user ALIAS 12;`)

	parseSqlError(t, "SELECT hash(a,,) AS id, `z` FROM nothing;")
	parseSqlError(t, "SELECT a, b INTO FROM user;")
	//parseSqlError(t, "SELECT a FROM user WHERE x")
	parseSqlError(t, "SELECT hash(join(, \", \")) AS id, `x`, `y`, `z` FROM nothing;")

	parseSqlTest(t, "SELECT COUNT(*) AS count FROM providers WHERE (`providers._id` != NULL)")

	parseSqlTest(t, "select title from article WITH distributed=true, node_ct=10")
	parseSqlTest(t, "SELECT `appearances`.`G_ph` AS `field` FROM `appearances` ORDER BY `appearances`.`G_ph` ASC LIMIT 500 OFFSET 0")

	parseSqlTest(t, `
		select  @@session.auto_increment_increment as auto_increment_increment, 
					@@character_set_client as character_set_client, 
					@@character_set_connection as character_set_connection, 
					@@character_set_results as character_set_results, 
					@@character_set_server as character_set_server, 
					@@init_connect as init_connect, 
					@@interactive_timeout as interactive_timeout, 
					@@license as license, 
					@@lower_case_table_names as lower_case_table_names,
					@@max_allowed_packet as max_allowed_packet, 
					@@net_buffer_length as net_buffer_length, 
					@@net_write_timeout as net_write_timeout, 
					@@query_cache_size as query_cache_size, 
					@@query_cache_type as query_cache_type, 
					@@sql_mode as sql_mode, 
					@@system_time_zone as system_time_zone, 
					@@time_zone as time_zone, 
					@@tx_isolation as tx_isolation, 
					@@wait_timeout as wait_timeout
	`)

	parseSqlTest(t, `
		SELECT a.language, a.template, Count(*) AS count
		FROM 
			(Select Distinct language, template FROM content) AS a
			Left Join users AS b
				On b.language = a.language AND b.template = b.template
		GROUP BY a.language, a.template`)

	parseSqlTest(t, `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN (
				SELECT price, order_date, user_id from ORDERS
				WHERE user_id IS NOT NULL AND price > 10
			) AS o 
			ON u.user_id = o.user_id
	`)

	parseSqlTest(t, `
		SELECT 
			t1.name, t2.salary, t3.price
		FROM employee AS t1 
		INNER JOIN info AS t2 
			ON t1.name = t2.name
		INNER JOIN orders AS t3
			ON t3.id = t2.fake_id;`)

	// TODO:
	//parseSqlTest(t, `INSERT INTO events (id,event_date,event) SELECT id,last_logon,"last_logon" FROM users;`)
	// parseSqlTest(t, `REPLACE INTO tbl_3 (id,lastname) SELECT id,lastname FROM tbl_1;`)
	parseSqlTest(t, `insert into mytable (id, str) values (0, "a")`)
	parseSqlTest(t, `upsert into mytable (id, str) values (0, "a")`)
	parseSqlTest(t, `insert into mytable (id, str) values (0, "a"),(1,"b");`)

	parseSqlError(t, `INSERT "a"`)
	parseSqlError(t, `INSERT INTO 12`)
	parseSqlError(t, `insert into mytable (id, str;`)
	parseSqlError(t, `insert into mytable (id, str)
		SELECT a FROM;`)

	parseSqlTest(t, `SELECT LAST_INSERT_ID();`)
	parseSqlTest(t, `SELECT CHARSET();`)
	parseSqlTest(t, `SELECT DATABASE()`)
	parseSqlTest(t, `select @@version_comment limit 1`)
	parseSqlTest(t, `select @@version_comment limit 1;`)

	parseSqlTest(t, `rollback`)
	parseSqlTest(t, `DESCRIBE mytable`)

	parseSqlTest(t, `CREATE SOURCE mysource;`)
	parseSqlTest(t, `CREATE OR REPLACE VIEW viewx 
		AS SELECT a, b FROM mydb.tbl 
		WITH stuff = "hello";`)
	parseSqlTest(t, `CREATE schema IF NOT EXISTS github_archive WITH {
       "type":"elasticsearch",
       "schema":"github_archive",
       "hosts": ["http://127.0.0.1:9200"] 
    };`)

	parseSqlTest(t, `show tables`)
	parseSqlTest(t, `show tables LIKE "user%";`)
	parseSqlTest(t, `show databases`)
	parseSqlTest(t, "SHOW FULL COLUMNS FROM `tablex` FROM `dbx` LIKE '%';")
	parseSqlTest(t, `SHOW VARIABLES`)
	parseSqlTest(t, `SHOW GLOBAL VARIABLES like '%'`)
	parseSqlTest(t, "show keys from `appearances` from `baseball`")
	parseSqlTest(t, "show indexes from `appearances` from `baseball`")
	//parseSqlTest(t, `SHOW VARIABLES where `)

	parseSqlTest(t, `select *, @@var_name from movies`)
	parseSqlTest(t, `select *, toint(a_field) AS ti from movies`)
	parseSqlTest(t, `select *, 12 AS twelve from movies`)
	parseSqlTest(t, `select toint(a_field) AS ti, * from movies`)
	parseSqlTest(t, `select 3, director from movies`)
	parseSqlTest(t, `select director, year from movies where year BETWEEN 2000 AND 2010;`)
	parseSqlTest(t, `select director, year from movies where director like 'Quentin'`)
	parseSqlTest(t, `select director, year from movies where !exists(user_id) OR toint(not_a_field) > 21`)
	parseSqlTest(t, `select count(*) from user;   `)
	parseSqlTest(t, `select name from movies where director IN ("Quentin","copola","Bay","another")`)
	parseSqlTest(t, `select id, name from users LIMIT 100 OFFSET 1000`)
	parseSqlTest(t, `SELECT count(*), email FROM users WHERE emaildomain(email) = "gmail.com" GROUP BY email WITH distributed = true;`)
	parseSqlTest(t, "select url, `_nmob`, `_cc`, `_uida` from events123 WHERE exists(url) LIMIT 500 WITH distributed = true;")

	parseSqlTest(t, `SELECT 
            lol AS notlol IF hey == 0
        FROM nothing
        WHERE this != that;`)

	parseSqlTest(t, `SELECT 
            lol AS notlol IF AND (
                    or (
                        event IN ("rq", "ab"),
                        NOT EXISTS event
                    )
                    product IN ("my", "app")
                )
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
	t.Parallel()
	sql := `
	SELECT terms(repository.description)
	FROM github_member
	GROUP BY repository.language, author
	`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, len(sel.GroupBy) == 2, "has 2 group by: %v", sel.GroupBy)
	gb := sel.GroupBy[0]
	assert.True(t, gb.Expr != nil, "")
	n := gb.Expr.(*expr.IdentityNode)
	assert.True(t, n.String() == "repository.language", "%v", n)
	assert.True(t, n.String() == "repository.language", "%v", n)

	sql = `select @@version_comment limit 7`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, sel.IsLiteral(), "Should be literal? %v", sql)
	assert.True(t, sel.Limit == 7, "has limit = 7: %v", sel.Limit)
	assert.True(t, len(sel.Columns) == 1, "has 1 col: %v", len(sel.Columns))
	assert.True(t, sel.Columns[0].As == "@@version_comment", "")

	sql = "select `repository.full_name` from `github_public` ORDER BY `respository.full_name` asc, TOINT(`fieldname`) DESC limit 100;"
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, sel.Limit == 100, "want limit = 100 but have %v", sel.Limit)
	assert.True(t, len(sel.OrderBy) == 2, "want 2 orderby but has %v", len(sel.OrderBy))
	assert.True(t, sel.OrderBy[0].Order == "ASC", "%v", sel.OrderBy[0].String())
	assert.True(t, sel.OrderBy[1].Order == "DESC", "%v", sel.OrderBy[1].String())

	sql = "select name from `github_public` limit 0, 100;"
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel = req.(*rel.SqlSelect)
	assert.True(t, sel.Limit == 100, "want limit = 100 but have %v", sel.Limit)
	assert.True(t, sel.Offset == 0, "want offset = 0 but have %v", sel.Offset)

	sql = "select `actor.id`, `actor.login` from github_watch where `actor.id` < 1000"
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, len(sel.Columns) == 2, "want 2 Columns but has %v", len(sel.Columns))
	assert.True(t, sel.Where != nil, "where not nil?: %v", sel.Where.String())
	assert.Equal(t, sel.Where.String(), "`actor.id` < 1000", "is where: %v", sel.Where.String())
	// We also need to ensure that the From[].Sources are populated?  Is this expected or needed?
	// assert.True(t, len(sel.From) == 1, "Has 1 from")
	// assert.True(t, len(sel.From[0].Columns) == 2, "wanted 2 columns in from: %#v", sel.From[0])

	sql = `select repository.name, repository.stargazers from github_fork GROUP BY repository.name ORDER BY repository.stargazers DESC;`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)

	sql = `select repository.name, repository.stargazers 
		FROM github_fork 
		WHERE eq(repository.name,"dataux")
		GROUP BY repository.name 
		HAVING eq(repository.name,"dataux")
		ORDER BY ` + "`repository.stargazers`" + ` DESC
		limit 9;`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, sel.Limit == 9, "want limit = 9 but have %v", sel.Limit)
	assert.True(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	assert.True(t, sel.OrderBy[0].String() == "`repository.stargazers` DESC", "want orderby but has %v", sel.OrderBy[0].String())

	// Unknown keyword SORT
	sql = "select `repository.name` from github_fork SORT BY `repository.stargazers_count` DESC limit 3"
	_, err = rel.ParseSql(sql)
	assert.True(t, err != nil, "Must fail parse: %v", err)
	//assert.True(t, reqNil == nil, "Must fail parse: %v", reqNil)

	sql = `select repository.name, respository.language, repository.stargazers 
		FROM github_fork 
		WHERE 
			eq(repository.name,"dataux") 
			AND repository.language = "go"
			AND repository.name NOT LIKE "docker"
	`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	//assert.True(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.Where.String())

	sql = `
		SELECT
			actor, repository.name, repository.stargazers_count, repository.language 
		FROM github_watch
		WHERE
				repository.language = "go" 
				AND repository.forks_count > 1000 
				AND repository.description NOT LIKE "docker";`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	//assert.True(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.Where.String())

	sql = `
		EXPLAIN EXTENDED SELECT
			actor, repository.name, repository.stargazers_count, repository.language 
		FROM github_watch
		WHERE
				repository.language = "go" 
				AND repository.forks_count > 1000 
				AND repository.description NOT LIKE "docker";`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	desc, ok := req.(*rel.SqlDescribe)
	assert.True(t, ok, "is SqlDescribe: %T", req)
	sel, ok = desc.Stmt.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	u.Info(sel.Where.String())

	// Where In Sub-Query Clause
	sql = `select user_id, email
				FROM mockcsv.users
				WHERE user_id in
					(select user_id from mockcsv.orders)`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, len(sel.From) == 1, "has 1 from: %v", sel.From)
	assert.True(t, sel.Where != nil && sel.Where.Source != nil, "has sub-select: %v", sel.Where)
}

func TestSqlAggregateTypeSelect(t *testing.T) {
	t.Parallel()
	sql := `select avg(char_length(title)) from article`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	sel.Rewrite()
	assert.True(t, sel.IsAggQuery(), "wanted IsAggQuery()==true but got false")
}

func TestSqlParseFromTypes(t *testing.T) {
	t.Parallel()
	sql := `select gh.repository.name, gh.id, gp.date 
		FROM github_fork as gh
		INNER JOIN github_push AS gp ON gp.repo_id = gh.repo_id
		WHERE 
			gh.repository.language = "go"
	`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	sel.Rewrite()
	assert.True(t, len(sel.From) == 2, "wanted 2 froms but got %v", len(sel.From))
	//assert.True(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.String())

	// Try compound join keys
	sql = `select u.fname, u.lname, u.userid, b.description
		FROM user as u
		INNER JOIN blog AS b ON b.first_name = u.fname AND b.last_name = u.lname
	`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel = req.(*rel.SqlSelect)
	sel.Rewrite()
	user := sel.From[0]
	//u.Infof("join nodes:   %q", user.JoinExpr.String())
	//blog := sel.From[1]
	assert.True(t, user.Source != nil, "")

	// 3 join tables
	sql = `
		SELECT 
			t1.name, t2.salary, t3.price
		FROM employee AS t1 
		INNER JOIN info AS t2 
			ON t1.name = t2.name
		INNER JOIN orders AS t3
			ON t3.id = t2.fake_id;`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok = req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, len(sel.From) == 3, "has 3 from: %v", sel.From)
	//assert.True(t, len(sel.OrderBy) == 1, "want 1 orderby but has %v", len(sel.OrderBy))
	u.Info(sel.String())
}

func TestSqlShowAst(t *testing.T) {
	t.Parallel()
	/*
		SHOW [FULL] TABLES [{FROM | IN} db_name]
		[LIKE 'pattern' | WHERE expr]
	*/
	sql := `show tables`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	show, ok := req.(*rel.SqlShow)
	assert.True(t, ok, "is SqlShow: %T", req)
	assert.True(t, show.ShowType == "tables", "has SHOW kw: %#v", show)

	sql = "SHOW FULL COLUMNS FROM `tablex` FROM `dbx` LIKE '%';"
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	show, ok = req.(*rel.SqlShow)
	assert.True(t, ok, "is SqlShow: %T", req)
	assert.True(t, show.Full, "Wanted full")
	assert.True(t, show.ShowType == "columns", "has SHOW 'Columns'? %#v", show)
	assert.True(t, show.Db == "dbx", "has SHOW db: %q", show.Db)
	assert.True(t, show.Identity == "tablex", "has identity: %q", show.Identity)
	assert.True(t, show.Like.String() == "Field LIKE \"%\"", "has Like? %q", show.Like.String())
}

func TestSqlCommands(t *testing.T) {
	t.Parallel()
	// Administrative commands
	sql := `set autocommit`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok := req.(*rel.SqlCommand)
	assert.True(t, ok, "is SqlCommand: %T", req)
	assert.True(t, cmd.Keyword() == lex.TokenSet, "has SET kw: %#v", cmd)
	assert.True(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "autocommit", "has autocommit: %#v", cmd.Columns)

	sql = `SET @@local.sort_buffer_size=10000;`
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok = req.(*rel.SqlCommand)
	assert.True(t, ok, "is SqlCommand: %T", req)
	assert.True(t, cmd.Keyword() == lex.TokenSet, "has SET kw: %#v", cmd)
	assert.True(t, len(cmd.Columns) == 1 && cmd.Columns[0].Name == "@@local.sort_buffer_size", "has autocommit: %#v", cmd.Columns)

	sql = "USE `myschema`;"
	req, err = rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	cmd, ok = req.(*rel.SqlCommand)
	assert.True(t, ok, "is SqlCommand: %T", req)
	assert.True(t, cmd.Keyword() == lex.TokenUse, "has USE kw: %#v", cmd)
	assert.True(t, cmd.Identity == "myschema", "has myschema: %#v", cmd.Identity)
}

func TestSqlAlias(t *testing.T) {
	t.Parallel()
	// This is obviously not exactly sql standard
	// but is an alternate syntax to prepared statement
	sql := `
		SELECT id, name FROM user
		ALIAS user_query
		`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, len(sel.From) == 1 && sel.From[0].Name == "user", "has 1 from: %v", sel.From)
	assert.True(t, sel.Alias == "user_query", "has alias: %v", sel.Alias)
}

func TestSqlUpsert(t *testing.T) {
	t.Parallel()
	// This is obviously not exactly sql standard
	// but many key/value and other document stores support it
	sql := `upsert into users (id, str) values (0, 'a')`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	up, ok := req.(*rel.SqlUpsert)
	assert.True(t, ok, "is SqlUpsert: %T", req)
	assert.True(t, up.Table == "users", "has users: %v", up.Table)
	//assert.True(t, sel.Alias == "user_query", "has alias: %v", sel.Alias)
}

func TestSqlMultiStatement(t *testing.T) {
	t.Parallel()
	sql := `SET @var1 = "hello"; select a, b from accounts where name = @var1;`
	stmts, err := rel.ParseSqlStatements(sql)
	assert.True(t, err == nil, "Must parse: %s  \n\t%v", sql, err)
	assert.True(t, len(stmts) == 2, "want 2 statements has %d", len(stmts))
	set, ok := stmts[0].(*rel.SqlCommand)
	assert.True(t, ok, "Wanted *SqlCommand but got %T", stmts[0])
	assert.True(t, set.Keyword().String() == "set", "Wanted set statement %v", set.Keyword().String())

	sel, ok := stmts[1].(*rel.SqlSelect)
	assert.True(t, ok, "wanted *SqlUpdate but got %T", stmts[1])
	assert.True(t, sel.From[0].Name == "accounts", "has accounts: %v", sel.From[0])
	assert.True(t, len(sel.Columns) == 2, "want 2 cols has %v", len(sel.Columns))
}

func TestSqlUpdate(t *testing.T) {
	t.Parallel()
	sql := `UPDATE users SET name = "was_updated", [deleted] = true WHERE id = "user815"`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	up, ok := req.(*rel.SqlUpdate)
	assert.True(t, ok, "is SqlUpdate: %T", req)
	assert.True(t, up.Table == "users", "has users: %v", up.Table)
	assert.True(t, len(up.Values) == 2, "%v", up)
}

func TestSqlCreate(t *testing.T) {
	t.Parallel()
	sql := `
	CREATE TABLE articles 
		 (
		  ID int(11) NOT NULL AUTO_INCREMENT,
		  Email char(150) NOT NULL DEFAULT '' COMMENT "email hello",
		  PRIMARY KEY (ID),
		  CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email) COMMENT "hello constraint"
		) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8
	WITH stuff = "hello";`
	req, err := rel.ParseSql(sql)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, req)
	cs, ok := req.(*rel.SqlCreate)
	assert.True(t, ok, "wanted SqlCreate got %T", req)
	assert.Equal(t, lex.TokenCreate, cs.Keyword(), "Has keyword CREATE")
	assert.Equal(t, "TABLE", cs.Tok.V, "Wanted TABLE: got %q", cs.Tok.V)
	assert.Equal(t, "articles", cs.Identity, "has articles: %v", cs.Identity)
	//assert.True(t, len(up.Values) == 2, "%v", up)
	assert.Equal(t, len(cs.Cols), 4, "Has 4 cols?")

	c2 := cs.Cols[1]
	assert.Equal(t, "email hello", c2.Comment, "%+v", c2)
	assert.Equal(t, "char", c2.DataType, "%+v", c2)
	assert.Equal(t, 150, c2.DataTypeSize, "%+v", c2)
}

func TestSqlDrop(t *testing.T) {
	t.Parallel()
	sql := `DROP TABLE articles;`
	req, err := rel.ParseSql(sql)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, req)
	ds, ok := req.(*rel.SqlDrop)
	assert.True(t, ok, "wanted SqlDrop got %T", req)
	assert.Equal(t, lex.TokenDrop, ds.Keyword(), "Has keyword DROP")
	assert.Equal(t, "TABLE", ds.Tok.V, "Wanted TABLE: got %q", ds.Tok.V)
	assert.Equal(t, "articles", ds.Identity, "has articles: %v", ds.Identity)
}

func TestWithNameValue(t *testing.T) {
	t.Parallel()
	// some sql dialects support a WITH name=value syntax
	sql := `
		SELECT id, name FROM user
		WITH key = "value", keyint = 45, keybool = true, keyfloat = 45.5
	`
	req, err := rel.ParseSql(sql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", sql, err)
	sel, ok := req.(*rel.SqlSelect)
	assert.True(t, ok, "is SqlSelect: %T", req)
	assert.True(t, len(sel.From) == 1 && sel.From[0].Name == "user", "has 1 from: %v", sel.From)
	assert.True(t, len(sel.With) == 4, "has 4 withs %v", sel.With)
	assert.True(t, sel.With["key"].(string) == "value", `want key = "value" but has %v`, sel.With["key"])
	assert.True(t, sel.With["keyint"].(int64) == 45, `want keyint = 45 but has %v`, sel.With["keyint"])
	assert.True(t, sel.With["keybool"].(bool) == true, `want keybool = true but has %v`, sel.With["keybool"])
	assert.True(t, sel.With["keyfloat"].(float64) == 45.5, `want keyfloat = 45.5 but has %v`, sel.With["keyfloat"])
}

func TestWithJson(t *testing.T) {
	t.Parallel()
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
	req, err := rel.ParseSql(sql)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, req)
	sel, ok := req.(*rel.SqlSelect)
	assert.True(t, ok)
	assert.Equal(t, 1, len(sel.From))
	assert.Equal(t, "user", sel.From[0].Name)
	assert.Equal(t, 8, len(sel.With))
	assert.Equal(t, 2, len(sel.With.Helper("keyobj")))
	u.Infof("sel.With:  \n%s", sel.With.PrettyJson())
}
