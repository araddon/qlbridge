package rel_test

import (
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

func parseFeatures(t testing.TB, f *schema.DataSourceFeatures, q string) *rel.SqlSelect {
	stmt, err := rel.ParseSqlSelect(q)
	assert.Equal(t, nil, err, "expected no error but got %v for %s", err, q)
	assert.NotEqual(t, nil, stmt)
	err = stmt.Rewrite()
	assert.Equal(t, nil, err)
	return stmt
}
func parse(t testing.TB, q string) *rel.SqlSelect {
	return parseFeatures(t, schema.FeaturesDefault(), q)
}

func TestSqlSelectReWrite(t *testing.T) {
	ss := parse(t, "SELECT user_id FROM users WHERE (`users.user_id` != NULL)")
	assert.Equal(t, 1, len(ss.From[0].Source.Columns))
	ss = parse(t, `select exists(email), email FROM users WHERE yy(reg_date) > 10;`)
	assert.Equal(t, 2, len(ss.From[0].Source.Columns))
}

func TestSqlRewriteTemp(t *testing.T) {

	s := `SELECT u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date FROM users AS u
	INNER JOIN (
				SELECT price, order_date, user_id from ORDERS
				WHERE user_id IS NOT NULL AND price > 10
			) AS o 
			ON u.user_id = o.user_id
	`
	sql := parseOrPanic(t, s).(*rel.SqlSelect)
	assert.True(t, len(sql.Columns) == 6, "has 6 cols: %v", len(sql.Columns))
	assert.True(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	assert.True(t, sql.String() == `SELECT u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date FROM users AS u
	INNER JOIN (
		SELECT price, order_date, user_id FROM ORDERS WHERE user_id != NULL AND price > 10
	) AS o ON u.user_id = o.user_id`, "Wrong Full SQL?: '%v'", sql.String())
}

func TestSqlRewrite(t *testing.T) {
	t.Parallel()
	/*
		SQL Re-writing is to take select statement with multiple sources (joins, sub-select)
		and rewrite these sub-statements/sources into standalone statements
		and prepare the column name, index mapping

		- Do we want to send the columns fully aliased?   ie
			SELECT name AS u.name, email as u.email, user_id as u.user_id FROM users
	*/
	s := `SELECT u.name, o.item_id, u.email, o.price
			FROM users AS u INNER JOIN orders AS o 
			ON u.user_id = o.user_id;`
	sql := parseOrPanic(t, s).(*rel.SqlSelect)
	err := sql.Finalize()
	assert.True(t, err == nil, "no error: %v", err)
	assert.True(t, len(sql.Columns) == 4, "has 4 cols: %v", len(sql.Columns))
	assert.True(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	// Test the Left/Right column level parsing
	//  TODO:   This field should not be u.name?   sourcefield should be name right? as = u.name?
	col, _ := sql.Columns.ByName("u.name")
	assert.True(t, col.As == "u.name", "col.As=%s", col.As)
	left, right, ok := col.LeftRight()
	//u.Debugf("left=%v  right=%v  ok%v", left, right, ok)
	assert.True(t, left == "u" && right == "name" && ok == true)

	rw1, _ := sql.From[0].Rewrite(sql)
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.Equal(t, rw1.String(), "SELECT name, email, user_id FROM users", "%v", rw1.String())

	rw1, _ = sql.From[1].Rewrite(sql)
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.True(t, rw1.String() == "SELECT item_id, price, user_id FROM orders", "%v", rw1.String())

	// Do we change?
	//assert.Equal(t, sql.Columns.FieldNames(), []string{"user_id", "email", "item_id", "price"})

	s = `SELECT u.name, u.email, b.title
			FROM users AS u INNER JOIN blog AS b 
			ON u.name = b.author;`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	assert.True(t, len(sql.Columns) == 3, "has 3 cols: %v", len(sql.Columns))
	assert.True(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))
	rw1, _ = sql.From[0].Rewrite(sql)
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 2, "has 2 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.True(t, rw1.String() == "SELECT name, email FROM users", "%v", rw1.String())
	jn := sql.From[0].JoinNodes()
	assert.True(t, len(jn) == 1, "%v", jn)
	assert.True(t, jn[0].String() == "name", "wanted 1 node %v", jn[0].String())
	cols := sql.From[0].UnAliasedColumns()
	assert.True(t, len(cols) == 2, "Should have 2: %#v", cols)
	//u.Infof("cols: %#v", cols)
	rw1, _ = sql.From[1].Rewrite(sql)
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 2, "has 2 cols: %v", rw1.Columns.String())
	// TODO:   verify that we can rewrite sql for aliases
	// jn, _ = sql.From[1].JoinValueExpr()
	// assert.True(t, jn.String() == "name", "%v", jn.String())
	// u.Infof("SQL?: '%v'", rw1.String())
	// assert.True(t, rw1.String() == "SELECT title, author as name FROM blog", "%v", rw1.String())

	s = `SELECT u.name, u.email, b.title
			FROM users AS u INNER JOIN blog AS b 
			ON tolower(u.author) = b.author;`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	sql.Rewrite()
	selu := sql.From[0].Source
	assert.True(t, len(selu.Columns) == 3, "user 3 cols: %v", selu.Columns.String())
	assert.True(t, selu.String() == "SELECT name, email, author FROM users", "%v", selu.String())
	jn = sql.From[0].JoinNodes()
	assert.True(t, len(jn) == 1, "wanted 1 node but got fromP: %p   %v", sql.From[0], jn)
	assert.True(t, jn[0].String() == "tolower(author)", "wanted 1 node %v", jn[0].String())
	cols = sql.From[0].UnAliasedColumns()
	assert.True(t, len(cols) == 3, "Should have 3: %#v", cols)

	// Now lets try compound join keys
	s = `SELECT u.name, u.email, b.title
			FROM users AS u INNER JOIN blog AS b 
			ON u.name = b.author and tolower(u.alias) = b.alias;`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	sql.Rewrite()
	assert.True(t, len(sql.Columns) == 3, "has 3 cols: %v", len(sql.Columns))
	assert.True(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))
	rw1 = sql.From[0].Source
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.True(t, rw1.String() == "SELECT name, email, alias FROM users", "%v", rw1.String())
	jn = sql.From[0].JoinNodes()
	assert.True(t, len(jn) == 2, "wanted 2 join nodes but %v", len(jn))
	assert.True(t, jn[0].String() == "name", `want "name" %v`, jn[0].String())
	assert.True(t, jn[1].String() == "tolower(alias)", `want "tolower(alias)" but got %q`, jn[1].String())
	cols = sql.From[0].UnAliasedColumns()
	assert.True(t, len(cols) == 3, "Should have 3: %#v", cols)
	//u.Infof("cols: %#v", cols)
	rw1 = sql.From[1].Source
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())

	// This test, is looking at these aspects of rewrite
	//  1 the dotted notation of 'repostory.name' ensuring we have removed the p.
	//  2 where clause
	s = `
		SELECT 
			p.actor, p.repository.name, a.title
		FROM article AS a 
		INNER JOIN github_push AS p 
			ON p.actor = a.author
		WHERE p.follow_ct > 20 AND a.email IS NOT NULL
	`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	assert.True(t, len(sql.Columns) == 3, "has 3 cols: %v", len(sql.Columns))
	assert.True(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	rw0, _ := sql.From[0].Rewrite(sql)
	rw1, _ = sql.From[1].Rewrite(sql)
	assert.True(t, rw0 != nil, "should not be nil:")
	assert.True(t, len(rw0.Columns) == 3, "has 3 cols: %v", rw0.String())
	assert.True(t, len(sql.From[0].Source.Columns) == 3, "has 3 cols? %s", sql.From[0].Source)
	assert.True(t, rw0.String() == "SELECT title, author, email FROM article WHERE email != NULL", "Wrong SQL 0: %v", rw0.String())
	assert.True(t, rw1 != nil, "should not be nil:")
	assert.True(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	assert.True(t, len(sql.From[1].Source.Columns) == 3, "has 3 cols? %s", sql.From[1].Source)
	assert.True(t, rw1.String() == "SELECT actor, `repository.name`, follow_ct FROM github_push WHERE follow_ct > 20", "Wrong SQL 1: %v", rw1.String())

	// Original should still be the same
	parts := strings.Split(sql.String(), "\n")
	for _, p := range parts {
		u.Debugf("----%v----", p)
	}
	assert.True(t, parts[0] == "SELECT p.actor, p.`repository.name`, a.title FROM article AS a", "Wrong Full SQL?: '%v'", parts[0])
	assert.True(t, parts[1] == `	INNER JOIN github_push AS p ON p.actor = a.author WHERE p.follow_ct > 20 AND a.email != NULL`, "Wrong Full SQL?: '%v'", parts[1])
	assert.True(t, sql.String() == `SELECT p.actor, p.`+"`repository.name`"+`, a.title FROM article AS a
	INNER JOIN github_push AS p ON p.actor = a.author WHERE p.follow_ct > 20 AND a.email != NULL`, "Wrong Full SQL?: '%v'", sql.String())

	s = `SELECT u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date FROM users AS u
	INNER JOIN (
				SELECT price, order_date, user_id from ORDERS
				WHERE user_id IS NOT NULL AND price > 10
			) AS o 
			ON u.user_id = o.user_id
	`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	assert.True(t, len(sql.Columns) == 6, "has 6 cols: %v", len(sql.Columns))
	assert.True(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	assert.True(t, sql.String() == `SELECT u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date FROM users AS u
	INNER JOIN (
		SELECT price, order_date, user_id FROM ORDERS WHERE user_id != NULL AND price > 10
	) AS o ON u.user_id = o.user_id`, "Wrong Full SQL?: '%v'", sql.String())

	// Rewrite to remove functions, and aliasing to send all fields needed down to source
	// used when we are going to poly-fill
	s = `SELECT count AS ct, name as nm, todate(myfield) AS mydate FROM user`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	sql.RewriteAsRawSelect()
	assert.True(t, sql.String() == `SELECT count, name, myfield FROM user`, "Wrong rewrite SQL?: '%v'", sql.String())

	// Now ensure a group by, and where columns
	s = `SELECT name as nm, todate(myfield) AS mydate FROM user WHERE created > todate("2016-01-01") GROUP BY referral;`
	sql = parseOrPanic(t, s).(*rel.SqlSelect)
	sql.RewriteAsRawSelect()
	assert.True(t, sql.String() == `SELECT name, myfield, referral, created FROM user WHERE created > todate("2016-01-01") GROUP BY referral`, "Wrong rewrite SQL?: '%v'", sql.String())

	//assert.True(t, sql.From[1].Name == "ORDERS", "orders?  %q", sql.From[1].Name)
	// sql.From[0].Rewrite(sql)
	// sql.From[1].Rewrite(sql)
	// assert.True(t, sql.From[0].Source.String() == `SELECT user_id, reg_date, email FROM users`, "Wrong Full SQL?: '%v'", sql.From[0].Source.String())
	// assert.True(t, sql.From[1].Source.String() == `SELECT item_id, price, order_date, user_id FROM ORDERS`, "Wrong Full SQL?: '%v'", sql.From[1].Source.String())

	// 	s = `SELECT  aa.*,
	// 			        bb.meal
	// 			FROM table1 aa
	// 				INNER JOIN table2 bb
	// 				    ON aa.tableseat = bb.tableseat AND
	// 				        aa.weddingtable = bb.weddingtable
	// 				INNER JOIN
	// 				(
	// 					SELECT  a.tableSeat
	// 					FROM    table1 a
	// 					        INNER JOIN table2 b
	// 					            ON a.tableseat = b.tableseat AND
	// 					                a.weddingtable = b.weddingtable
	// 					WHERE b.meal IN ('chicken', 'steak')
	// 					GROUP by a.tableSeat
	// 					HAVING COUNT(DISTINCT b.Meal) = 2
	// 				) c ON aa.tableseat = c.tableSeat
	// `
}
