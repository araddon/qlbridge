package expr

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
)

var (
	_ = fmt.Sprint
	_ = u.EMPTY

	sqlStrings = []string{`/*
	DESCRIPTION
*/
SELECT 
    fname
    , lname AS last_name
    , count(host(_ses)) IF contains(_ses,"google.com")
    , now() AS created_ts
    , count(*) as ct
    , name   -- comment 
    , valuect(event) 
    , todate(reg_date)
    , todate(` + "`field xyz $%`" + `)
INTO table 
FROM mystream 
WHERE 
   ne(event,"stuff") AND ge(party, 1)
`, `/*
	  multi line comment
*/
	SELECT 
	    fname -- First Name
	    , lname AS last_name 
	    , count(_ses) IF contains(_ses,google.com)
	    , email
	    , set(cc)          AS choices 
	FROM mystream 
	WHERE 
	   ne(event,"stuff") AND ge(party, 1)
`,
		`SELECT 
			u.user_id, u.email, o.item_id,o.price
		FROM users AS u 
		INNER JOIN orders AS o 
		ON u.user_id = o.user_id;
	`}
)

func parseOrPanic(t *testing.T, query string) SqlStatement {
	stmt, err := ParseSql(query)
	if err != nil {
		t.Errorf("Parse failed: %s \n%s", query, err)
		t.FailNow()
	}
	return stmt
}

// We need to be able to re-write queries, as we during joins we have
// to re-write query that we are going to send to a single data source
func TestToSql(t *testing.T) {
	for _, sqlStrIn := range sqlStrings {
		u.Debug("parsing next one ", sqlStrIn)
		stmt1 := parseOrPanic(t, sqlStrIn)
		sqlSel1 := stmt1.(*SqlSelect)
		sqlRt := sqlSel1.String()
		u.Warnf("About to parse roundtrip \n%v", sqlRt)
		stmt2 := parseOrPanic(t, sqlRt)
		compareAst(t, stmt1, stmt2)
	}
}

func compareFroms(t *testing.T, fl, fr []*SqlSource) {
	assert.T(t, len(fl) == len(fr), "must have same froms")
	for i, f := range fl {
		compareFrom(t, f, fr[i])
	}
}

func compareFrom(t *testing.T, fl, fr *SqlSource) {
	assert.T(t, fl.Name == fr.Name)
	assert.Equal(t, fl.Op, fr.Op)
	assert.Equal(t, fl.Alias, fr.Alias)
	assert.Equal(t, fl.LeftOrRight, fr.LeftOrRight)
	assert.Equal(t, fl.JoinType, fr.JoinType)
	compareNode(t, fl.JoinExpr, fr.JoinExpr)
}

func compareAstColumn(t *testing.T, colLeft, colRight *Column) {
	//assert.Tf(t, colLeft.LongDesc == colRight.LongDesc, "Longdesc")

	assert.Tf(t, colLeft.As == colRight.As, "As: '%v' != '%v'", colLeft.As, colRight.As)

	assert.Tf(t, colLeft.Comment == colRight.Comment, "Comments?  '%s' '%s'", colLeft.Comment, colRight.Comment)

	compareNode(t, colLeft.Guard, colRight.Guard)
	compareNode(t, colLeft.Expr, colRight.Expr)

}

func compareAst(t *testing.T, in1, in2 SqlStatement) {
	switch s1 := in1.(type) {
	case *SqlSelect:
		s2, ok := in2.(*SqlSelect)
		assert.T(t, ok, "Must also be SqlSelect")
		u.Debugf("original:\n%s", s1.String())
		u.Debugf("after:\n%s", s2.String())
		//assert.T(t, s1.Alias == s2.Alias)
		//assert.T(t, len(s1.Columns) == len(s2.Columns))
		for i, c := range s1.Columns {
			compareAstColumn(t, c, s2.Columns[i])
		}
		//compareWhere(s1.Where)
		compareFroms(t, s1.From, s2.From)
	default:
		t.Fatalf("Must be SqlSelect")
	}
}

func compareNode(t *testing.T, n1, n2 Node) {
	if n1 == nil && n2 == nil {
		return
	}
	rv1, rv2 := reflect.ValueOf(n1), reflect.ValueOf(n2)
	assert.Tf(t, rv1.Kind() == rv2.Kind(), "kinds match: %T %T", n1, n2)
}

func TestSqlRewrite(t *testing.T) {
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
	sql := parseOrPanic(t, s).(*SqlSelect)
	err := sql.Finalize()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(sql.Columns) == 4, "has 4 cols: %v", len(sql.Columns))
	assert.Tf(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	// Test the Left/Right column level parsing
	//  TODO:   This field should not be u.name?   sourcefield should be name right? as = u.name?
	col, _ := sql.Columns.ByName("u.name")
	assert.Tf(t, col.As == "u.name", "col.As=%s", col.As)
	left, right, ok := col.LeftRight()
	//u.Debugf("left=%v  right=%v  ok%v", left, right, ok)
	assert.T(t, left == "u" && right == "name" && ok == true)

	rw1 := sql.From[0].Rewrite(sql)
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.Tf(t, rw1.String() == "SELECT u.name, u.email, user_id FROM users", "%v", rw1.String())

	rw1 = sql.From[1].Rewrite(sql)
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.Tf(t, rw1.String() == "SELECT o.item_id, o.price, user_id FROM orders", "%v", rw1.String())

	// Do we change?
	//assert.Equal(t, sql.Columns.FieldNames(), []string{"user_id", "email", "item_id", "price"})

	s = `SELECT u.name, u.email, b.title
			FROM users AS u INNER JOIN blog AS b 
			ON u.name = b.author;`
	sql = parseOrPanic(t, s).(*SqlSelect)
	assert.Tf(t, len(sql.Columns) == 3, "has 3 cols: %v", len(sql.Columns))
	assert.Tf(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))
	rw1 = sql.From[0].Rewrite(sql)
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 2, "has 2 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.Tf(t, rw1.String() == "SELECT u.name, u.email FROM users", "%v", rw1.String())
	jn := sql.From[0].JoinNodes()
	assert.Tf(t, len(jn) == 1, "%v", jn)
	assert.Tf(t, jn[0].String() == "name", "wanted 1 node %v", jn[0].String())
	cols := sql.From[0].UnAliasedColumns()
	assert.Tf(t, len(cols) == 2, "Should have 2: %#v", cols)
	//u.Infof("cols: %#v", cols)
	rw1 = sql.From[1].Rewrite(sql)
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 2, "has 2 cols: %v", rw1.Columns.String())
	// TODO:   verify that we can rewrite sql for aliases
	// jn, _ = sql.From[1].JoinValueExpr()
	// assert.Tf(t, jn.String() == "name", "%v", jn.String())
	// u.Infof("SQL?: '%v'", rw1.String())
	// assert.Tf(t, rw1.String() == "SELECT title, author as name FROM blog", "%v", rw1.String())

	s = `SELECT u.name, u.email, b.title
			FROM users AS u INNER JOIN blog AS b 
			ON tolower(u.author) = b.author;`
	sql = parseOrPanic(t, s).(*SqlSelect)
	sql.Rewrite()
	selu := sql.From[0].Source
	assert.Tf(t, len(selu.Columns) == 3, "user 3 cols: %v", selu.Columns.String())
	assert.Tf(t, selu.String() == "SELECT u.name, u.email, author FROM users", "%v", selu.String())
	jn = sql.From[0].JoinNodes()
	assert.Tf(t, len(jn) == 1, "wanted 1 node but got fromP: %p   %v", sql.From[0], jn)
	assert.Tf(t, jn[0].String() == "tolower(author)", "wanted 1 node %v", jn[0].String())
	cols = sql.From[0].UnAliasedColumns()
	assert.Tf(t, len(cols) == 3, "Should have 3: %#v", cols)

	// Now lets try compound join keys
	s = `SELECT u.name, u.email, b.title
			FROM users AS u INNER JOIN blog AS b 
			ON u.name = b.author and tolower(u.alias) = b.alias;`
	sql = parseOrPanic(t, s).(*SqlSelect)
	sql.Rewrite()
	assert.Tf(t, len(sql.Columns) == 3, "has 3 cols: %v", len(sql.Columns))
	assert.Tf(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))
	rw1 = sql.From[0].Source
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	//u.Infof("SQL?: '%v'", rw1.String())
	assert.Tf(t, rw1.String() == "SELECT u.name, u.email, alias FROM users", "%v", rw1.String())
	jn = sql.From[0].JoinNodes()
	assert.Tf(t, len(jn) == 2, "wanted 2 join nodes but %v", len(jn))
	assert.Tf(t, jn[0].String() == "name", `want "name" %v`, jn[0].String())
	assert.Tf(t, jn[1].String() == "tolower(alias)", `want "tolower(alias)" but got %q`, jn[1].String())
	cols = sql.From[0].UnAliasedColumns()
	assert.Tf(t, len(cols) == 3, "Should have 3: %#v", cols)
	//u.Infof("cols: %#v", cols)
	rw1 = sql.From[1].Source
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())

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
	sql = parseOrPanic(t, s).(*SqlSelect)
	assert.Tf(t, len(sql.Columns) == 3, "has 3 cols: %v", len(sql.Columns))
	assert.Tf(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	rw0 := sql.From[0].Rewrite(sql)
	rw1 = sql.From[1].Rewrite(sql)
	assert.Tf(t, rw0 != nil, "should not be nil:")
	assert.Tf(t, len(rw0.Columns) == 3, "has 3 cols: %v", rw0.String())
	assert.Tf(t, len(sql.From[0].Source.Columns) == 3, "has 3 cols? %s", sql.From[0].Source)
	assert.Tf(t, rw0.String() == "SELECT a.title, author, email FROM article WHERE email != NULL", "Wrong SQL 0: %v", rw0.String())
	assert.Tf(t, rw1 != nil, "should not be nil:")
	assert.Tf(t, len(rw1.Columns) == 3, "has 3 cols: %v", rw1.Columns.String())
	assert.Tf(t, len(sql.From[1].Source.Columns) == 3, "has 3 cols? %s", sql.From[1].Source)
	assert.Tf(t, rw1.String() == "SELECT p.actor, p.repository.name, follow_ct FROM github_push WHERE follow_ct > 20", "Wrong SQL 1: %v", rw1.String())

	// Original should still be the same
	parts := strings.Split(sql.String(), "\n")
	for _, p := range parts {
		u.Debugf("----%v----", p)
	}
	assert.Tf(t, parts[0] == `SELECT p.actor, p.repository.name, a.title FROM article AS a`, "Wrong Full SQL?: '%v'", parts[0])
	assert.Tf(t, parts[1] == `	INNER JOIN github_push AS p ON p.actor = a.author WHERE p.follow_ct > 20 AND a.email != NULL`, "Wrong Full SQL?: '%v'", parts[1])
	assert.Tf(t, sql.String() == `SELECT p.actor, p.repository.name, a.title FROM article AS a
	INNER JOIN github_push AS p ON p.actor = a.author WHERE p.follow_ct > 20 AND a.email != NULL`, "Wrong Full SQL?: '%v'", sql.String())

	s = `SELECT u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date FROM users AS u
	INNER JOIN (
				SELECT price, order_date, user_id from ORDERS
				WHERE user_id IS NOT NULL AND price > 10
			) AS o 
			ON u.user_id = o.user_id
	`
	sql = parseOrPanic(t, s).(*SqlSelect)
	assert.Tf(t, len(sql.Columns) == 6, "has 6 cols: %v", len(sql.Columns))
	assert.Tf(t, len(sql.From) == 2, "has 2 sources: %v", len(sql.From))

	// Original should still be the same
	// parts = strings.Split(sql.String(), "\n")
	// for _, p := range parts {
	// 	u.Debugf("----%v----", p)
	// }
	assert.Tf(t, sql.String() == `SELECT u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date FROM users AS u
	INNER JOIN (
		SELECT price, order_date, user_id FROM ORDERS WHERE user_id != NULL AND price > 10
	) AS o ON u.user_id = o.user_id`, "Wrong Full SQL?: '%v'", sql.String())

	//assert.Tf(t, sql.From[1].Name == "ORDERS", "orders?  %q", sql.From[1].Name)
	// sql.From[0].Rewrite(sql)
	// sql.From[1].Rewrite(sql)
	// assert.Tf(t, sql.From[0].Source.String() == `SELECT user_id, reg_date, email FROM users`, "Wrong Full SQL?: '%v'", sql.From[0].Source.String())
	// assert.Tf(t, sql.From[1].Source.String() == `SELECT item_id, price, order_date, user_id FROM ORDERS`, "Wrong Full SQL?: '%v'", sql.From[1].Source.String())

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

func TestDEV1(t *testing.T) {

}

func TestSqlFingerPrinting(t *testing.T) {
	// Fingerprinting allows the select statement to have a cached plan regardless
	//   of prepared statement
	sql1 := parseOrPanic(t, `SELECT name, item_id, email, price
			FROM users WHERE user_id = "12345"`).(*SqlSelect)
	sql2 := parseOrPanic(t, `select name, ITEM_ID, email, price
			FROM users WHERE user_id = "789456"`).(*SqlSelect)
	assert.Tf(t, sql1.FingerPrintID() == sql2.FingerPrintID(),
		"Has equal fingerprints\n%s\n%s", sql1.FingerPrint('?'), sql2.FingerPrint('?'))
}
