package expr

import (
	"fmt"
	"reflect"
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
		sqlRt := sqlSel1.StringAST()
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
		u.Debugf("original:\n%s", s1.StringAST())
		u.Debugf("after:\n%s", s2.StringAST())
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
	s := `SELECT u.user_id, u.email, o.item_id,o.price
			FROM users AS u INNER JOIN orders AS o 
			ON u.user_id = o.user_id;`
	sql := parseOrPanic(t, s).(*SqlSelect)
	assert.Tf(t, len(sql.Columns) == 4, "has 4 cols: %v", len(sql.Columns))
	// Do we change?
	//assert.Equal(t, sql.Columns.FieldNames(), []string{"user_id", "email", "item_id", "price"})
}
