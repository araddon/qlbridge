package rel_test

import (
	"fmt"
	"reflect"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = fmt.Sprint
	_ = u.EMPTY

	sqlStrings = []string{`
SELECT
    email IF NOT (email IN ("hello"))
INTO table FROM mystream
`, `/*
	DESCRIPTION
*/
SELECT
    fname
    , lname AS last_name
    , count(host(_ses)) IF contains(_ses,"google.com")
    , now() AS created_ts
    , count(*) as ct
    , name   -- comment 
    , email IF email NOT IN ("hello")
    , email as email2 IF NOT EXISTS reg_date
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

func parseOrPanic(t *testing.T, query string) rel.SqlStatement {
	stmt, err := rel.ParseSql(query)
	if err != nil {
		t.Errorf("Parse failed: %s \n%s", query, err)
		t.FailNow()
	}
	return stmt
}

// We need to be able to re-write queries, as we during joins we have
// to re-write query that we are going to send to a single data source
func TestToSql(t *testing.T) {
	t.Parallel()
	for _, sqlStrIn := range sqlStrings {
		u.Debug("parsing next one ", sqlStrIn)
		stmt1 := parseOrPanic(t, sqlStrIn)
		sqlSel1 := stmt1.(*rel.SqlSelect)
		sqlRt := sqlSel1.String()
		u.Warnf("About to parse roundtrip \n%v", sqlRt)
		stmt2 := parseOrPanic(t, sqlRt)
		compareAst(t, stmt1, stmt2)
		//comparePb(t, stmt1, stmt2)
	}
}

/*
func comparePb(t *testing.T, sl, sr SqlStatement) {
	lb, err := sl.ToPB()
	assert.True(t, err == nil, "Should not error on ToBP but got err=%v for %s", err, sl)
	rb, err := sr.ToPB()
	assert.True(t, err == nil, "Should not error on ToBP but got err=%v for %s", err, sr)
	assert.True(t, len(lb) > 0 && len(rb) > 0, "should have bytes output")
}
*/

func compareFroms(t *testing.T, fl, fr []*rel.SqlSource) {
	assert.True(t, len(fl) == len(fr), "must have same froms")
	for i, f := range fl {
		compareFrom(t, f, fr[i])
	}
}

func compareFrom(t *testing.T, fl, fr *rel.SqlSource) {
	assert.True(t, fl.Name == fr.Name)
	assert.Equal(t, fl.Op, fr.Op)
	assert.Equal(t, fl.Alias, fr.Alias)
	assert.Equal(t, fl.LeftOrRight, fr.LeftOrRight)
	assert.Equal(t, fl.JoinType, fr.JoinType)
	compareNode(t, fl.JoinExpr, fr.JoinExpr)
}

func compareAstColumn(t *testing.T, colLeft, colRight *rel.Column) {
	assert.True(t, colLeft.As == colRight.As, "As: '%v' != '%v'", colLeft.As, colRight.As)
	assert.True(t, colLeft.Comment == colRight.Comment, "Comments?  '%s' '%s'", colLeft.Comment, colRight.Comment)
	compareNode(t, colLeft.Guard, colRight.Guard)
	compareNode(t, colLeft.Expr, colRight.Expr)
}

func compareAst(t *testing.T, in1, in2 rel.SqlStatement) {
	switch s1 := in1.(type) {
	case *rel.SqlSelect:
		s2, ok := in2.(*rel.SqlSelect)
		assert.True(t, ok, "Must also be SqlSelect")
		u.Debugf("original:\n%s", s1.String())
		u.Debugf("after:\n%s", s2.String())
		//assert.True(t, s1.Alias == s2.Alias)
		//assert.True(t, len(s1.Columns) == len(s2.Columns))
		for i, c := range s1.Columns {
			compareAstColumn(t, c, s2.Columns[i])
		}
		//compareWhere(s1.Where)
		compareFroms(t, s1.From, s2.From)
	default:
		t.Fatalf("Must be SqlSelect")
	}
}

func compareNode(t *testing.T, n1, n2 expr.Node) {
	if n1 == nil && n2 == nil {
		return
	}
	rv1, rv2 := reflect.ValueOf(n1), reflect.ValueOf(n2)
	assert.True(t, rv1.Kind() == rv2.Kind(), "kinds match: %T %T", n1, n2)
}

func TestSqlFingerPrinting(t *testing.T) {
	t.Parallel()
	// Fingerprinting allows the select statement to have a cached plan regardless
	//   of prepared statement
	sql1 := parseOrPanic(t, `SELECT name, item_id, email, price
			FROM users WHERE user_id = "12345"`).(*rel.SqlSelect)
	sql2 := parseOrPanic(t, `select name, ITEM_ID, email, price
			FROM users WHERE user_id = "789456"`).(*rel.SqlSelect)
	fw1 := expr.NewFingerPrinter()
	fw2 := expr.NewFingerPrinter()
	sql1.WriteDialect(fw1)
	sql2.WriteDialect(fw2)
	assert.Equal(t, fw1.String(), fw2.String())
	assert.Equal(t, sql1.FingerPrintID(), sql2.FingerPrintID(), "Should have equal fingerprints")
}
