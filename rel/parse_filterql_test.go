package rel_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var FilterTests = []string{
	`FILTER "bob@gmail.com" IN ("hello","world")`,
	`FILTER "bob@gmail.com" NOT IN ("hello","world")`,
	`FILTER "bob@gmail.com" IN identityname`,
	`FILTER email CONTAINS "gmail.com"`,
	`FILTER NOT INCLUDE ffe5817811c2270aa5d4aff2d9eafed3`,
	`FILTER AND ( NOT news INTERSECTS ("a"), domains intersects ("b"))`,
	`FILTER email INTERSECTS ("a", "b")`,
	`FILTER email NOT INTERSECTS ("a", "b")`,
	"FILTER EXISTS email ALIAS `Has Spaces Alias`",
	`FILTER AND ( NOT INCLUDE abcd, (lastvisit_ts > "now-1M") ) FROM user`,
	`FILTER COMPANY IN ("Toys R"" Us", "Toys R' Us, Inc.")`,
	`FILTER *`,
	`
	FILTER AND (
        a IN ("Analyst")
        b IN ("C-Level Other")
        c IN ("Management")
    )
    FROM x
    ALIAS abc
    `,
	`
		FILTER score > 0
		WITH
			name = "My Little Pony",
			public = false,
			kind = "aspect"
		ALIAS with_attributes
	`,
	`
		FILTER OR ( 
			AND (
				score NOT BETWEEN 5 and 10, 
				email NOT IN ("abc") 
			),
			NOT date > "now-3d"
		)`,
	`
		FILTER AND ( EXISTS user_id, NOT OR ( user_id like "a", user_id like "b", user_id like "c", user_id like "d", user_id like "e", user_id like "f" ) )
	`,
	`
		FILTER OR ( AND ( our_names like "2. has spaces", our_names like "1. has more spa'ces" ), INCLUDE 'f9f0dc74234af7e86ddeb660c50350e1' )
	`,
	`
		FILTER  AND ( NOT INCLUDE '791734b084019d99c82a475264464304', 
			NOT INCLUDE 'd750a11e72b58778e302eb0893788680', NOT INCLUDE '61a624e5ca4153645ddc9e6ebaee8000' )
		`,
	`FILTER AND ( visitct >= "1", NOT INCLUDE 3d4240482815b9848caf2e6f )`,
	`
		FILTER AND ( 
			AND (
				score NOT BETWEEN 5 and 10, 
				email NOT IN ("abc") 
			),
			x > 7
		)`,
	`FILTER AND ( visitct >= "1", INCLUDE 3d4240482815b9848caf2e6f )`,
	`FILTER x > 7`,
	`FILTER AND ( NOT EXISTS email, email NOT IN ("abc") )`,
	`FILTER AND ( score NOT BETWEEN 5 and 10, email NOT IN ("abc") )`,
	`
		FILTER
			AND (
				NAME != NULL
				, tostring(fieldname) == "hello"
			)

			LIMIT 100
	`,
	`
      -- this function tests a LOT of comments
      FILTER
        -- and this expression
        AND (  -- and even here which makes no sense
          NAME != NULL   -- ensures name not nill
          , tostring(fieldname) == "hello"  -- also that fieldname == hello
        ) -- again
        -- and our limit is 100
        LIMIT 100
        -- and some more
    `,
}

func init() {
	lex.IDENTITY_CHARS = lex.IDENTITY_SQL_CHARS
	if t := os.Getenv("trace"); t != "" {
		expr.Trace = true
	}
}

func parseFilterQlTest(t *testing.T, ql string) {

	u.Debugf("before: %s", ql)
	req, err := rel.ParseFilterQL(ql)
	//u.Debugf("parse filter %#v  %s", req, ql)
	assert.True(t, err == nil && req != nil, "Must parse: %s  \n\t%v", ql, err)
	u.Debugf("after:  %s", req.String())
	req2, err := rel.ParseFilterQL(req.String())
	assert.True(t, err == nil, "must parse roundtrip %v for %s", err, ql)
	req.Raw = ""
	req2.Raw = ""
	assert.True(t, req.Equal(req2), "must roundtrip")

	ast := req.Filter.Expr()
	by, err := json.Marshal(ast)
	//u.Debugf("ast %s", string(by))
	assert.Equal(t, nil, err, "Should not error %v", err)

	ast2 := &expr.Expr{}
	err = json.Unmarshal(by, ast2)
	assert.Equal(t, nil, err)
	n, err := expr.NodeFromExpr(ast2)
	assert.Equal(t, nil, err)
	req2.Where = n
	assert.True(t, req.Equal(req2), "must roundtrip expr/ast")
	//u.Debugf("after2 %s", req.String())
}

func parseFilterSelectTest(t *testing.T, ql string) {

	u.Debugf("parse filter select: %s", ql)
	sel, err := rel.ParseFilterSelect(ql)
	//u.Debugf("parse filter %#v  %s", sel, ql)
	assert.True(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	sel2, err := rel.ParseFilterSelect(sel.String())
	assert.True(t, err == nil, "must parse roundtrip %v --\n%s", err, sel.String())
	assert.True(t, sel2 != nil, "Must parse but didnt")
	// sel.Raw = ""
	// sel2.Raw = ""
	// u.Debugf("after:  %s", sel2.String())
	// assert.Equal(t, sel, sel2, "must roundtrip")
}

type selsTest struct {
	query  string
	expect int
}

func parseFilterSelectsTest(t *testing.T, st selsTest) {

	u.Debugf("parse filter select: %v", st)
	sels, err := rel.NewFilterParser(st.query).ParseFilterSelects()
	assert.True(t, err == nil, "Must parse: %s  \n\t%v", st.query, err)
	assert.True(t, len(sels) == st.expect, "Expected %d filters got %v", st.expect, len(sels))
	for _, sel := range sels {
		sel2, err := rel.ParseFilterSelect(sel.String())
		assert.True(t, err == nil, "must parse roundtrip %v --\n%s", err, sel.String())
		assert.True(t, sel2 != nil, "Must parse but didnt")
	}
}

type foo struct{}

func (*foo) Type() value.ValueType { return value.BoolType }
func (*foo) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		return value.NewBoolValue(true), true
	}, nil
}

func TestFuncResolver(t *testing.T) {
	t.Parallel()

	funcs := expr.NewFuncRegistry()
	funcs.Add("foo", &foo{})

	fs, err := rel.NewFilterParserfuncs(`SELECT foo() FROM name FILTER foo()`, funcs).
		ParseFilter()
	assert.True(t, err == nil, "err:%v", err)
	assert.True(t, len(fs.Columns) == 1)

	funcs2 := expr.NewFuncRegistry()
	_, err2 := rel.NewFilterParserfuncs(`SELECT foo() FROM name FILTER foo()`, funcs2).
		ParseFilter()

	assert.True(t, err2 != nil)
	assert.True(t, strings.Contains(err2.Error(), "non existent function foo"), "err:%v", err2)
}

func TestFilterErrMsg(t *testing.T) {
	t.Parallel()

	_, err := rel.ParseFilterQL("FILTER * FROM user ALIAS ALIAS stuff")
	assert.NotEqual(t, err, nil, "Should have errored")
	assert.True(t, strings.Contains(err.Error(), "Line 1"), err)
}

func TestFilterNewLines(t *testing.T) {
	t.Parallel()
	_, err := rel.ParseFilterQL(`
	FILTER AND (
        a IN ("Analyst")
        b IN ("C-Level Other")
        c IN ("Management")
    )
    FROM x
    ALIAS abc
    `)
	assert.Equal(t, nil, err)
}

func TestFilterQlRoundTrip(t *testing.T) {
	t.Parallel()
	for _, fql := range FilterTests {
		parseFilterQlTest(t, fql)
	}
}

func TestFilterQlFingerPrint(t *testing.T) {
	t.Parallel()

	req1, _ := rel.ParseFilterQL(`FILTER visit_ct > 74`)
	req2, _ := rel.ParseFilterQL(`FILTER visit_ct > 101`)
	assert.True(t, req1.FingerPrintID() == req2.FingerPrintID())

	wrongCt := 0
	for i := 0; i < 1000; i++ {
		fs, err := rel.ParseFilterSelect(`SELECT * FROM user.changes FILTER OR ( entered("abc123"), exited("abc123") ) WITH backfill=true, track_deltas = true;`)
		if err != nil {
			t.Fatalf("Must not have parse error %v", err)
		}
		if int64(72361533482220960) != fs.FingerPrintID() {
			wrongCt++
		}
	}
	assert.Equal(t, 0, wrongCt, "Expected 0 wrong got %v", wrongCt)
}

func TestFilterSelectParse(t *testing.T) {
	t.Parallel()
	parseFilterSelectTest(t, `SELECT a, b, domain(url) FROM name FILTER email NOT INTERSECTS ("a", "b") WITH x="y";`)

	parseFilterSelectsTest(t, selsTest{`
		SELECT a, b, domain(url) FROM name FILTER email NOT INTERSECTS ("a", "b") WITH x="y";
		SELECT a, b, domain(url) FROM name FILTER email NOT INTERSECTS ("a", "b") WITH x="y";
	`, 2})

	ql := `
    SELECT *
    FROM users
    WHERE
      domain(url) == "google.com"
      OR momentum > 20
    ALIAS my_filter_name
	`
	sel, err := rel.ParseFilterSelect(ql)
	assert.True(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.True(t, len(sel.Columns) == 1, "Wanted 1 col got : %v", len(sel.Columns))
	assert.True(t, sel.Alias == "my_filter_name", "has alias: %q", sel.Alias)
	assert.NotEqual(t, nil, sel.Where, "Should have Where expr ", sel.Where)
	assert.Equal(t, sel.Where.String(), `domain(url) == "google.com" OR momentum > 20`, "%v", sel.Where)

	ql = `
    SELECT a, b, *
    FROM users
    FILTER AND (
      domain(url) == "google.com"
      momentum > 20
     )
    ALIAS my_filter_name
	`
	sel, err = rel.ParseFilterSelect(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, sel, nil, ql)
	assert.True(t, len(sel.Columns) == 3, "Wanted 3 col's got : %v", len(sel.Columns))
	assert.True(t, sel.Alias == "my_filter_name", "has alias: %q", sel.Alias)
	assert.Equal(t, sel.Filter.String(), `AND ( domain(url) == "google.com", momentum > 20 )`, "%v", sel.Filter)

	ql = `
    SELECT a, b, *
    FROM users
    FILTER  domain(url) == "google.com"
    WITH aname = "b", bname = 2
    ALIAS my_filter_name
	`
	sel, err = rel.ParseFilterSelect(ql)
	assert.True(t, err == nil && sel != nil, "Must parse: %s  \n\t%v", ql, err)
	assert.True(t, len(sel.With) == 2, "Wanted 3 withs's got : %v", sel.With)
}

func TestFilterQLAstCheck(t *testing.T) {
	t.Parallel()
	ql := `
		FILTER 
			AND (
				NAME != NULL, 
				tostring(fieldname) == "hello",
			)

		LIMIT 100
	`
	req, err := rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	f, _ := req.Filter.(*expr.BooleanNode)
	assert.Equal(t, len(f.Args), 2, "expected 2 child filters got:%d for %s", len(f.Args), req.Filter.String())
	f1 := f.Args[0]
	assert.NotEqual(t, f1, nil)
	assert.Equal(t, f1.String(), "NAME != NULL", "%v", f1)
	assert.Equal(t, req.Limit, 100, "wanted limit=100: %v", req.Limit)

	// This should get re-written in simplest form as
	//    FILTER NAME != "bob"
	ql = `FILTER NOT AND ( name == "bob" ) ALIAS root`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	un := req.Filter.(*expr.UnaryNode)
	bn := un.Arg.(*expr.BinaryNode)
	u.Warnf("t %T", req.Filter)
	assert.Equal(t, len(bn.Args), 2, "has binary expression: %#v", f)
	assert.Equal(t, bn.String(), `(name == "bob")`, "Should have expr %v", bn)
	assert.Equal(t, req.String(), `FILTER NOT (name == "bob") ALIAS root`, "roundtrip? %v", req.String())

	ql = `FILTER OR ( INCLUDE child_1, INCLUDE child_2 ) ALIAS root`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	f, _ = req.Filter.(*expr.BooleanNode)
	assert.Equal(t, len(f.Args), 2, "has 2 filter expr: %#v", f)
	assert.Equal(t, f.Operator.T, lex.TokenLogicOr, "must have or op %v", f.Operator)
	f1 = f.Args[1]
	assert.Equal(t, f1.String(), `INCLUDE child_2`, "Should have include %q", f1.String())

	ql = `FILTER NOT AND ( name == "bob", OR ( NOT INCLUDE filter_xyz , NOT exists abc ) ) ALIAS root`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	f, _ = req.Filter.(*expr.BooleanNode)
	assert.Equal(t, len(f.Args), 2, "has 2 filter expr: %#v", f)
	assert.Equal(t, f.Negated(), true, "must negate")
	fc := f.Args[1].(*expr.BooleanNode)
	assert.Equal(t, fc.Operator.T, lex.TokenLogicOr, "is or %#v", fc.Operator)
	f2 := fc.Args[0].(expr.NegateableNode)
	assert.Equal(t, f2.Negated(), true)
	assert.Equal(t, f2.String(), `NOT INCLUDE filter_xyz`, "Should have include %v", f2)
	//assert.True(t, req.String() == ql, "roundtrip? %v", req.String())

	ql = `
    FILTER
      AND (
          -- Lets make sure the date is good
          daysago(datefield) < 100
          -- as well as domain
          , domain(url) == "google.com"
          , INCLUDE my_other_named_filter
          , OR (
              momentum > 20
             , propensity > 50
             , INCLUDE nested_filter
          )
          , NOT AND ( score > 20 , score < 50 )
       )
    ALIAS my_filter_name
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equal(t, req.Alias, "my_filter_name", "has alias: %q", req.Alias)
	u.Info(req.String())
	f = req.Filter.(*expr.BooleanNode)
	assert.Equal(t, len(f.Args), 5, "expected 5 filters: %#v", f)
	f5 := f.Args[4].(*expr.BooleanNode)
	assert.True(t, f5.Negated(), "expr negated? %s", f5.String())
	assert.Equal(t, len(f5.Args), 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")
	assert.Equal(t, len(req.Includes()), 2, "has 2 includes: %v", req.Includes())
	//assert.Equal(t, f5.Expr.NodeType(), UnaryNodeType, "%s != %s", f5.Expr.NodeType(), UnaryNodeType)

	ql = `
    FILTER
      AND (
          -- Lets make sure the date is good
          daysago(datefield) < 100
          -- as well as domain
          domain(url) == "google.com"
          INCLUDE my_other_named_filter
          OR (
              momentum > 20
             propensity > 50
          )
          NOT AND ( score > 20 , score < 50 )
       )
    ALIAS my_filter_name
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equal(t, req.Alias, "my_filter_name", "has alias: %q", req.Alias)
	//u.Info(req.String())
	f = req.Filter.(*expr.BooleanNode)
	assert.Equal(t, len(f.Args), 5, "expected 5 filters: %#v", f)
	f5 = f.Args[4].(*expr.BooleanNode)
	assert.True(t, f5.Negated(), "expr negated? %s", f5.String())
	assert.Equal(t, len(f5.Args), 2, "expr? %s", f5.String())
	assert.Equal(t, f5.String(), "NOT AND ( score > 20, score < 50 )")

	ql = `FILTER AND (
				INCLUDE child_1, 
				INCLUDE child_2
			) ALIAS root`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equal(t, req.Alias, "root", "has alias: %q", req.Alias)
	f = req.Filter.(*expr.BooleanNode)
	for _, f := range f.Args {
		in := f.(*expr.IncludeNode)
		assert.True(t, in.Identity.Text != "", "has include filter %q", in.String())
	}
	assert.Equal(t, len(f.Args), 2, "want 2 filter expr: %d", len(f.Args))

	ql = `FILTER NOT INCLUDE child_1 ALIAS root`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equal(t, req.Alias, "root", "has alias: %q", req.Alias)
	incn := req.Filter.(*expr.IncludeNode)
	//assert.True(t, len(f.Args) == 1, "has 1 filter expr: %#v", f)
	assert.True(t, incn.Negated(), "must negate %s", req.String())
	assert.Equal(t, incn.Identity.Text, "child_1")
	//fInc := cf.Filter.Filters[0]
	//assert.True(t, fInc.Include != "", "Should have include")

	ql = `
		FILTER *
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	idn := req.Filter.(*expr.IdentityNode)
	assert.Equal(t, idn.Text, "*")

	ql = `
		FILTER match_all
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	idn = req.Filter.(*expr.IdentityNode)
	assert.Equal(t, idn.Text, "match_all")

	ql = `
    FILTER
      AND (
          EXISTS datefield
       )
	FROM user
    ALIAS my_filter_name
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	assert.Equal(t, req.Alias, "my_filter_name", "has alias: %q", req.Alias)
	assert.True(t, req.From == "user", "has FROM: %q", req.From)
	un = req.Filter.(*expr.UnaryNode)
	assert.Equal(t, un.String(), "EXISTS datefield", "%#v", un)

	ql = `
    FILTER AND ( NOT news INTERSECTS ("a"), domains intersects ("b"))
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	bon := req.Filter.(*expr.BooleanNode)
	assert.Equal(t, 2, len(bon.Args), "has 2 args")
	lh := bon.Args[0]
	u.Debugf("lh %T  %s  %#v", lh, lh, lh)
	assert.Equal(t, bon.String(), "AND ( NOT (news INTERSECTS (\"a\")), domains intersects (\"b\") )", "%#v", bon)

	// Make sure we have a HasDateMath flag
	ql = `
		FILTER created > "now-3d"
	`
	req, err = rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	//bn := req.Filter.(*expr.BinaryNode)
	//assert.Equal(t, req.HasDateMath, true, "Must recognize datemath")
}

func TestFilterQL1(t *testing.T) {
	t.Parallel()
	ql := `
    FILTER AND ( NOT news INTERSECTS ("a"), domains intersects ("b"))
	`
	req, err := rel.ParseFilterQL(ql)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, req, nil, ql, err)
	bon := req.Filter.(*expr.BooleanNode)
	assert.Equal(t, 2, len(bon.Args), "has 2 args")
	lh := bon.Args[0]
	u.Debugf("lh %T  %s  %#v", lh, lh, lh)
	assert.Equal(t, bon.String(), "AND ( NOT (news INTERSECTS (\"a\")), domains intersects (\"b\") )", "%#v", bon)
}

func TestFilterQLInvalidCheck(t *testing.T) {
	t.Parallel()
	// This is invalid note the extra paren
	ql := `
		FILTER OR (_uid == "bob", email IN ("steve@steve.com")))
		ALIAS entity_basic_test
	`
	_, err := rel.ParseFilterQL(ql)
	assert.NotEqual(t, err, nil)
}

func TestFilterQLKeywords(t *testing.T) {
	t.Parallel()
	ql := `
	  -- Test comment 1
		FILTER 
		  -- Test comment 2
			AND (
				created < "now-24h",
				deleted == false
			)
		FROM accounts
		LIMIT 100
		ALIAS new_accounts
	`
	fs, err := rel.ParseFilterQL(ql)
	assert.Equal(t, nil, err)
	assert.Equal(t, " Test comment 1", fs.Description)
	assert.Equal(t, "accounts", fs.From)
	assert.Equal(t, 100, fs.Limit)
	assert.Equal(t, "new_accounts", fs.Alias)
}
