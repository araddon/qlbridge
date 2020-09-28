package expr_test

import (
	"flag"
	"os"
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
)

const (
	noError  = true
	hasError = false
)

func TestMain(m *testing.M) {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
	builtins.LoadAllBuiltins()
	if t := os.Getenv("trace"); t != "" {
		expr.Trace = true
	}
	// Now run the actual Tests
	os.Exit(m.Run())
}

type State struct{}

type numberTest struct {
	text    string
	isInt   bool
	isFloat bool
	int64
	uint64
	float64
}

var numberTests = []numberTest{
	// basics
	{"0", true, true, 0, 0, 0},
	{"73", true, true, 73, 73, 73},
	{"073", true, true, 073, 073, 073},
	{"0x73", true, true, 0x73, 0x73, 0x73},
	{"100", true, true, 100, 100, 100},
	{"1e9", true, true, 1e9, 1e9, 1e9},
	{"1e19", false, true, 0, 1e19, 1e19},
	// funny bases
	{"0123", true, true, 0123, 0123, 0123},
	{"0xdeadbeef", true, true, 0xdeadbeef, 0xdeadbeef, 0xdeadbeef},
	// some broken syntax
	{text: "+-2"},
	{text: "0x123."},
	{text: "1e."},
	{text: "'x"},
	{text: "'xx'"},
}

func TestNumberParse(t *testing.T) {
	t.Parallel()
	for _, test := range numberTests {
		n, err := expr.NewNumberStr(test.text)
		ok := test.isInt || test.isFloat
		if ok && err != nil {
			t.Errorf("unexpected error for %q: %s", test.text, err)
			continue
		}
		if !ok && err == nil {
			t.Errorf("expected error for %q", test.text)
			continue
		}
		if !ok {
			if *VerboseTests {
				u.Debugf("%s\n\t%s", test.text, err)
			}
			continue
		}
		if test.isInt && !n.IsInt {
			t.Errorf("did not expect unsigned integer for %q", test.text)
		}
		if test.isFloat {
			if !n.IsFloat {
				t.Errorf("expected float for %q", test.text)
			}
			if n.Float64 != test.float64 {
				t.Errorf("float64 for %q should be %g Is %g", test.text, test.float64, n.Float64)
			}
		} else if n.IsFloat {
			t.Errorf("did not expect float for %q", test.text)
		}
	}
}

type exprTest struct {
	qlText string
	result string
	ok     bool
}

var exprTestsx = []exprTest{
	{
		`email IN ["hello"]`,
		`email IN ["hello"]`,
		true,
	},
}

var exprTests = []exprTest{
	{
		"`content table`.`Ford Motor Company` >= \"0.58\"",
		"`content table`.`Ford Motor Company` >= \"0.58\"",
		true,
	},
	{
		"content.`Ford Motor Company` >= \"0.58\"",
		"content.`Ford Motor Company` >= \"0.58\"",
		true,
	},
	{
		`AND ( EXISTS x, EXISTS y)`,
		`AND ( EXISTS x, EXISTS y )`,
		true,
	},
	{
		`AND ( EXISTS x, INCLUDE ref_name )`,
		`AND ( EXISTS x, INCLUDE ref_name )`,
		true,
	},
	{
		`AND ( EXISTS x, INCLUDE ref_name, x == "y" AND ( EXISTS x, EXISTS y ) )`,
		`AND ( EXISTS x, INCLUDE ref_name, x == "y", AND ( EXISTS x, EXISTS y ) )`,
		true,
	},
	// Testing a non binary AND with paren
	{
		`x = "y" AND ( EXISTS a OR EXISTS b)`,
		`x = "y" AND (EXISTS a OR EXISTS b)`,
		true,
	},
	{
		"NOT `fieldname` INTERSECTS (\"hello\")",
		"NOT (`fieldname` INTERSECTS (\"hello\"))",
		true,
	},
	{
		`company = "Toys R"" Us"`,
		`company = "Toys R"" Us"`,
		true,
	},
	{
		`NOT INCLUDE name`,
		`NOT INCLUDE name`,
		true,
	},
	{
		`eq(event,"stuff") OR ge(party, 1)`,
		`eq(event, "stuff") OR ge(party, 1)`,
		true,
	},
	{
		`eq(event,"stuff") OR (ge(party, 1) AND true)`,
		`eq(event, "stuff") OR (ge(party, 1) AND true)`,
		true,
	},
	{
		`eq(event,"stuff") AND ge(party, 1)`,
		`eq(event, "stuff") AND ge(party, 1)`,
		true,
	},
	{
		`eq(event,"stuff") OR ge(party, 1)`,
		`eq(event, "stuff") OR ge(party, 1)`,
		true,
	},
	{
		`item * 5`,
		`item * 5`,
		true,
	},
	{
		`eq(toint(item),5)`,
		`eq(toint(item), 5)`,
		true,
	},
	{
		`eq(5,5)`,
		`eq(5, 5)`,
		true,
	},
	{
		`eq((1+1),2)`,
		`eq((1 + 1), 2)`,
		true,
	},
	{
		`oneof("1",item,4)`,
		`oneof("1", item, 4)`,
		true,
	},
	{
		`toint("1")`,
		`toint("1")`,
		true,
	},
	{
		`item IN "value1"`,
		`item IN "value1"`,
		true,
	},
	{
		`item NOT IN "value2"`,
		`NOT (item IN "value2")`,
		true,
	},
	{
		`NOT item IN "value3"`,
		`NOT (item IN "value3")`,
		true,
	},
	{
		`NOT 10 IN "value4"`,
		`NOT (10 IN "value4")`,
		true,
	},
	{
		`"value5" IN ident`,
		`"value5" IN ident`,
		true,
	},
	{
		`NOT (email IN ("hello"))`,
		`NOT (email IN ("hello"))`,
		true,
	},
	{
		`email IN ["hello"]`,
		`email IN ["hello"]`,
		true,
	},
	{
		`1 IN ident`, `1 IN ident`,
		true,
	},
	{
		"`tablename` LIKE \"%\"",
		"`tablename` LIKE \"%\"",
		true,
	},
	{
		"`content table`.`Ford Motor Company` >= \"0.58\"",
		"`content table`.`Ford Motor Company` >= \"0.58\"",
		true,
	},
	{
		"`content.Ford Motor Company` >= \"0.58\"",
		"`content.Ford Motor Company` >= \"0.58\"",
		true,
	},
	{
		`"value" IN hosts(@@content_whitelist_domains)`,
		"\"value\" IN hosts(@@content_whitelist_domains)",
		true,
	},
	// Complex nested statements
	{
		`and (
                not(
                    or (event IN ("rq", "ab") , product IN ("my", "app"))
                )
            )`,
		`not(or ( event IN ("rq", "ab"), product IN ("my", "app") ))`,
		true,
	},
	{
		`
		NOT(exists(@@content_whitelist_domains))
		OR len(@@content_whitelist_domains) == 0 
		`,
		`NOT(exists(@@content_whitelist_domains)) OR len(@@content_whitelist_domains) == 0`,
		true,
	},
	{
		`
		version == 4
		AND (
			NOT(exists(@@content_whitelist_domains))
			OR len(@@content_whitelist_domains) == 0
			OR host(url) IN hosts(@@content_whitelist_domains)
		)`,
		`version == 4 AND (NOT(exists(@@content_whitelist_domains)) OR len(@@content_whitelist_domains) == 0 OR host(url) IN hosts(@@content_whitelist_domains))`,
		true,
	},
	// Invalid Statements
	{
		"`fieldname` INTERSECTS \"hello\"", // Right Side only allows (identity|array|func)
		"",
		false,
	},
	{
		"`fieldname` INTERSECTS false", // Right Side only allows (identity|array|func)
		"",
		false,
	},
	// Try a bunch of code simplification
	{
		`OR (x == "y")`,
		`x == "y"`,
		true,
	},
	{
		`NOT OR (x == "y")`,
		`NOT (x == "y")`,
		true,
	},
	{
		`NOT AND (x == "y")`,
		`NOT (x == "y")`,
		true,
	},
	{
		`AND (x == "y" , AND ( stuff == x ))`,
		`AND ( x == "y", stuff == x )`,
		true,
	},
}

func TestParseExpressions(t *testing.T) {
	t.Parallel()
	for _, test := range exprTests {
		u.Infof("parsing %s", test.qlText)
		exprNode, err := expr.ParseExpression(test.qlText)
		u.Infof("After Parse:  %v", err)
		switch {
		case err == nil && !test.ok:
			t.Errorf("%q: 1 expected error; got none", test.qlText)
			continue
		case err != nil && test.ok:
			t.Errorf("%q: 2 unexpected error: %v", test.qlText, err)
			continue
		case err != nil && !test.ok:
			// expected error, got one
			if *VerboseTests {
				u.Infof("%s: %s\n\t%s", test.qlText, test.qlText, err)
			}
			continue
		}
		var result string
		result = exprNode.String()
		if result != test.result {
			//t.Errorf("reslen: %v vs %v", len(result), len(test.result))
			t.Errorf("\nGot     :\t%v\nExpected:\t%v", result, test.result)
			u.Warnf("%#v", exprNode)
		}
	}
}
