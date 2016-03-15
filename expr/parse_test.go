package expr_test

import (
	"flag"
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

func init() {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}

	builtins.LoadAllBuiltins()
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

type parseTest struct {
	name   string
	qlText string
	ok     bool
	result string // ?? what is this?
}

var parseTestsx = []parseTest{
	{"general parse test", `item * 5`, noError, `item * 5`},
}

var parseTests = []parseTest{
	{"mv in", `eq(event,"stuff") OR ge(party, 1)`, noError, `eq(event, "stuff") OR ge(party, 1)`},
	{"compound where", `eq(event,"stuff") OR (ge(party, 1) AND true)`, noError, `eq(event, "stuff") OR (ge(party, 1) AND true)`},
	{"compound where", `eq(event,"stuff") AND ge(party, 1)`, noError, `eq(event, "stuff") AND ge(party, 1)`},
	{"compound where", `eq(event,"stuff") OR ge(party, 1)`, noError, `eq(event, "stuff") OR ge(party, 1)`},
	{"general parse test", `item * 5`, noError, `item * 5`},
	{"general parse test", `eq(toint(item),5)`, noError, `eq(toint(item), 5)`},
	{"general parse test", `eq(5,5)`, noError, `eq(5, 5)`},
	{"general parse test", `oneof("1",item,4)`, noError, `oneof("1", item, 4)`},
	{"general parse test", `toint("1")`, noError, `toint("1")`},
	{"in ident", `"value" IN ident`, noError, `"value" IN ident`},
	{"in ident", `1 IN ident`, noError, `1 IN ident`},
	{"general parse test", "`tablename` LIKE '%'", noError, "`tablename` LIKE '%'"},
	{"general parse test", `"value" IN hosts(@@content_whitelist_domains)`, noError, "\"value\" IN hosts(`@@content_whitelist_domains`)"},
}

func TestParseExpressions(t *testing.T) {
	t.Parallel()
	for _, test := range parseTests {
		exprTree, err := expr.ParseExpression(test.qlText)
		//u.Infof("After Parse:  %v", err)
		switch {
		case err == nil && !test.ok:
			t.Errorf("%q: 1 expected error; got none", test.name)
			continue
		case err != nil && test.ok:
			t.Errorf("%q: 2 unexpected error: %v", test.name, err)
			continue
		case err != nil && !test.ok:
			// expected error, got one
			if *VerboseTests {
				u.Infof("%s: %s\n\t%s", test.name, test.qlText, err)
			}
			continue
		}
		var result string
		result = exprTree.Root.String()
		if result != test.result {
			t.Errorf("reslen: %v vs %v", len(result), len(test.result))
			t.Errorf("\n%s \n\t'%v'\nexpected\n\t'%v'", test.name, result, test.result)
		}
	}
}
