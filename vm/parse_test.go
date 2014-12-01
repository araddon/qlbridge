package vm

import (
	"flag"
	u "github.com/araddon/gou"
	"testing"
)

var (
	VerboseTests *bool = flag.Bool("vv", false, "Verbose Logging?")
)

func init() {
	flag.Parse()
	if *VerboseTests {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
}

type numberTest struct {
	text    string
	isInt   bool
	isUint  bool
	isFloat bool
	int64
	uint64
	float64
}

var numberTests = []numberTest{
	// basics
	{"0", true, true, true, 0, 0, 0},
	{"73", true, true, true, 73, 73, 73},
	{"073", true, true, true, 073, 073, 073},
	{"0x73", true, true, true, 0x73, 0x73, 0x73},
	{"100", true, true, true, 100, 100, 100},
	{"1e9", true, true, true, 1e9, 1e9, 1e9},
	{"1e19", false, true, true, 0, 1e19, 1e19},
	// funny bases
	{"0123", true, true, true, 0123, 0123, 0123},
	{"0xdeadbeef", true, true, true, 0xdeadbeef, 0xdeadbeef, 0xdeadbeef},
	// some broken syntax
	{text: "+-2"},
	{text: "0x123."},
	{text: "1e."},
	{text: "'x"},
	{text: "'xx'"},
}

func TestNumberParse(t *testing.T) {
	for _, test := range numberTests {
		n, err := NewNumber(0, test.text)
		ok := test.isInt || test.isUint || test.isFloat
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
				u.Infof("%s\n\t%s", test.text, err)
			}
			continue
		}
		if test.isUint {
			if !n.IsInt {
				t.Errorf("expected unsigned integer for %q", test.text)
			}
			if n.Int64 != test.int64 {
				t.Errorf("uint64 for %q should be %d Is %d", test.text, test.uint64, n.Int64)
			}
		} else if n.IsInt {
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

const (
	noError  = true
	hasError = false
)

var parseTests = []parseTest{
	{"general parse test", `eq(toint(item),5)`, noError, `eq(toint(item), 5)`},
}

func TestParseQls(t *testing.T) {

	for _, test := range parseTests {
		exprTree, err := ParseTree(test.qlText)
		u.Infof("After Parse:  %v", err)
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
			t.Errorf("\n%s -- (%v): \n\t%v\nexpected\n\t%v", test.name, test.qlText, result, test.result)
		}
	}
}
