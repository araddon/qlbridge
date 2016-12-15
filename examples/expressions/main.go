package main

import (
	"log"
	"net/mail"
	"os"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

func init() {
	u.SetLogger(log.New(os.Stderr, "", 0), "debug")
	u.SetColorOutput()

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
}

func main() {

	// Add a custom function to the VM to make available to expression language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// This is the evaluation context which will be evaluated against the expressions
	evalContext := datasource.NewContextSimpleNative(map[string]interface{}{
		"int5":     5,
		"str5":     "5",
		"created":  dateparse.MustParse("12/18/2015"),
		"bvalt":    true,
		"bvalf":    false,
		"user_id":  "abc",
		"urls":     []string{"http://google.com", "http://nytimes.com"},
		"hits":     map[string]int64{"google.com": 5, "bing.com": 1},
		"email":    "bob@bob.com",
		"emailbad": "bob",
		"mt": map[string]time.Time{
			"event0": dateparse.MustParse("12/18/2015"),
			"event1": dateparse.MustParse("12/22/2015"),
		},
	})

	exprs := []string{
		"int5 == 5",
		`6 > 5`,
		`6 > 5.5`,
		`(4 + 5) / 2`,
		`6 == (5 + 1)`,
		`2 * (3 + 5)`,
		`todate("12/12/2012")`,
		`created > "now-1M"`, // Date math
		`created > "now-10y"`,
		`user_id == "abc"`,
		`email_is_valid(email)`,
		`email_is_valid(emailbad)`,
		`email_is_valid("not_an_email")`,
		`EXISTS int5`,
		`!exists(user_id)`,
		`mt.event0 > now()`, // step into child of maps
		`["portland"] LIKE "*land"`,
		`email contains "bob"`,
		`email NOT contains "bob"`,
		`[1,2,3] contains int5`,
		`[1,2,3,5] NOT contains int5`,
		`urls contains "http://google.com"`,
		`split("chicago,portland",",") LIKE "*land"`,
		`10 BETWEEN 1 AND 50`,
		`15.5 BETWEEN 1 AND "55.5"`,
		`created BETWEEN "now-50w" AND "12/18/2020"`,
		`toint(not_a_field) NOT IN ("a","b" 4.5)`,
		`
		OR (
			email != "bob@bob.com"
			AND (
				NOT EXISTS not_a_field
				int5 == 5 
			)
		)`,
	}

	for _, expression := range exprs {
		// Same ast can be re-used safely concurrently
		exprAst := expr.MustParse(expression)
		// Evaluate AST in the vm
		val, _ := vm.Eval(evalContext, exprAst)
		v := val.Value()
		u.Debugf("%46s  ==> %-35v T:%-15T ", expression, v, v)
	}
}

// Example of a custom Function, that we are adding into the Expression VM
//
func EmailIsValid(ctx expr.EvalContext, email value.Value) (value.BoolValue, bool) {
	if email.Err() || email.Nil() {
		return value.BoolValueFalse, true
	}
	if _, err := mail.ParseAddress(email.ToString()); err == nil {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}
