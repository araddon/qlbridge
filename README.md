QLBridge - Go SQL Runtime Engine
=====================================================

A SQL execution engine for embedded use as a library for SQL or SQL-Like functionality.
Hackable, add datasources ("Storage" can be rest apis, or anything), and add functions.  See usage in https://github.com/dataux/dataux
a federated Sql Engine mysql-compatible with backends (Elasticsearch, Google-Datastore, Mongo, Cassandra, Files).

[![Code Coverage](https://codecov.io/gh/araddon/qlbridge/branch/master/graph/badge.svg)](https://codecov.io/gh/araddon/qlbridge)
[![GoDoc](https://godoc.org/github.com/araddon/qlbridge?status.svg)](http://godoc.org/github.com/araddon/qlbridge)
[![Build Status](https://travis-ci.org/araddon/qlbridge.svg?branch=master)](https://travis-ci.org/araddon/qlbridge)
[![Go ReportCard](https://goreportcard.com/badge/araddon/qlbridge)](https://goreportcard.com/report/araddon/qlbridge)


### QLBridge Features and Goals
* expression engine for evaluation of single expressions
* execution of sql queries against your data, embedable, not coupled to storage layer
* extend VM with custom go functions, provide rich basic library of functions
* provide example backends (csv, elasticsearch, etc)

### Dialects
* SQL [see examples](https://github.com/araddon/qlbridge/blob/master/exec/exec_test.go)
* FilterQL (just Where clause) with more of a DSL for filter [see examples](https://github.com/araddon/qlbridge/blob/master/vm/filterqlvm_test.go#L75)
* Simple Expressions [see examples](https://github.com/araddon/qlbridge/blob/master/vm/vm_test.go#L59)

### Example of Expression Evaluation Engine

These expressions can be used stand-alone embedded usage in your app.  But, 
are the same expressions which might be columns, where, group-by clauses in SQL.
[see example](examples/expressions/main.go)
```go
func main() {

	// Add a custom function to the VM to make available to expression language
	expr.FuncAdd("email_is_valid", &EmailIsValid{})

	// This is the evaluation context which will be the data-source
	// to be evaluated against the expressions.  There is a very simple
	// interface you can use to create your own.
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

	// Example list of expressions
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
		u.Debugf("Output: %-35v T:%-15T expr:  %s", v, v, expression)
	}
}

// Example of a custom Function, that we are making available in the Expression VM
type EmailIsValid struct{}

func (m *EmailIsValid) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailIsValid(arg) but got %s", n)
	}
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		if args[0] == nil || args[0].Err() || args[0].Nil() {
			return value.BoolValueFalse, true
		}
		if _, err := mail.ParseAddress(args[0].ToString()); err == nil {
			return value.BoolValueTrue, true
		}

		return value.BoolValueFalse, true
	}, nil
}
func (m *EmailIsValid) Type() value.ValueType { return value.BoolType }


```
### Example SQL Runtime for Reading a Csv via Stdio, File

See example in [qlcsv](https://github.com/araddon/qlbridge/tree/master/examples/qlcsv)
folder for a CSV reader, parser, evaluation engine.

```sh

./qlcsv -sql 'select 
		user_id, email, item_count * 2, yy(reg_date) > 10 
	FROM stdin where email_is_valid(email);' < users.csv

```
```go

func main() {

	if sqlText == "" {
		u.Errorf("You must provide a valid select query in argument:    --sql=\"select ...\"")
		return
	}

	// load all of our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", &EmailIsValid{})

	// We are registering the "csv" datasource, to show that
	// the backend/sources can be easily created/added.  This csv
	// reader is an example datasource that is very, very simple.
	exit := make(chan bool)
	src, _ := datasource.NewCsvSource("stdin", 0, bytes.NewReader([]byte("##")), exit)
	schema.RegisterSourceAsSchema("example_csv", src)

	db, err := sql.Open("qlbridge", "example_csv")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(sqlText)
	if err != nil {
		u.Errorf("could not execute query: %v", err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	fmt.Printf("\n\nScanning through CSV: (%v)\n\n", strings.Join(cols, ","))
	for rows.Next() {
		rows.Scan(readCols...)
		fmt.Println(strings.Join(writeCols, ", "))
	}
	fmt.Println("")
}

// Example of a custom Function, that we are adding into the Expression VM
//
//         select
//              user_id AS theuserid, email, item_count * 2, reg_date
//         FROM stdin
//         WHERE email_is_valid(email)
type EmailIsValid struct{}

func (m *EmailIsValid) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailIsValid(arg) but got %s", n)
	}
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		if args[0] == nil || args[0].Err() || args[0].Nil() {
			return value.BoolValueFalse, true
		}
		if _, err := mail.ParseAddress(args[0].ToString()); err == nil {
			return value.BoolValueTrue, true
		}

		return value.BoolValueFalse, true
	}, nil
}
func (m *EmailIsValid) Type() value.ValueType { return value.BoolType }

```

[x]QL languages are making a comeback.   It is still an easy, approachable
way of working with data.   Also, we see more and more ql's that are xql'ish but
un-apologetically non-standard.  This matches our observation that
data is stored in more and more formats in more tools, services that aren't
traditional db's but querying that data should still be easy.  Examples
[Influx](http://influxdb.com/docs/v0.8/api/query_language.html), 
[GitQL](https://github.com/cloudson/gitql), 
[Presto](http://prestosql.io/), 
[Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select), 
[CQL](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html),
[yql](https://developer.yahoo.com/yql/),
[ql.io](http://ql.io/), etc


Projects that access non-sql data via [x]ql
----------------------------------------------------
* http://prestosql.io/
* https://crate.io/docs/current/sql/index.html
* http://senseidb.com/
* http://influxdb.com/docs/v0.8/api/query_language.html
* https://github.com/crosbymichael/dockersql
* http://harelba.github.io/q/
* https://github.com/dinedal/textql
* https://github.com/cloudson/gitql
* https://github.com/brendandburns/ksql 

Go Script/VM interpreters
---------------------------------------
* https://github.com/robpike/ivy
* https://github.com/yuin/gopher-lua
* https://github.com/SteelSeries/golisp
* [Complete List](https://github.com/golang/go/wiki/Projects#virtual-machines-and-languages)

