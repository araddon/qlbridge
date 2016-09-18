QLBridge - a Go SQL Runtime Engine
====================================================

A SQL execution engine for embedded use as a library for Sql OR sql-Like functionality.
Hackable, add datasources, functions.  See usage in https://github.com/dataux/dataux
a federated Sql Engine mysql-compatible with backends (Elasticsearch, Google-Datastore, Mongo, Cassandra, Files).


### QLBridge Features and Goals
* execution of sql queries against your data, embedable, not coupled to storage layer
* extend VM with custom go functions, provide rich basic library of functions
* provide example backends (csv, elasticsearch, etc)

### Dialects
* Sql [see examples](https://github.com/araddon/qlbridge/blob/master/exec/exec_test.go)
* FilterQL (just Where clause) with more of a DSL for filter [see examples](https://github.com/araddon/qlbridge/blob/master/vm/filterqlvm_test.go#L75)
* Simple Expressions [see examples](https://github.com/araddon/qlbridge/blob/master/vm/vm_test.go#L59)

### Example of Evaluation Expression Engine

[see example](https://github.com/araddon/qlbridge/blob/master/examples/expressions)
```go
func main() {

	// Add a custom function to the VM to make available to expression language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// This is the evaluation context which will serve be the data-source
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


```
### Example SQL Runtime for Reading a Csv via Stdio, File

See example in [qlcsv](https://github.com/araddon/qlbridge/tree/master/examples/qlcsv)
folder for a CSV reader, parser, evaluation engine.

```sh

./qlcsv -sql 'select 
		user_id, email, item_count * 2, yy(reg_date) > 10 
	FROM stdio where email_is_valid(email);' < users.csv

```
```go

func main() {

	// load the libray of pre-built functions for usage in sql queries
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	// showing lexer/parser accepts it
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// Datasources are easy to write and can be added
	datasource.Register("csv", &datasource.CsvDataSource{})

	// now from here down is standard go database/sql query handling
	db, err := sql.Open("qlbridge", "csv:///dev/stdin")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(sqlText)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}

	for rows.Next() {
		rows.Scan(readCols...)
		fmt.Println(strings.Join(writeCols, ", "))
	}
}

// Example of a custom Function, that we are adding into the Expression VM
//
//         select
//              user_id AS theuserid, email, item_count * 2, reg_date
//         FROM stdio
//         WHERE email_is_valid(email)
func EmailIsValid(ctx vm.EvalContext, email value.Value) (value.BoolValue, bool) {
	emailstr, ok := value.ToString(email.Rv())
	if !ok || emailstr == "" {
		return value.BoolValueFalse, true
	}
	if _, err := mail.ParseAddress(emailstr); err == nil {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}


```

[x]QL languages are making a comeback.   It is still an easy, approachable
way of working with data.   Also, we see more and more ql's that are xql'ish but
un-apologetically non-standard.  This matches our observation that
data is stored in more and more formats in more tools, services that aren't
traditional db's but querying that data should still be easy.  Examples
[Influx](http://influxdb.com/docs/v0.8/api/query_language.html), 
[GitQL](https://github.com/cloudson/gitql), 
[Presto](http://prestodb.io/), 
[Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select), 
[CQL](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html),
[yql](https://developer.yahoo.com/yql/),
[ql.io](http://ql.io/), etc


Projects that access non-sql data via [x]ql
----------------------------------------------------
* http://prestodb.io/
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

