QLBridge - a Go SQL Runtime Engine
====================================================

A SQL execution engine for embedded use as a library for Sql OR sql-Like functionality.
Hackable, add datasources, functions.


### QLBridge Features and Goals
* execution of sql queries against your data, embedable, not coupled to storage layer
* extend VM with custom go functions, provide rich basic library of functions
* provide example backends (csv, elasticsearch, etc)

### Dialects
* Sql [see examples](https://github.com/araddon/qlbridge/blob/master/exec/exec_test.go)
* FilterQL (just Where clause) with more of a DSL for filter [see examples](https://github.com/araddon/qlbridge/blob/master/vm/filterqlvm_test.go#L75)
* Create Your Own [see influxql example](https://github.com/araddon/qlbridge/blob/master/dialects/influxql)

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

