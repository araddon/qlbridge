QLBridge - a Go SQL Runtime Engine
====================================================

This is a SQL execution engine to process data with sql for embedded use, 
includes a native go lexer, parser.  Extend with native go functions.


### QLBridge Features and Goals
* execution of sql queries against your data, embedable, not coupled to storage layer
* extend VM with custom go functions, provide rich basic library of functions
* provide example backends (csv, elasticsearch, etc)

### Example VM Runtime for Reading a Csv via Stdio, and evaluating


See example in [qlcsv](https://github.com/araddon/qlbridge/tree/master/examples/qlcsv)
folder for a CSV reader, parser, evaluation engine.

```sh

./qlcsv -sql 'select 
		user_id, email, item_count * 2, yy(reg_date) > 10 
	FROM stdio' < users.csv

```
```go

func main() {

	// load all of our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// We are registering the "csv" datasource 
	datasource.Register("csv", &datasource.CsvDataSource{})

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

Projects that value-add at proxy
--------------------------------------------------
* https://github.com/Netflix/dynomite (multiple key/value)
* https://github.com/wandoulabs/codis  (redis)
* https://github.com/youtube/vitess     (mysql)
* https://github.com/twitter/twemproxy  (memcached)
* https://github.com/siddontang/mixer  (mysql)
* https://github.com/couchbaselabs/query (sql layer over k/v)

Inspiration/Other works
--------------------------
* https://github.com/pubsubsql/pubsubsql
* https://github.com/linkedin/databus

Go Script/VM interpreters
---------------------------------------
* https://github.com/robpike/ivy
* https://github.com/mattn/anko
* https://github.com/influxdb/influxdb/tree/master/influxql
* https://github.com/SteelSeries/golisp
* https://github.com/couchbaselabs/query

### Creating a custom Lexer/Parser

See example in `exampledialect` folder for a custom ql dialect, this
example creates a mythical *SUBSCRIBETO* query language...
```go
// Tokens Specific to our PUBSUB
var TokenSubscribeTo lex.TokenType = 1000

// Custom lexer for our maybe hash function
func LexMaybe(l *ql.Lexer) ql.StateFn {

	l.SkipWhiteSpaces()

	keyWord := strings.ToLower(l.PeekWord())

	switch keyWord {
	case "maybe":
		l.ConsumeWord("maybe")
		l.Emit(lex.TokenIdentity)
		return ql.LexExpressionOrIdentity
	}
	return ql.LexExpressionOrIdentity
}

func main() {

	// We are going to inject new tokens into qlbridge
	lex.TokenNameMap[TokenSubscribeTo] = &lex.TokenInfo{Description: "subscribeto"}

	// OverRide the Identity Characters in qlbridge to allow a dash in identity
	ql.IDENTITY_CHARS = "_./-"

	ql.LoadTokenInfo()
	ourDialect.Init()

	// We are going to create our own Dialect that uses a "SUBSCRIBETO" keyword
	pubsub = &ql.Statement{TokenSubscribeTo, []*ql.Clause{
		{Token: TokenSubscribeTo, Lexer: ql.LexColumns},
		{Token: lex.TokenFrom, Lexer: LexMaybe},
		{Token: lex.TokenWhere, Lexer: ql.LexColumns, Optional: true},
	}}
	ourDialect = &ql.Dialect{
		"Subscribe To", []*ql.Statement{pubsub},
	}

	l := ql.NewLexer(`
			SUBSCRIBETO
				count(x), Name
			FROM ourstream
			WHERE 
				k = REPLACE(LOWER(Name),'cde','xxx');
		`, ourDialect)

}

```


