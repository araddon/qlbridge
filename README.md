QL - a Go Lexer/Parser/VM
====================================================

This is a [x]QL generic lexer parser, and expression VM to evaluate SQL ish 
queries.


### QL Features and Goals
* execution of sql queries against your data, embedded
* extend VM with custom go functions, provide rich basic library of functions
* provide example backends (csv, elasticsearch, etc)

### Example VM Runtime for Reading a Csv via Stdio, and evaluating


See example in [ql](https://github.com/araddon/qlbridge/tree/master/ql)
folder for a CSV reader, parser, evaluation engine.


```go

//   ./ql -sql "select user_id, email, item_count * 2, yy(reg_date) > 10 FROM stdio" < users.csv

func main() {

	flag.StringVar(&sqlText, "sql", "", "SQL query such as [select user_id, yy(reg_date) from stdio];")
	flag.Parse()

	msgChan := make(chan url.Values, 100)
	quit := make(chan bool)
	go CsvProducer(msgChan, quit)

	go singleExprEvaluation(msgChan)

	<-quit
}

func sqlEvaluation(msgChan chan url.Values) {

	// A write context holds the results of evaluation
	// could be a storage layer, in this case a simple map
	writeContext := vm.NewContextSimple()

	// our parsed sql, and runtime to evaluate sql
	exprVm, err := vm.NewSqlVm(sqlText)
	if err != nil {
		return
	}
	for msg := range msgChan {
		readContext := vm.NewContextUrlValues(msg)
		err := exprVm.Execute(writeContext, readContext)
		if err != nil {
			fmt.Println(err)
			return
		} 
		fmt.Printall(printall(writeContext.All()))
	}
}

func CsvProducer(msgChan chan url.Values, quit chan bool) {
	defer func() {
		quit <- true
	}()
	csvr := csv.NewReader(os.Stdin)
	headers, _ := csvr.Read()
	for {
		row, err := csvr.Read()
		if err != nil && err == io.EOF {
			return
		}

		v := make(url.Values)

		// If values exist for desired indexes, set them.
		for idx, fieldName := range headers {
			if idx <= len(row)-1 {
				v.Set(fieldName, strings.TrimSpace(row[idx]))
			}
		}

		msgChan <- v
	}
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
* https://github.com/wandoulabs/codis
* https://github.com/youtube/vitess

Inspiration/Other works
--------------------------
* https://github.com/mattn/anko
* https://github.com/pubsubsql/pubsubsql
* https://github.com/linkedin/databus


### Creating a custom Lexer/Parser

See example in `exampledialect` folder for a custom ql dialect, this
example creates a mythical *SUBSCRIBETO* query language...
```go
// Tokens Specific to our PUBSUB
var TokenSubscribeTo ql.TokenType = 1000

// Custom lexer for our maybe hash function
func LexMaybe(l *ql.Lexer) ql.StateFn {

	l.SkipWhiteSpaces()

	keyWord := strings.ToLower(l.PeekWord())

	switch keyWord {
	case "maybe":
		l.ConsumeWord("maybe")
		l.Emit(ql.TokenIdentity)
		return ql.LexExpressionOrIdentity
	}
	return ql.LexExpressionOrIdentity
}

func main() {

	// We are going to inject new tokens into qlbridge
	ql.TokenNameMap[TokenSubscribeTo] = &ql.TokenInfo{Description: "subscribeto"}

	// OverRide the Identity Characters in qlbridge to allow a dash in identity
	ql.IDENTITY_CHARS = "_./-"

	ql.LoadTokenInfo()
	ourDialect.Init()

	// We are going to create our own Dialect that uses a "SUBSCRIBETO" keyword
	pubsub = &ql.Statement{TokenSubscribeTo, []*ql.Clause{
		{Token: TokenSubscribeTo, Lexer: ql.LexColumns},
		{Token: ql.TokenFrom, Lexer: LexMaybe},
		{Token: ql.TokenWhere, Lexer: ql.LexColumns, Optional: true},
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


