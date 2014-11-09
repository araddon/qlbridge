QL - a Go Lexer/Parser
====================================================

This is a [x]QL generic lexer parser, that should be useful
for constructing *Dialect* Specific Parsers.  

[x]QL languages are making a comeback.   It is still an easy, approachable
way of interrogating data easily.   Also, we see more and more ql's that are not standard
but still approachable such as Influx, GitQl, Presto, Hive, CQL etc.   


See example in `exampledialect` folder for a custom ql dialect
```go
var (
	// Tokens Specific to our PUBSUB
	TokenSubscribeTo ql.TokenType = 1000
	// We are going to create our own Dialect Right now
	// that uses a "SUBSCRIBETO" keyword
	pubsub = &ql.Statement{TokenSubscribeTo, []*ql.Clause{
		{Token: TokenSubscribeTo, Lexer: ql.LexColumns},
		{Token: ql.TokenFrom, Lexer: LexMaybe},
		{Token: ql.TokenWhere, Lexer: ql.LexColumns, Optional: true},
	}}
	ourDialect = &ql.Dialect{
		"Subscribe To", []*ql.Statement{pubsub},
	}
)
func init() {
	// We are going to inject new tokens into QLparse
	ql.TokenNameMap[TokenSubscribeTo] = &ql.TokenInfo{Description: "subscribeto"}
	// OverRide the Identity Characters in QLparse to allow a dash in identity
	ql.IDENTITY_CHARS = "_./-"
	ql.LoadTokenInfo()
	ourDialect.Init()
}

// Custom lexer for our maybe hash function
//
//  SUBSCRIBE
//       valuect(item) AS stuff
//  FROM maybe(stuff)
//  WHERE x = y
//
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

	l := ql.NewLexer(`
			SUBSCRIBETO
				count(x), Name
			FROM ourstream
			WHERE 
				k = REPLACE(LOWER(Name),'cde','xxx');
		`, ourDialect)

}

```

Inspiration/Other works
--------------------------

* http://harelba.github.io/q/
* https://github.com/dinedal/textql
* https://github.com/cloudson/gitql
* https://github.com/mattn/anko
* http://influxdb.com/docs/v0.8/api/query_language.html
* https://github.com/youtube/vitess
* http://prestodb.io/
* https://crate.io/docs/current/sql/index.html
* http://senseidb.com/
* https://github.com/pubsubsql/pubsubsql
* https://github.com/linkedin/databus
