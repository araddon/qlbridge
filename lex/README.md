### Lexing/Dialects

QLBridge implements a few different Dialects:   *Sql*, *FilterQL*, *Expressions*, *json*

* *SQL* a subset, non-complete implementation of SQL
* *FilterQL*  A filtering language, think just the WHERE part of SQL but more DSL'ish with
    syntax `AND ( <expr>, <expr>, <expr> )` instead of `<expr> AND <expr> AND <expr>`
* *Expression*  Simple boolean logic expressions see https://github.com/araddon/qlbridge/blob/master/vm/vm_test.go#L57 for examples
* *Json*  Lexes json (instead of marshal)

### Creating a custom Lexer/Parser ie Dialect

See example in `dialects/example` folder for a custom ql dialect, this
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


