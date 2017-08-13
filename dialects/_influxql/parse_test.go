package influxql

import (
	"flag"
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/stretchr/testify/assert"
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

func tv(t lex.TokenType, v string) lex.Token {
	return lex.Token{T: t, V: v}
}

func verifyTokens(t *testing.T, sql string, tokens []lex.Token) {
	l := lex.NewLexer(sql, InfluxQlDialect)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equal(t, tok.V, goodToken.V, "has='%v' want='%v'", tok.V, goodToken.V)
		assert.Equal(t, tok.T, goodToken.T, "has='%v' want='%v'", tok.V, goodToken.V)
	}
}

func TestLexSimple(t *testing.T) {
	verifyTokens(t, `select * from "series with special characters!"`,
		[]lex.Token{
			tv(lex.TokenSelect, "select"),
			tv(lex.TokenStar, "*"),
			tv(lex.TokenFrom, "from"),
			tv(lex.TokenValue, "series with special characters!"),
		})
	verifyTokens(t, `select * from /.*/ limit 1"`,
		[]lex.Token{
			tv(lex.TokenSelect, "select"),
			tv(lex.TokenStar, "*"),
			tv(lex.TokenFrom, "from"),
			tv(lex.TokenRegex, "/.*/"),
			tv(lex.TokenLimit, "limit"),
			tv(lex.TokenInteger, "1"),
		})
	verifyTokens(t, `select * from /^stats\./i where time > now() - 1h;`,
		[]lex.Token{
			tv(lex.TokenSelect, "select"),
			tv(lex.TokenStar, "*"),
			tv(lex.TokenFrom, "from"),
			tv(lex.TokenRegex, "/^stats\\./i"),
			tv(lex.TokenWhere, "where"),
			tv(lex.TokenIdentity, "time"),
			tv(lex.TokenGT, ">"),
			tv(lex.TokenUdfExpr, "now"),
			tv(lex.TokenLeftParenthesis, "("),
			tv(lex.TokenRightParenthesis, ")"),
			tv(lex.TokenMinus, "-"),
		})
}

func TestLexContinuous(t *testing.T) {
	verifyTokens(t, `select percentile(value,95) from response_times group by time(5m) 
						into response_times.percentiles.5m.95`,
		[]lex.Token{
			tv(lex.TokenSelect, "select"),
			tv(lex.TokenUdfExpr, "percentile"),
			tv(lex.TokenLeftParenthesis, "("),
			tv(lex.TokenIdentity, "value"),
			tv(lex.TokenComma, ","),
			tv(lex.TokenInteger, "95"),
			tv(lex.TokenRightParenthesis, ")"),
			tv(lex.TokenFrom, "from"),
			tv(lex.TokenIdentity, "response_times"),
			tv(lex.TokenGroupBy, "group by"),
			tv(lex.TokenUdfExpr, "time"),
			tv(lex.TokenLeftParenthesis, "("),
			tv(lex.TokenDuration, "5m"),
		})
}
