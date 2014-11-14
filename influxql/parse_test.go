package influxql

import (
	"flag"
	"testing"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser"
	"github.com/bmizerany/assert"
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

func tv(t ql.TokenType, v string) ql.Token {
	return ql.Token{T: t, V: v}
}

func verifyTokens(t *testing.T, sql string, tokens []ql.Token) {
	l := ql.NewLexer(sql, InfluxQlDialect)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equalf(t, tok.V, goodToken.V, "has='%v' want='%v'", tok.V, goodToken.V)
		assert.Equalf(t, tok.T, goodToken.T, "has='%v' want='%v'", tok.V, goodToken.V)
	}
}

func TestLexSimple(t *testing.T) {
	verifyTokens(t, `select * from "series with special characters!"`,
		[]ql.Token{
			tv(ql.TokenSelect, "select"),
			tv(ql.TokenStar, "*"),
			tv(ql.TokenFrom, "from"),
			tv(ql.TokenValue, "series with special characters!"),
		})
	verifyTokens(t, `select * from /.*/ limit 1"`,
		[]ql.Token{
			tv(ql.TokenSelect, "select"),
			tv(ql.TokenStar, "*"),
			tv(ql.TokenFrom, "from"),
			tv(ql.TokenRegex, "/.*/"),
			tv(ql.TokenLimit, "limit"),
			tv(ql.TokenInteger, "1"),
		})
	verifyTokens(t, `select * from /^stats\./i where time > now() - 1h;`,
		[]ql.Token{
			tv(ql.TokenSelect, "select"),
			tv(ql.TokenStar, "*"),
			tv(ql.TokenFrom, "from"),
			tv(ql.TokenRegex, "/^stats\\./i"),
			tv(ql.TokenWhere, "where"),
			tv(ql.TokenIdentity, "time"),
			tv(ql.TokenGT, ">"),
			tv(ql.TokenUdfExpr, "now"),
			tv(ql.TokenLeftParenthesis, "("),
			tv(ql.TokenRightParenthesis, ")"),
			tv(ql.TokenMinus, "-"),
		})
}

func TestLexContinuous(t *testing.T) {
	verifyTokens(t, `select percentile(value,95) from response_times group by time(5m) 
						into response_times.percentiles.5m.95`,
		[]ql.Token{
			tv(ql.TokenSelect, "select"),
			tv(ql.TokenUdfExpr, "percentile"),
			tv(ql.TokenLeftParenthesis, "("),
			tv(ql.TokenIdentity, "value"),
			tv(ql.TokenComma, ","),
			tv(ql.TokenInteger, "95"),
			tv(ql.TokenRightParenthesis, ")"),
			tv(ql.TokenFrom, "from"),
			tv(ql.TokenIdentity, "response_times"),
			tv(ql.TokenGroupBy, "group by"),
			tv(ql.TokenUdfExpr, "time"),
			tv(ql.TokenLeftParenthesis, "("),
			tv(ql.TokenDuration, "5m"),
		})
}
