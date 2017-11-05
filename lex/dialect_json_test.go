package lex

import (
	"encoding/json"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
)

func TestJsonDialectInit(t *testing.T) {
	// Make sure we can init more than once, see if it panics
	JsonDialect.Init()
	for _, stmt := range JsonDialect.Statements {
		assert.NotEqual(t, "", stmt.String())
	}
}

func verifyJsonTokenTypes(t *testing.T, expString string, tokens []TokenType) {
	l := NewJsonLexer(expString)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equal(t, tok.T, goodToken, "want='%v' has %v ", goodToken, tok)
	}
}

func verifyJsonTokens(t *testing.T, expString string, tokens []Token) {
	l := NewJsonLexer(expString)
	for i, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equal(t, tok.T, goodToken.T, "%d want token type ='%v' has %v ", i, goodToken.T, tok.T)
		assert.Equal(t, tok.V, goodToken.V, "%d want token value='%v' has %v ", i, goodToken.V, tok.V)
	}
}

func TestLexJsonTokens(t *testing.T) {
	verifyJsonTokens(t, `["a",2,"b",true,{"name":"world"}]`,
		[]Token{
			tv(TokenLeftBracket, "["),
			tv(TokenValue, "a"),
			tv(TokenComma, ","),
			tv(TokenInteger, "2"),
			tv(TokenComma, ","),
			tv(TokenValue, "b"),
			tv(TokenComma, ","),
			tv(TokenBool, "true"),
			tv(TokenComma, ","),
			tv(TokenLeftBrace, "{"),
			tv(TokenIdentity, "name"),
			tv(TokenColon, ":"),
			tv(TokenValue, "world"),
			tv(TokenRightBrace, "}"),
			tv(TokenRightBracket, "]"),
		})
}

func TestLexJsonDialect(t *testing.T) {
	// The lexer should be able to parse json
	verifyJsonTokenTypes(t, `
		{
			"key1":"value2"
			,"key2":45, 
			"key3":["a",2,"b",true],
			"key4":{"hello":"value","age":55}
		}
		`,
		[]TokenType{TokenLeftBrace,
			TokenIdentity, TokenColon, TokenValue,
			TokenComma,
			TokenIdentity, TokenColon, TokenInteger,
			TokenComma,
			TokenIdentity, TokenColon, TokenLeftBracket, TokenValue, TokenComma, TokenInteger, TokenComma, TokenValue, TokenComma, TokenBool, TokenRightBracket,
			TokenComma,
			TokenIdentity, TokenColon, TokenLeftBrace, TokenIdentity, TokenColon, TokenValue, TokenComma, TokenIdentity, TokenColon, TokenInteger, TokenRightBrace,
			TokenRightBrace,
		})
}

/*

Benchmark testing


BenchmarkJsonLexer1	   10000	    121277 ns/op
BenchmarkJsonLexer2	  500000	      2982 ns/op
BenchmarkJsonMarshal	   10000	    106905 ns/op


go test -bench="Json"

go test -bench="JsonLexer" --cpuprofile cpu.out

go tool pprof lex.test cpu.out

web


*/

func BenchmarkJsonLexer1(b *testing.B) {
	jsonData := `{
		"took":62436,
		"errors":true,
		"items":[{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}}]
	}`
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		l := NewJsonLexer(jsonData)
		for {
			tok := l.NextToken()
			if tok.T == TokenEOF {
				break
			}
		}
	}
}

func BenchmarkJsonLexer2(b *testing.B) {
	jsonData := `{
		"took":62436,
		"errors":true,
		"items":[{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}}]
	}`
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		l := NewJsonLexer(jsonData)
	tokenLoop:
		for {
			tok := l.NextToken()
			switch {
			case tok.T == TokenEOF:
				break
			case tok.T == TokenIdentity && tok.V == "errors":
				tok = l.NextToken()
				tok = l.NextToken()
				if tok.T == TokenBool && tok.V == "true" {
					break tokenLoop // early exit
				}
			}
		}
	}
}

func BenchmarkJsonMarshal(b *testing.B) {
	jsonData := `{
		"took":62436,
		"errors":true,
		"items":[{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}},{"delete":{"_index":"testdel","_type":"type1","_id":"2","status":503,"error":"UnavailableShardsException[[testdel][3] Primary shard is not active or isn't assigned to a known node. Timeout: [1m], request: org.elasticsearch.action.bulk.BulkShardRequest@633961d0]"}}]
	}`
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		m := make(u.JsonHelper)
		err := json.Unmarshal([]byte(jsonData), &m)
		if err != nil {
			b.Fail()
		}
	}
}
