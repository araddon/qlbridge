package expr

import (
	"github.com/dataux/dataux/vendor/mixer/sqlparser"
	surgesql "github.com/surge/sqlparser"
	"testing"
)

/*

* surge/sqlparser is actually == vitess, branched
* dataux/dataux/vendor/mixer/sqlparser is also derived from vitess -> mixer


Benchmark testing, mostly used to try out different runtime strategies for speed


BenchmarkVitessParser1		   10000	    139669 ns/op
BenchmarkSurgeVitessParser1	   10000	    201577 ns/op
BenchmarkQlbridgeParser1	   50000	     35545 ns/op

go test -bench="Parser"

go test -bench="QlbridgeParser" --cpuprofile cpu.out

go tool pprof testutil.test cpu.out

web


*/

func BenchmarkVitessParser1(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := sqlparser.Parse(`
			SELECT count(*), repository.name
			FROM github_watch
			GROUP BY repository.name, repository.language`)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSurgeVitessParser1(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := surgesql.Parse(`
			SELECT count(*), repository.name
			FROM github_watch
			GROUP BY repository.name, repository.language`)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkQlbridgeParser1(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSql(`
			SELECT count(*), repository.name
			FROM github_watch
			GROUP BY repository.name, repository.language`)
		if err != nil {
			b.Fail()
		}
	}
}
