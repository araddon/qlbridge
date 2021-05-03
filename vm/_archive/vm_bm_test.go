package vm

import (
	"testing"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/value"
)

/*

go test -bench="Vm"

12/18/2014
BenchmarkVmParse			50000	     36411 ns/op	    3578 B/op	     118 allocs/op
BenchmarkVmExecute			50000	     45530 ns/op	    4605 B/op	     138 allocs/op

12/19/2014   (string contactenation in lex.PeekWord())
BenchmarkVmParse			100000	     19346 ns/op	    1775 B/op	      33 allocs/op
BenchmarkVmExecute			100000	     27774 ns/op	    2994 B/op	      53 allocs/op

12/20/2014  (faster machine - d5)
BenchmarkVmParse			200000	     13374 ns/op	    1775 B/op	      33 allocs/op
BenchmarkVmExecute			100000	     17472 ns/op	    2998 B/op	      53 allocs/op
BenchmarkVmExecuteNoParse	500000	      3429 ns/op	     737 B/op	      17 allocs/op
*/

var bmSql = []string{
	`select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`,
}

func BenchmarkVmParse(b *testing.B) {
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, sqlText := range bmSql {
			_, err := expr.ParseSql(sqlText)
			if err != nil {
				panic(err.Error())
			}
		}
	}
}

func verifyBenchmarkSql(t *testing.B, sql string, readContext datasource.ContextReader) *datasource.ContextSimple {

	sqlVm, err := NewSqlVm(sql)
	if err != nil {
		t.Fail()
	}

	writeContext := datasource.NewContextSimple()
	err = sqlVm.Execute(writeContext, readContext)
	if err != nil {
		t.Fail()
	}

	return writeContext
}

func BenchmarkVmExecute(b *testing.B) {
	msg := datasource.NewContextSimpleData(
		map[string]value.Value{
			"int5":       value.NewIntValue(5),
			"item_count": value.NewStringValue("5"),
			"reg_date":   value.NewStringValue("2014/11/01"),
			"user_id":    value.NewStringValue("abc")},
	)
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, sqlText := range bmSql {
			verifyBenchmarkSql(b, sqlText, msg)
		}
	}
}

func BenchmarkVmExecuteNoParse(b *testing.B) {
	readContext := datasource.NewContextSimpleData(
		map[string]value.Value{
			"int5":       value.NewIntValue(5),
			"item_count": value.NewStringValue("5"),
			"reg_date":   value.NewStringValue("2014/11/01"),
			"user_id":    value.NewStringValue("abc")},
	)
	sqlVm, err := NewSqlVm(bmSql[0])
	if err != nil {
		b.Fail()
	}
	writeContext := datasource.NewContextSimple()
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = sqlVm.Execute(writeContext, readContext)
		if err != nil {
			b.Fail()
		}
	}
}
