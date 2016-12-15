package vm

import (
	"testing"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

/*

go test -bench="Vm"


Benchmark testing for a few different aspects of vm

BenchmarkVmFuncNew-4   	 2000000	       789 ns/op
BenchmarkVmFuncOld-4   	  300000	      5741 ns/op


*/

func init() {
	// temp for benchmarking
	expr.AggFuncAdd("count.old", countFunc)
}

// this is the old style func that used reflection
func countFunc(ctx expr.EvalContext, val value.Value) (value.IntValue, bool) {
	if val.Err() || val.Nil() {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(1), true
}

// The new vm reflection-less func count
func BenchmarkVmFuncNew(b *testing.B) {

	n, err := expr.ParseExpression("count(str5) + count(int5)")
	if err != nil {
		b.Fail()
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		val, ok := Eval(msgContext, n)
		if !ok {
			b.Fail()
		}
		if iv, isInt := val.(value.IntValue); isInt {
			if iv.Val() != 2 {
				b.Fail()
			}
		} else {
			b.Fail()
		}
	}
}

// The old vm reflection-based func count
func BenchmarkVmFuncOld(b *testing.B) {

	n, err := expr.ParseExpression("count.old(str5) + count.old(int5)")
	if err != nil {
		b.Fail()
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		val, ok := Eval(msgContext, n)
		if !ok {
			b.Fail()
		}
		if iv, isInt := val.(value.IntValue); isInt {
			if iv.Val() != 2 {
				b.Fail()
			}
		} else {
			b.Fail()
		}
	}
}
