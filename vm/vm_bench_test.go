package vm_test

import (
	"testing"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

/*

go test -bench="Vm"


Benchmark testing for a few different aspects of vm

BenchmarkVmFuncNew-4   	 2000000	       789 ns/op
BenchmarkVmFuncOld-4   	  300000	      5741 ns/op


*/

// The new vm reflection-less func count
func BenchmarkVmFuncNew(b *testing.B) {

	n, err := expr.ParseExpression("count(str5) + count(int5)")
	if err != nil {
		b.Fail()
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		val, ok := vm.Eval(msgContext, n)
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
