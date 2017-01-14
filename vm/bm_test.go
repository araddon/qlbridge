package vm_test

import (
	"reflect"
	"testing"
)

/*
	Benchmark testing, mostly used to try out different runtime strategies for speed


BenchmarkReflectionKind	    10000000	       285  ns/op
BenchmarkReflectionKind2	 5000000	       615  ns/op
BenchmarkReflectionOurType	20000000	       136  ns/op
BenchmarkReflectionOurType2	10000000	       192  ns/op
BenchmarkReflectionOurType3	50000000	       33.8 ns/op
BenchmarkReflectionOurType4	20000000	       90.5 ns/op
BenchmarkReflectionOurType5	50000000	       42.3 ns/op

*/
// go test -bench="Reflection"

type OurType int

const (
	OurInt OurType = iota
	OurString
	OurBool
)

func (m OurType) String() string {
	switch m {
	case OurInt:
		return "int"
	case OurString:
		return "string"
	default:
		return "unknown"
	}
}

type FakeNode interface {
	Kind() reflect.Kind
	OurType() OurType
}

type FakeNodeStuff struct {
	ot  OurType
	rv  reflect.Value
	val interface{}
}

func (m *FakeNodeStuff) Kind() reflect.Kind {
	switch m.ot {
	case OurInt:
		return m.rv.Kind()
	case OurString:
		return m.rv.Kind()
	default:
		panic("unknown")
	}
}

func (m *FakeNodeStuff) Kind2() reflect.Kind {
	return reflect.ValueOf(m.val).Kind()
}

func (m *FakeNodeStuff) OurType() OurType {
	return m.ot
}

func (m *FakeNodeStuff) OurType2() OurType {
	switch m.ot {
	case OurInt:
		return m.ot
	case OurString:
		return m.ot
	case OurBool:
		return m.ot
	default:
		panic("unknown")
	}
}

/*
Notes:
  - type switching isn't that expensive, it is but not hugely so
  - you do want to memoize the reflect.Value though

*/
// We are going to test use of one time creation of Reflect Value, then 20 iterations
func BenchmarkReflectionKind(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString, rv: reflect.ValueOf("hello")}
		for j := 0; j < 20; j++ {
			if k := n.Kind(); k != reflect.String {
				//
			}
		}
	}
}
func BenchmarkReflectionKind2(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString, val: "hello"}
		for j := 0; j < 20; j++ {
			if k := n.Kind2(); k != reflect.String {
				//
			}
		}
	}
}
func BenchmarkReflectionOurType(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString, rv: reflect.ValueOf("hello")}
		for j := 0; j < 20; j++ {
			if k := n.OurType(); k != OurString {
				//
			}
		}
	}
}
func BenchmarkReflectionOurType2(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString, rv: reflect.ValueOf("hello")}
		for j := 0; j < 20; j++ {
			if k := n.OurType2(); k != OurString {
				//
			}
		}
	}
}
func BenchmarkReflectionOurType3(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString}
		for j := 0; j < 20; j++ {
			if k := n.OurType(); k != OurString {
				//
			}
		}
	}
}
func BenchmarkReflectionOurType4(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString}
		for j := 0; j < 20; j++ {
			if k := n.OurType2(); k != OurString {
				//
			}
		}
	}
}
func BenchmarkReflectionOurType5(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := FakeNodeStuff{ot: OurString}
		for j := 0; j < 20; j++ {
			switch n.OurType() {
			case OurInt:
				//
			case OurString:
				//
			case OurBool:
				//
			default:
				panic("unknown")
			}
		}
	}
}
