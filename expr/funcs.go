package expr

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// the func mutex
	funcMu   sync.Mutex
	funcs    = make(map[string]Func)
	aggFuncs = make(map[string]Func)
)

type (
	// CustomFunc allows custom functions to be added for run-time evaluation
	// - Validate is called at parse time
	CustomFunc interface {
		Func(ctx EvalContext, args []value.Value) (value.Value, bool)
		Validate(n *FuncNode) error
	}
	// AggFunc allows custom functions to specify if they provide aggregation
	AggFunc interface {
		IsAgg() bool
	}
	// FuncResolver is a function resolution service that allows
	//  local/namespaced function resolution
	FuncResolver interface {
		FuncGet(name string) (Func, bool)
	}

	// FuncRegistry contains lists of functions
	// for different scope/run-time evaluation contexts
	FuncRegistry struct {
		mu    sync.Mutex
		funcs map[string]Func
	}
)

func NewFuncRegistry() *FuncRegistry {
	return &FuncRegistry{funcs: make(map[string]Func)}
}
func (m *FuncRegistry) Add(name string, fn interface{}) {
	name = strings.ToLower(name)
	newFunc := makeFunc(name, fn)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.funcs[name] = newFunc
}
func (m *FuncRegistry) FuncGet(name string) (Func, bool) {
	fn, ok := m.funcs[name]
	return fn, ok
}

// FuncAdd Global add Functions to the VM func registry occurs here.
//  Functions have the following pseudo interface.
//
//      1.  They must have expr.ContextReader as first argument
//      2.  They must accept 1 OR variadic number of value.Value arguments
//      3.  Return must be a value.Value, or anything that implements value Interface
//           and bool
//
//      func(ctx expr.ContextReader, value.Value...) (value.Value, bool) {
//          // function
//      }
//      func(ctx expr.ContextReader, value.Value...) (value.StringValue, bool) {
//          // function
//      }
//      func(ctx expr.ContextReader, value.Value, value.Value) (value.NumberValue, bool) {
//          // function
//      }
func FuncAdd(name string, fn interface{}) {
	funcMu.Lock()
	defer funcMu.Unlock()
	name = strings.ToLower(name)
	funcs[name] = makeFunc(name, fn)
}

// AggFuncAdd Adding Aggregate functions which are special functions
//  that perform aggregation operations
func AggFuncAdd(name string, fn interface{}) {
	funcMu.Lock()
	defer funcMu.Unlock()
	name = strings.ToLower(name)
	fun := makeFunc(name, fn)
	fun.Aggregate = true
	funcs[name] = fun
	aggFuncs[name] = fun
}

// FuncsGet get the global func registry
func FuncsGet() map[string]Func {
	return funcs
}

// IsAgg is this a aggregate function?
func IsAgg(name string) bool {
	_, isAgg := aggFuncs[name]
	return isAgg
}

func makeFunc(name string, fn interface{}) Func {

	f := Func{Name: name}

	if cf, isCustomFunc := fn.(CustomFunc); isCustomFunc {

		f.CustomFunc = cf

		if aggfn, hasAggFlag := fn.(AggFunc); hasAggFlag {
			f.Aggregate = aggfn.IsAgg()
		}

		return f
	}

	f.F = reflect.ValueOf(fn)
	funcType := f.F.Type()

	// Verify Return Values are appropriate
	if funcType.NumOut() != 2 {
		panic(fmt.Sprintf("%s must have 2 return values:   %s(Value, bool)", name, name))
	}

	f.ReturnValueType = value.ValueTypeFromRT(funcType.Out(0))

	if funcType.Out(1).Kind() != reflect.Bool {
		panic("Must have bool as 2nd return value (Value, bool)")
	}

	methodNumArgs := funcType.NumIn()
	if methodNumArgs == 0 {
		panic("Must have EvalContext as first func arg")
	}

	// first arg must meet expr.EvalContext
	// if funcType.In(0) == reflect.TypeOf(()(nil)) {
	// }
	methodNumArgs--

	f.Args = make([]reflect.Value, methodNumArgs)
	if funcType.IsVariadic() {
		f.VariadicArgs = true
	}

	return f
}
