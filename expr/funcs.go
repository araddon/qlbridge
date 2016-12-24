package expr

import (
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
	// Evaluator func is an evaluator which may be stateful (or not) for
	// evaluating custom functions
	EvaluatorFunc func(ctx EvalContext, args []value.Value) (value.Value, bool)
	// CustomFunc allows custom functions to be added for run-time evaluation
	// - Validate is called at parse time
	CustomFunc interface {
		Type() value.ValueType
		Validate(n *FuncNode) (EvaluatorFunc, error)
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

func EmptyEvalFunc(ctx EvalContext, args []value.Value) (value.Value, bool) {
	return value.NilValueVal, false
}

func NewFuncRegistry() *FuncRegistry {
	return &FuncRegistry{funcs: make(map[string]Func)}
}
func (m *FuncRegistry) Add(name string, fn CustomFunc) {
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
func FuncAdd(name string, fn CustomFunc) {
	funcMu.Lock()
	defer funcMu.Unlock()
	name = strings.ToLower(name)
	funcs[name] = makeFunc(name, fn)
}

// AggFuncAdd Adding Aggregate functions which are special functions
//  that perform aggregation operations
func AggFuncAdd(name string, fn CustomFunc) {
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

func makeFunc(name string, fn CustomFunc) Func {

	f := Func{Name: name, CustomFunc: fn}

	if aggfn, hasAggFlag := fn.(AggFunc); hasAggFlag {
		f.Aggregate = aggfn.IsAgg()
		if f.Aggregate {
			aggFuncs[name] = f
		}
	}

	return f
}
