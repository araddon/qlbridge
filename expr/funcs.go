package expr

import (
	"strings"
	"sync"

	"github.com/araddon/qlbridge/value"
)

var (
	// The global function registry
	funcReg = NewFuncRegistry()
)

type (
	// EvaluatorFunc defines the evaluator func which may be stateful (or not) for
	// evaluating custom functions
	EvaluatorFunc func(ctx EvalContext, args []value.Value) (value.Value, bool)
	// CustomFunc allows custom functions to be added for run-time evaluation
	CustomFunc interface {
		// Type Define the Return Type of this function, or use value.Value for unknown.
		Type() value.ValueType
		// Validate is parse time syntax and type evaluation.  Also returns the evaluation
		// function.
		Validate(n *FuncNode) (EvaluatorFunc, error)
	}
	// AggFunc allows custom functions to specify if they provide aggregation
	AggFunc interface {
		IsAgg() bool
	}
	// FuncResolver is a function resolution interface that allows
	// local/namespaced function resolution.
	FuncResolver interface {
		FuncGet(name string) (Func, bool)
	}

	// FuncRegistry contains lists of functions for different scope/run-time evaluation contexts.
	FuncRegistry struct {
		mu    sync.RWMutex
		funcs map[string]Func
		aggs  map[string]struct{}
	}
)

// EmptyEvalFunc a no-op evaluation function for use in
func EmptyEvalFunc(ctx EvalContext, args []value.Value) (value.Value, bool) {
	return value.NilValueVal, false
}

// NewFuncRegistry create a new function registry. By default their is a
// global one, but you can have local function registries as well.
func NewFuncRegistry() *FuncRegistry {
	return &FuncRegistry{
		funcs: make(map[string]Func),
		aggs:  make(map[string]struct{}),
	}
}

// Add a name/function to registry
func (m *FuncRegistry) Add(name string, fn CustomFunc) {
	name = strings.ToLower(name)
	newFunc := Func{Name: name, CustomFunc: fn}
	m.mu.Lock()
	defer m.mu.Unlock()
	aggfn, hasAggFlag := fn.(AggFunc)
	if hasAggFlag {
		newFunc.Aggregate = aggfn.IsAgg()
		if newFunc.Aggregate {
			m.aggs[name] = struct{}{}
		}
	}
	m.funcs[name] = newFunc
}

// FuncGet gets a function from registry if it exists.
func (m *FuncRegistry) FuncGet(name string) (Func, bool) {
	m.mu.RLock()
	fn, ok := m.funcs[name]
	m.mu.RUnlock()
	return fn, ok
}

// FuncAdd Global add Functions to the VM func registry occurs here.
func FuncAdd(name string, fn CustomFunc) {
	funcReg.Add(name, fn)
}
