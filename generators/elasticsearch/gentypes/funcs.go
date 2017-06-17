package gentypes

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY
)

// FuncGenerators define functions that Generate Partial Filter statements
// They allow a qlbridge expression function to be converted to (elasticsearch,) etc
// underlying dialect specific object.
type FuncGenerator func(node *expr.FuncNode, depth int) (interface{}, error)

// FuncGenRegistry is a registry to register functions for a specific
// generator type
type FuncGenRegistry struct {
	GeneratorType string
	// Map of func name to generator
	funcs map[string]FuncGenerator
	mu    sync.Mutex
}

func NewFuncGenRegistry(genType string) *FuncGenRegistry {
	return &FuncGenRegistry{
		GeneratorType: genType,
		funcs:         make(map[string]FuncGenerator),
	}
}

func (m *FuncGenRegistry) Add(name string, fg FuncGenerator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = strings.ToLower(name)
	m.funcs[name] = fg
}
func (m *FuncGenRegistry) Get(name string) FuncGenerator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.funcs[name]
}
