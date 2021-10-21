package aggr

import (
	"strings"
	"sync"

	"github.com/araddon/qlbridge/value"
)

var aggrReg = NewAggrRegistry()

type AggregatorFactory func() Aggregator

type AggrRegistry struct {
	mu   sync.RWMutex
	aggs map[string]AggregatorFactory
}

type AggFactory interface {
	GetAggregator() AggregatorFactory
}

// Add a name/function to registry
func (m *AggrRegistry) Add(name string, aggr AggregatorFactory) {
	name = strings.ToLower(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aggs[name] = aggr
}

// FuncGet gets a function from registry if it exists.
func (m *AggrRegistry) AggrGet(name string) (AggregatorFactory, bool) {
	m.mu.RLock()
	fn, ok := m.aggs[name]
	m.mu.RUnlock()
	return fn, ok
}

// NewAggrRegistry create a new aggregator registry.
func NewAggrRegistry() *AggrRegistry {
	return &AggrRegistry{
		aggs: make(map[string]AggregatorFactory),
	}
}

// AggPartial is a struct to represent the partial aggregation
// that will be reduced on finalizer.  IE, for consistent-hash based
// group-bys calculated across multiple nodes this holds info that
// needs to be further calculated it only represents this hash.
type AggPartial struct {
	Ct int64
	N  float64
}

type AggFunc func(v value.Value)
type resultFunc func() interface{}
type Aggregator interface {
	Do(v value.Value)
	Result() interface{}
	Reset()
	Merge(*AggPartial)
}
type agg struct {
	do     AggFunc
	result resultFunc
}
type GroupByFunc struct {
	Last interface{}
}

func (m *GroupByFunc) Do(v value.Value)    { m.Last = v.Value() }
func (m *GroupByFunc) Result() interface{} { return m.Last }
func (m *GroupByFunc) Reset()              { m.Last = nil }
func (m *GroupByFunc) Merge(a *AggPartial) {}
func NewGroupByValue() Aggregator {
	return &GroupByFunc{}
}

type sum struct {
	partial bool
	ct      int64
	n       float64
}

func (m *sum) Do(v value.Value) {
	m.ct++
	switch vt := v.(type) {
	case value.IntValue:
		m.n += vt.Float()
	case value.NumberValue:
		m.n += vt.Val()
	}
}
func (m *sum) Result() interface{} {
	if !m.partial {
		return m.n
	}
	return &AggPartial{
		m.ct,
		m.n,
	}
}
func (m *sum) Reset() { m.n = 0 }
func (m *sum) Merge(a *AggPartial) {
	m.ct += a.Ct
	m.n += a.N
}
func NewSum(partial bool) Aggregator {
	return &sum{partial: partial}
}

type avg struct {
	partial bool
	ct      int64
	n       float64
}

func (m *avg) Do(v value.Value) {
	m.ct++
	switch vt := v.(type) {
	case value.IntValue:
		m.n += vt.Float()
	case value.NumberValue:
		m.n += vt.Val()
	}
}
func (m *avg) Result() interface{} {
	if !m.partial {
		return m.n / float64(m.ct)
	}
	return &AggPartial{
		m.ct,
		m.n,
	}
}
func (m *avg) Reset() { m.n = 0; m.ct = 0 }
func (m *avg) Merge(a *AggPartial) {
	m.ct += a.Ct
	m.n += a.N
}
func NewAvg(partial bool) Aggregator {
	return &avg{partial: partial}
}

type count struct {
	n int64
}

func (m *count) Do(v value.Value) {
	if v == nil || v.Nil() {
		return
	}
	m.n++
}
func (m *count) Result() interface{} {
	return m.n
}
func (m *count) Reset() { m.n = 0 }
func (m *count) Merge(a *AggPartial) {
	m.n += a.Ct
}
func NewCount() Aggregator {
	return &count{}
}

func AggrAdd(name string, aggr AggregatorFactory) {
	aggrReg.Add(name, aggr)
}

func AggrGet(name string) (AggregatorFactory, bool) {
	return aggrReg.AggrGet(name)
}
