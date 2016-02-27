package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*GroupBy)(nil)
)

// Group by
//
type GroupBy struct {
	*TaskBase
	p *plan.GroupBy
}

// A very stupid naive parallel groupby
//
//   task   ->  groupby  -->
//
func NewGroupBy(ctx *plan.Context, p *plan.GroupBy) *GroupBy {

	m := &GroupBy{
		TaskBase: NewTaskBase(ctx),
	}

	m.p = p

	return m
}

func (m *GroupBy) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *GroupBy) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()
	inCh := m.MessageIn()

	columns := m.p.Stmt.Columns
	colIndex := m.p.Stmt.ColIndexes()

	aggs, err := buildAggs(m.p)
	if err != nil {
		return err
	}

	gb := make(map[string][]*datasource.SqlDriverMessageMap)

msgReadLoop:
	for {

		select {
		case <-m.SigChan():
			u.Warnf("got signal quit")
			return nil
		case msg, ok := <-inCh:
			if !ok {
				//u.Debugf("NICE, got closed channel shutdown")
				break msgReadLoop
			} else {
				switch mt := msg.(type) {
				case *datasource.SqlDriverMessageMap:
					keys := make([]string, len(m.p.Stmt.GroupBy))
					for i, col := range m.p.Stmt.GroupBy {
						if col.Expr != nil {
							if key, ok := vm.Eval(mt, col.Expr); ok {
								//u.Debugf("msgtype:%T  key:%q for-expr:%s", mt, key, col.Expr)
								keys[i] = key.ToString()
							} else {
								//u.Warnf("no key?  %s for %+v", col.Expr, mt)
							}
						} else {
							u.Warnf("no col.expr? %#v", col)
						}
					}
					key := strings.Join(keys, ",")
					//u.Infof("found key:%s for %+v", key, mt)
					gb[key] = append(gb[key], mt)
				default:
					err := fmt.Errorf("To use Join must use SqlDriverMessageMap but got %T", msg)
					u.Errorf("unrecognized msg %T", msg)
					close(m.TaskBase.sigCh)
					return err
				}
			}
		}
	}

	i := uint64(0)
	for _, v := range gb {
		//u.Debugf("got %s:%v msgs", k, len(v))

		for _, mm := range v {
			for i, col := range columns {
				//u.Debugf("col: idx:%v sidx: %v pidx:%v key:%v   %s", col.Index, col.SourceIndex, col.ParentIndex, col.Key(), col.Expr)

				if col.Expr == nil {
					u.Warnf("wat?   nil col expr? %#v", col)
				} else {
					v, ok := vm.Eval(mm, col.Expr)
					//u.Infof("mt: %T  mm %#v", mm, mm)
					if !ok || v == nil {
						//u.Debugf("evaled nil? key=%v  val=%v expr:%s", col.Key(), v, col.Expr.String())
						//u.Infof("mt: %T  mm %#v", mm, mm)
						aggs[i].Do(value.NewNilValue())
					} else {
						//u.Debugf("evaled: key=%v  val=%v", col.Key(), v.Value())
						aggs[i].Do(v)
					}
				}
			}
		}

		row := make([]driver.Value, len(columns))
		for i, agg := range aggs {
			row[i] = driver.Value(agg.Result())
			agg.Reset()
			u.Debugf("agg result: %#v  %v", row[i], row[i])
		}
		u.Debugf("GroupBy output row? %v", row)
		outCh <- datasource.NewSqlDriverMessageMap(i, row, colIndex)
		i++
	}

	return nil
}

type AggFunc func(v value.Value)
type resultFunc func() interface{}
type Aggregator interface {
	Do(v value.Value)
	Result() interface{}
	Reset()
}
type agg struct {
	do     AggFunc
	result resultFunc
}
type groupByFunc struct {
	last interface{}
}

func (m *groupByFunc) Do(v value.Value)    { m.last = v }
func (m *groupByFunc) Result() interface{} { return m.last }
func (m *groupByFunc) Reset()              { m.last = nil }
func NewGroupByValue(col *rel.Column) Aggregator {
	return &groupByFunc{}
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
	return [2]interface{}{
		m.ct,
		m.n,
	}
}
func (m *sum) Reset() { m.n = 0 }
func NewSum(col *rel.Column, partial bool) Aggregator {
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
	return [2]interface{}{
		m.ct,
		m.n,
	}
}
func (m *avg) Reset() { m.n = 0; m.ct = 0 }
func NewAvg(col *rel.Column, partial bool) Aggregator {
	return &avg{partial: partial}
}

type count struct {
	n int64
}

func (m *count) Do(v value.Value)    { m.n++ }
func (m *count) Result() interface{} { return m.n }
func (m *count) Reset()              { m.n = 0 }
func NewCount(col *rel.Column) Aggregator {
	return &count{}
}

func buildAggs(p *plan.GroupBy) ([]Aggregator, error) {
	u.Infof("build aggs: partial:%v  sql:%s", p.Partial, p.Stmt)
	aggs := make([]Aggregator, len(p.Stmt.Columns))
colLoop:
	for colIdx, col := range p.Stmt.Columns {
		for _, gb := range p.Stmt.GroupBy {
			if gb.As == col.As {
				// simple
				aggs[colIdx] = NewGroupByValue(col)
				continue colLoop
			}
		}
		// Since we made it here, ann aggregate func
		//  move to a registry of some kind to allow extension
		switch n := col.Expr.(type) {
		case *expr.FuncNode:
			switch strings.ToLower(n.Name) {
			case "avg":
				aggs[colIdx] = NewAvg(col, p.Partial)
			case "count":
				aggs[colIdx] = NewCount(col)
			case "sum":
				aggs[colIdx] = NewSum(col, p.Partial)
			default:
				return nil, fmt.Errorf("Not impelemneted groupby for column: %s", col.Expr)
			}
		case *expr.BinaryNode:
			// binary logic?
			return nil, fmt.Errorf("Not impelemneted groupby for column: %s", col.Expr)
		}
	}
	return aggs, nil
}
