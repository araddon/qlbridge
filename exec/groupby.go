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
	//colIndex: make(map[string]int),

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

	aggs, err := buildAggs(m.p.Stmt)
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
								u.Warnf("no key?  %s for %+v", col.Expr, mt)
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
			val := agg.Result()
			if val != nil {
				row[i] = val.Value()
			}
			agg.Reset()
			//u.Debugf("agg result: %#v  %v", row[i], row[i])
		}
		//u.Infof("row? %v", row)
		outCh <- datasource.NewSqlDriverMessageMap(i, row, colIndex)
		i++
	}

	return nil
}

type AggFunc func(v value.Value)
type resultFunc func() value.Value
type Aggregator interface {
	Do(v value.Value)
	Result() value.Value
	Reset()
}
type agg struct {
	do     AggFunc
	result resultFunc
}
type groupByFunc struct {
	last value.Value
}

func (m *groupByFunc) Do(v value.Value)    { m.last = v }
func (m *groupByFunc) Result() value.Value { return m.last }
func (m *groupByFunc) Reset()              { m.last = nil }
func NewGroupByValue(col *rel.Column) Aggregator {
	return &groupByFunc{}
}

type sum struct {
	n float64
}

func (m *sum) Do(v value.Value) {
	switch vt := v.(type) {
	case value.IntValue:
		m.n += vt.Float()
	case value.NumberValue:
		m.n += vt.Val()
	}
}
func (m *sum) Result() value.Value { return value.NewNumberValue(m.n) }
func (m *sum) Reset()              { m.n = 0 }
func NewSum(col *rel.Column) Aggregator {
	return &sum{}
}

type avg struct {
	n  float64
	ct int64
}

func (m *avg) Do(v value.Value) {
	switch vt := v.(type) {
	case value.IntValue:
		m.n += vt.Float()
	case value.NumberValue:
		m.n += vt.Val()
	}
	m.ct++
}
func (m *avg) Result() value.Value { return value.NewNumberValue(m.n / float64(m.ct)) }
func (m *avg) Reset()              { m.n = 0; m.ct = 0 }
func NewAvg(col *rel.Column) Aggregator {
	return &avg{}
}

type count struct {
	n int64
}

func (m *count) Do(v value.Value)    { m.n++ }
func (m *count) Result() value.Value { return value.NewIntValue(m.n) }
func (m *count) Reset()              { m.n = 0 }
func NewCount(col *rel.Column) Aggregator {
	return &count{}
}

func buildAggs(sql *rel.SqlSelect) ([]Aggregator, error) {
	aggs := make([]Aggregator, len(sql.Columns))
colLoop:
	for colIdx, col := range sql.Columns {
		for _, gb := range sql.GroupBy {
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
				aggs[colIdx] = NewAvg(col)
			case "count":
				aggs[colIdx] = NewCount(col)
			case "sum":
				aggs[colIdx] = NewCount(col)
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
