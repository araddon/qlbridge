package exec

import (
	"database/sql/driver"
	"math"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

// Projection Execution Task
type Projection struct {
	*TaskBase
	closed bool
	p      *plan.Projection
}

// In Process projections are used when mapping multiple sources together
// and additional columns such as those used in Where, GroupBy etc are used
// even if they will not be used in Final projection
func NewProjection(ctx *plan.Context, p *plan.Projection) *Projection {
	if p.Final {
		return NewProjectionFinal(ctx, p)
	}
	return NewProjectionInProcess(ctx, p)
}

// In Process projections are used when mapping multiple sources together
//  and additional columns such as those used in Where, GroupBy etc are used
//  even if they will not be used in Final projection
func NewProjectionInProcess(ctx *plan.Context, p *plan.Projection) *Projection {
	s := &Projection{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	s.Handler = s.projectionEvaluator(p.Final)
	return s
}

// Final Projections project final select columns for result-writing
func NewProjectionFinal(ctx *plan.Context, p *plan.Projection) *Projection {
	s := &Projection{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	s.Handler = s.projectionEvaluator(p.Final)
	return s
}

// NewProjectionLimit Only provides counting/limit projection
func NewProjectionLimit(ctx *plan.Context, p *plan.Projection) *Projection {
	s := &Projection{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	s.Handler = s.limitEvaluator()
	return s
}

func (m *Projection) drain() {
	drainCt := 0
	for {
		select {
		case _, ok := <-m.msgInCh:
			if !ok {
				if drainCt > 0 {
					u.Debugf("%p NICE, drained %v msgs", m, drainCt)
				}
				return
			}
			drainCt++
			//u.Debugf("%p dropping msg %v", msg)
		}
	}
}

// Close cleans up and closes channels
func (m *Projection) Close() error {
	//u.Debugf("Projection Close  alreadyclosed?%v", m.closed)
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()

	go m.drain()

	//return m.TaskBase.Close()
	return nil
}

// CloseFinal after exit, cleanup some more
func (m *Projection) CloseFinal() error {
	//u.Debugf("Projection CloseFinal  alreadyclosed?%v", m.closed)
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	//return nil
	return m.TaskBase.Close()
}

// Create handler function for evaluation (ie, field selection from tuples)
func (m *Projection) projectionEvaluator(isFinal bool) MessageHandler {

	out := m.MessageOut()
	columns := m.p.Stmt.Columns
	colIndex := m.p.Stmt.ColIndexes()
	limit := m.p.Stmt.Limit
	if limit == 0 {
		limit = math.MaxInt32
	}
	colCt := len(columns)
	// If we have a projection, use that as col count
	if m.p.Proj != nil {
		colCt = len(m.p.Proj.Columns)
	}

	rowCt := 0
	return func(ctx *plan.Context, msg schema.Message) bool {

		select {
		case <-m.SigChan():
			u.Debugf("%p closed, returning", m)
			return false
		default:
		}

		//u.Infof("got projection message: %T %#v", msg, msg.Body())
		var outMsg schema.Message
		switch mt := msg.(type) {
		case *datasource.SqlDriverMessageMap:
			// use our custom write context for example purposes
			row := make([]driver.Value, colCt)
			rdr := datasource.NewNestedContextReader([]expr.ContextReader{
				mt,
				ctx.Session,
			}, mt.Ts())
			//u.Debugf("about to project: %#v", mt)
			colIdx := -1
			for _, col := range columns {
				colIdx += 1
				//u.Debugf("%d  colidx:%v sidx: %v pidx:%v key:%q Expr:%v", colIdx, col.Index, col.SourceIndex, col.ParentIndex, col.Key(), col.Expr)

				if isFinal && col.ParentIndex < 0 {
					continue
				}

				if col.Guard != nil {
					ifColValue, ok := vm.Eval(rdr, col.Guard)
					if !ok {
						// Most likely scenario here is Missing Columns.
						// Unlikely traditional sql, we are going to operate in both strict-schema mode
						// which would error, and sparse which will not, more like no-sql.
						u.Errorf("Could not evaluate if:   %v", col.Guard.String())
						//return fmt.Errorf("Could not evaluate if clause: %v", col.Guard.String())
					}
					//u.Debugf("if eval val:  %T:%v", ifColValue, ifColValue)
					switch ifColVal := ifColValue.(type) {
					case value.BoolValue:
						if ifColVal.Val() == false {
							//u.Debugf("Filtering out col")
							continue
						}
					}
				}
				if col.Star {
					starRow := mt.Values()
					//u.Infof("star row: %#v", starRow)
					if len(columns) > 1 {
						//   select *, myvar, 1
						newRow := make([]driver.Value, colCt)
						for curi := 0; curi < colIdx; curi++ {
							newRow[curi] = row[curi]
						}
						row = newRow
						for _, v := range starRow {
							//writeContext.Put(&expr.Column{As: k}, nil, value.NewValue(v))
							row[colIdx] = v
							colIdx += 1
						}
						colIdx--
					} else {
						//   select * FROM Z
						for _, v := range starRow {
							//writeContext.Put(&expr.Column{As: k}, nil, value.NewValue(v))
							//u.Infof("colct: %v   v:%v", colIdx, v)
							row[colIdx] = v
							colIdx += 1
						}
						colIdx--
					}

				} else if col.Expr == nil {
					u.Warnf("wat?   nil col expr? %#v", col)
				} else {
					v, ok := vm.Eval(rdr, col.Expr)
					if !ok {
						u.Warnf("failed eval key=%q  val=%#v expr:%q  expr:%#v mt:%#v", col.Key(), v, col.Expr, col.Expr, mt)
						// for k, v := range ctx.Session.Row() {
						// 	u.Infof("%p session? %s: %v", ctx.Session, k, v.Value())
						// }

					} else if v == nil {
						//u.Debugf("%#v", col)
						//u.Debugf("evaled nil? key=%v  val=%v expr:%s", col.Key(), v, col.Expr.String())
						//writeContext.Put(col, mt, v)
						//u.Infof("mt: %T  mt %#v", mt, mt)
						row[colIdx] = nil //v.Value()
					} else {
						//u.Debugf("%d:%d row:%d evaled: %v  val=%v", colIdx, colCt, len(row), col, v.Value())
						//writeContext.Put(col, mt, v)
						row[colIdx] = v.Value()
					}
				}
			}
			//u.Infof("row: %#v", row)
			//u.Infof("row cols: %v", colIndex)
			outMsg = datasource.NewSqlDriverMessageMap(0, row, colIndex)

		case expr.ContextReader:
			//u.Warnf("nice, got context reader? %T", mt)
			row := make([]driver.Value, len(columns))
			//u.Debugf("about to project: %#v", mt)
			colIdx := 0
			for i, col := range columns {
				//u.Debugf("col: idx:%v sidx: %v pidx:%v key:%v   %s", col.Index, col.SourceIndex, col.ParentIndex, col.Key(), col.Expr)

				if isFinal && col.ParentIndex < 0 {
					continue
				}

				if col.Guard != nil {
					ifColValue, ok := vm.Eval(mt, col.Guard)
					if !ok {
						u.Errorf("Could not evaluate if:   %v", col.Guard.String())
						//return fmt.Errorf("Could not evaluate if clause: %v", col.Guard.String())
					}
					//u.Debugf("if eval val:  %T:%v", ifColValue, ifColValue)
					switch ifColVal := ifColValue.(type) {
					case value.BoolValue:
						if ifColVal.Val() == false {
							//u.Debugf("Filtering out col")
							continue
						}
					}
				}
				if col.Star {
					starRow := mt.Row()
					newRow := make([]driver.Value, len(starRow)+len(colIndex))
					for curi := 0; curi < i; curi++ {
						newRow[curi] = row[curi]
					}
					row = newRow
					for _, v := range starRow {
						colIdx += 1
						//writeContext.Put(&expr.Column{As: k}, nil, value.NewValue(v))
						row[i+colIdx] = v
					}
				} else if col.Expr == nil {
					u.Warnf("wat?   nil col expr? %#v", col)
				} else {
					v, ok := vm.Eval(mt, col.Expr)
					if !ok {
						//u.Warnf("failed eval key=%v  val=%#v expr:%s   mt:%#v", col.Key(), v, col.Expr, mt.Row())
					} else if v == nil {
						//u.Debugf("%#v", col)
						//u.Debugf("evaled nil? key=%v  val=%v expr:%s", col.Key(), v, col.Expr.String())
						//writeContext.Put(col, mt, v)
						//u.Infof("mt: %T  mt %#v", mt, mt)
						row[i+colIdx] = nil //v.Value()
					} else {
						//u.Debugf("evaled: key=%v  val=%v", col.Key(), v.Value())
						//writeContext.Put(col, mt, v)
						row[i+colIdx] = v.Value()
					}
				}
			}
			//u.Infof("row: %#v cols:%#v", row, colIndex)
			//u.Infof("row cols: %v", colIndex)
			outMsg = datasource.NewSqlDriverMessageMap(0, row, colIndex)

		default:
			u.Errorf("could not project msg:  %T", msg)
		}

		if rowCt >= limit {
			//u.Debugf("%p Projection reaching Limit!!! rowct:%v  limit:%v", m, rowCt, limit)
			out <- nil // Sending nil message is a message to downstream to shutdown
			m.Quit()   // should close rest of dag as well
			return false
		}
		rowCt++

		//u.Debugf("row:%d  completed projection for: %p %#v", rowCt, out, outMsg)
		select {
		case out <- outMsg:
			return true
		case <-m.SigChan():
			return false
		}
	}
}

// Limit only evaluator
func (m *Projection) limitEvaluator() MessageHandler {

	out := m.MessageOut()
	limit := m.p.Stmt.Limit
	if limit == 0 {
		limit = math.MaxInt32
	}

	rowCt := 0
	return func(ctx *plan.Context, msg schema.Message) bool {

		select {
		case <-m.SigChan():
			u.Debugf("%p closed, returning", m)
			return false
		default:
		}

		if rowCt >= limit {
			if rowCt == limit {
				//u.Debugf("%p Projection reaching Limit!!! rowct:%v  limit:%v", m, rowCt, limit)
				out <- nil // Sending nil message is a message to downstream to shutdown
				//m.Close()
				m.Quit()
			}
			rowCt++
			//return false
			return true // swallow it
		}
		rowCt++

		select {
		case out <- msg:
			return true
		case <-m.SigChan():
			return false
		}
	}
}
