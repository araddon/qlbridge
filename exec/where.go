package exec

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

// A filter to implement where clause
type Where struct {
	*TaskBase
	filter expr.Node
	sel    *rel.SqlSelect
}

// Where-Filter
//  filters vs final differ bc the Final does final column aliasing
func NewWhere(ctx *plan.Context, p *plan.Where) *Where {
	if p.Final {
		return NewWhereFinal(ctx, p)
	}
	return NewWhereFilter(ctx, p.Stmt)
}

func NewWhereFinal(ctx *plan.Context, p *plan.Where) *Where {
	s := &Where{
		TaskBase: NewTaskBase(ctx),
		sel:      p.Stmt,
		filter:   p.Stmt.Where.Expr,
	}
	cols := make(map[string]*rel.Column)

	if len(p.Stmt.From) == 1 {
		cols = p.Stmt.UnAliasedColumns()
	} else {
		// for _, col := range p.Stmt.Columns {
		// 	_, right, _ := col.LeftRight()
		// 	u.Debugf("p.Stmt col: %s %#v", right, col)
		// }

		for _, from := range p.Stmt.From {
			//u.Debugf("cols: %v", from.Columns)
			//u.Infof("source: %#v", from.Source)
			for _, col := range from.Source.Columns {
				_, right, _ := col.LeftRight()
				//u.Debugf("col: %s %#v", right, col)
				if _, ok := cols[right]; !ok {

					cols[right] = col.Copy()
					cols[right].Index = len(cols) - 1
				} else {
					//u.Debugf("has col: %#v", col)
				}
			}
		}
	}

	//u.Debugf("found where columns: %d", len(cols))

	s.Handler = whereFilter(s.filter, s, cols)
	return s
}

// Where-Filter
//  filters vs final differ bc the Final does final column aliasing
func NewWhereFilter(ctx *plan.Context, sql *rel.SqlSelect) *Where {
	s := &Where{
		TaskBase: NewTaskBase(ctx),
		filter:   sql.Where.Expr,
	}
	cols := sql.UnAliasedColumns()
	s.Handler = whereFilter(s.filter, s, cols)
	return s
}

// Having-Filter
func NewHaving(ctx *plan.Context, p *plan.Having) *Where {
	// cols map[string]*rel.Column, filter expr.Node
	// NewHavingFilter(m.Ctx, sp.Stmt.UnAliasedColumns(), sp.Stmt.Having)
	s := &Where{
		TaskBase: NewTaskBase(ctx),
		filter:   p.Stmt.Having,
	}
	s.Handler = whereFilter(p.Stmt.Having, s, p.Stmt.UnAliasedColumns())
	return s
}

func whereFilter(filter expr.Node, task TaskRunner, cols map[string]*rel.Column) MessageHandler {
	out := task.MessageOut()
	evaluator := vm.Evaluator(filter)
	return func(ctx *plan.Context, msg schema.Message) bool {

		var filterValue value.Value
		var ok bool
		//u.Debugf("WHERE:  T:%T  body%#v", msg, msg.Body())
		switch mt := msg.(type) {
		case *datasource.SqlDriverMessage:
			//u.Debugf("WHERE:  T:%T  vals:%#v", msg, mt.Vals)
			//u.Debugf("cols:  %#v", cols)
			msgReader := datasource.NewValueContextWrapper(mt, cols)
			filterValue, ok = evaluator(msgReader)
		case *datasource.SqlDriverMessageMap:
			filterValue, ok = evaluator(mt)
			//u.Debugf("WHERE: result:%v T:%T  \n\trow:%#v \n\tvals:%#v", filterValue, msg, mt, mt.Values())
			//u.Debugf("cols:  %#v", cols)
		default:
			if msgReader, isContextReader := msg.(expr.ContextReader); isContextReader {
				filterValue, ok = evaluator(msgReader)
				if !ok {
					u.Warnf("wat? %v  filterval:%#v", filter.String(), filterValue)
				}
			} else {
				u.Errorf("could not convert to message reader: %T", msg)
			}
		}
		//u.Debugf("msg: %#v", msgReader)
		//u.Infof("evaluating: ok?%v  result=%v filter expr: '%s'", ok, filterValue.ToString(), filter.String())
		if !ok {
			u.Debugf("could not evaluate: %T %#v", msg, msg)
			return false
		}
		switch valTyped := filterValue.(type) {
		case value.BoolValue:
			if valTyped.Val() == false {
				//u.Debugf("Filtering out: T:%T   v:%#v", valTyped, valTyped)
				return true
			}
		case nil:
			return false
		default:
			if valTyped.Nil() {
				return false
			}
		}

		//u.Debugf("about to send from where to forward: %#v", msg)
		select {
		case out <- msg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}
