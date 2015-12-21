package exec

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

// A filter to implement where clause
type Where struct {
	*TaskBase
	where expr.Node
}

func NewWhereFinal(ctx *plan.Context, stmt *expr.SqlSelect) *Where {
	s := &Where{
		TaskBase: NewTaskBase(ctx, "Where"),
		where:    stmt.Where.Expr,
	}
	cols := make(map[string]*expr.Column)

	if len(stmt.From) == 1 {
		cols = stmt.UnAliasedColumns()
	} else {
		// for _, col := range stmt.Columns {
		// 	_, right, _ := col.LeftRight()
		// 	u.Debugf("stmt col: %s %#v", right, col)
		// }

		for _, from := range stmt.From {
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

	s.Handler = whereFilter(s.where, s, cols)
	return s
}

// Where-Filter
func NewWhereFilter(ctx *plan.Context, stmt *expr.SqlSelect) *Where {
	s := &Where{
		TaskBase: NewTaskBase(ctx, "WhereFilter"),
		where:    stmt.Where.Expr,
	}
	cols := stmt.UnAliasedColumns()
	s.Handler = whereFilter(s.where, s, cols)
	return s
}

func whereFilter(where expr.Node, task TaskRunner, cols map[string]*expr.Column) MessageHandler {
	out := task.MessageOut()
	evaluator := vm.Evaluator(where)
	return func(ctx *plan.Context, msg datasource.Message) bool {

		var whereValue value.Value
		var ok bool
		//u.Debugf("WHERE:  T:%T  body%#v", msg, msg.Body())
		switch mt := msg.(type) {
		case *datasource.SqlDriverMessage:
			//u.Debugf("WHERE:  T:%T  vals:%#v", msg, mt.Vals)
			//u.Debugf("cols:  %#v", cols)
			msgReader := datasource.NewValueContextWrapper(mt, cols)
			whereValue, ok = evaluator(msgReader)
		case *datasource.SqlDriverMessageMap:
			whereValue, ok = evaluator(mt)
			//u.Debugf("WHERE: result:%v T:%T  \n\trow:%#v \n\tvals:%#v", whereValue, msg, mt.Row(), mt.Values())
			//u.Debugf("cols:  %#v", cols)
		default:
			if msgReader, ok := msg.(expr.ContextReader); ok {
				whereValue, ok = evaluator(msgReader)
			} else {
				u.Errorf("could not convert to message reader: %T", msg)
			}
		}
		//u.Debugf("msg: %#v", msgReader)
		//u.Infof("evaluating: ok?%v  result=%v where expr: '%s'", ok, whereValue.ToString(), where.String())
		if !ok {
			u.Debugf("could not evaluate: %v", msg)
			return false
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				//u.Debugf("Filtering out: T:%T   v:%#v", whereVal, whereVal)
				return true
			}
		case nil:
			return false
		default:
			if whereVal.Nil() {
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
