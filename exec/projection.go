package exec

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/vm"
)

type Projection struct {
	*TaskBase
	sql *expr.SqlSelect
}

func NewProjection(sqlSelect *expr.SqlSelect) *Projection {
	s := &Projection{
		TaskBase: NewTaskBase("Projection"),
		sql:      sqlSelect,
	}
	s.Handler = projectionEvaluator(sqlSelect, s)
	return s
}

func projectionEvaluator(sql *expr.SqlSelect, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	//evaluator := vm.Evaluator(where)
	return func(ctx *Context, msg datasource.Message) bool {
		defer func() {
			if r := recover(); r != nil {
				u.Errorf("crap, %v", r)
			}
		}()

		var outMsg datasource.Message
		// uv := msg.Body().(url.Values)
		switch mt := msg.Body().(type) {
		case *datasource.ContextUrlValues:
			// readContext := datasource.NewContextUrlValues(uv)
			// use our custom write context for example purposes
			writeContext := datasource.NewContextSimple()
			outMsg = writeContext
			//u.Infof("about to evaluate:  %T", outMsg)
			for _, col := range sql.Columns {
				if col.Guard != nil {
					// TODO:  evaluate if guard
				}
				if col.Star {
					for k, v := range mt.Row() {
						writeContext.Put(&expr.Column{As: k}, nil, v)
					}
				} else {
					//u.Debugf("tree.Root: as?%v %#v", col.As, col.Tree.Root)
					v, ok := vm.Eval(mt, col.Tree.Root)
					//u.Debugf("evaled: ok?%v key=%v  val=%v", ok, col.Key(), v)
					if ok {
						writeContext.Put(col, mt, v)
					}
				}

			}
		}

		//u.Debugf("about to send msg: %T", outMsg)
		select {
		case out <- outMsg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}
