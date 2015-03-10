package exec

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

// A scanner to filter by where clause
type Where struct {
	*TaskBase
	where expr.Node
}

func NewWhere(where expr.Node) *Where {
	s := &Where{
		TaskBase: NewTaskBase("Where"),
		where:    where,
	}
	s.Handler = whereFilter(where, s)
	return s
}

func whereFilter(where expr.Node, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	evaluator := vm.Evaluator(where)
	return func(ctx *Context, msg datasource.Message) bool {
		u.Debugf("got msg in where?:")
		// defer func() {
		// 	if r := recover(); r != nil {
		// 		u.Errorf("crap, %v", r)
		// 	}
		// }()
		if msgReader, ok := msg.Body().(expr.ContextReader); ok {

			whereValue, ok := evaluator(msgReader)
			u.Infof("evaluating: %v", ok)
			if !ok {
				u.Errorf("could not evaluate: %v", msg)
				return false
			}
			switch whereVal := whereValue.(type) {
			case value.BoolValue:
				if whereVal == value.BoolValueFalse {
					//u.Debugf("Filtering out")
					return true
				} else {
					//u.Debugf("NOT FILTERED OUT")
				}
			default:
				u.Warnf("unknown type? %T", whereVal)
			}
		} else {
			u.Errorf("could not convert to message reader: %T", msg.Body())
		}

		//u.Debug("about to send from where to forward")
		select {
		case out <- msg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}
