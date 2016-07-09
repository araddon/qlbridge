package exec

import (
	"fmt"
	"sort"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/vm"
)

// Order
type Order struct {
	*TaskBase
	p          *plan.Order
	complete   chan bool
	closed     bool
	isComplete bool
}

// NewORder create new order by exec task
func NewOrder(ctx *plan.Context, p *plan.Order) *Order {
	o := &Order{
		TaskBase: NewTaskBase(ctx),
		p:        p,
		complete: make(chan bool),
	}
	return o
}

func (m *Order) Close() error {
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()

	// what should this be?
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	//u.Infof("%p group by final Close() waiting for complete", m)
	select {
	case <-ticker.C:
		u.Warnf("order by timeout???? ")
	case <-m.complete:
		//u.Warnf("%p got groupbyfinal complete", m)
	}

	return m.TaskBase.Close()
}

func (m *Order) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()
	inCh := m.MessageIn()

	colIndex := m.p.Stmt.ColIndexes()
	orderCt := len(m.p.Stmt.OrderBy)

	// are are going to hold entire row in memory while we are calculating
	//  so obviously not scalable.
	sl := NewOrderMessages(m.p)

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
				var sdm *datasource.SqlDriverMessageMap

				switch mt := msg.(type) {
				case *datasource.SqlDriverMessageMap:
					sdm = mt
				default:

					msgReader, isContextReader := msg.(expr.ContextReader)
					if !isContextReader {
						err := fmt.Errorf("To use Join must use SqlDriverMessageMap but got %T", msg)
						u.Errorf("unrecognized msg %T", msg)
						close(m.TaskBase.sigCh)
						return err
					}

					sdm = datasource.NewSqlDriverMessageMapCtx(msg.Id(), msgReader, colIndex)
				}

				// We are going to use VM Engine to create a value for each statement in group by
				//  then join each value together to create a unique key.
				keys := make([]string, orderCt)
				for i, col := range m.p.Stmt.OrderBy {
					if col.Expr != nil {
						if key, ok := vm.Eval(sdm, col.Expr); ok {
							//u.Debugf("msgtype:%T  key:%q for-expr:%s", sdm, key, col.Expr)
							keys[i] = key.ToString()
						} else {
							// Is this an error?
							//u.Warnf("no key?  %s for %+v", col.Expr, sdm)
						}
					} else {
						//u.Warnf("no col.expr? %#v", col)
					}
				}

				//u.Infof("found key:%s for %+v", key, sdm)
				sl.l = append(sl.l, &msgkey{keys, sdm})
			}
		}
	}

	sort.Sort(sl)

	for _, m := range sl.l {
		//u.Debugf("got %s:%v msgs", key, vals)
		outCh <- m.msg
	}

	m.isComplete = true
	close(m.complete)

	return nil
}

type msgkey struct {
	keys []string
	msg  *datasource.SqlDriverMessageMap
}
type OrderMessages struct {
	l      []*msgkey
	invert []bool
}

func NewOrderMessages(p *plan.Order) *OrderMessages {
	invert := make([]bool, len(p.Stmt.OrderBy))
	for i, col := range p.Stmt.OrderBy {
		//u.Debugf("invert?  %s ORDER %v", col.Expr, col.Order)
		if col.Expr != nil {
			if !col.Asc() {
				invert[i] = true
			}
		}
	}
	return &OrderMessages{
		l:      make([]*msgkey, 0),
		invert: invert,
	}
}
func (m *OrderMessages) Len() int {
	return len(m.l)
}
func (m *OrderMessages) Less(i, j int) bool {
	for ki, key := range m.l[i].keys {
		if key < m.l[j].keys[ki] {
			if m.invert[ki] {
				return false
			}
			return true
		} else {
			if m.invert[ki] {
				return true
			}
		}
	}
	return false
}
func (m *OrderMessages) Swap(i, j int) {
	m.l[i], m.l[j] = m.l[j], m.l[i]
}
