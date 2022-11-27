package exec

import (
    "database/sql/driver"
	"fmt"
	"net/url"
	"time"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

var (
    sinkFactories = make(map[string]SinkMaker)
)

// Into - Write to output sink
type Into struct {
	*TaskBase
	p          *plan.Into
	complete   chan bool
	Closed     bool
	isComplete bool
	colIndexes map[string]int
	sink	   Sink
}

// NewInto create new into exec task
func NewInto(ctx *plan.Context, p *plan.Into) *Into {
	o := &Into{
		TaskBase: NewTaskBase(ctx),
		p:        p,
		complete: make(chan bool),
	}
	return o
}

// Registry for sinks
func Register(name string, factory SinkMaker) {
    if factory == nil {
        panic(fmt.Sprintf("SinkMaker factory %s does not exist.", name))
    }
    _, registered := sinkFactories[name]
    if registered {
        return
    }
    sinkFactories[name] = factory
}


func (m *Into) Open(ctx *plan.Context, destination string) (err error) {

	params := make(map[string]interface{}, 0)
  	if m.TaskBase.Ctx.Stmt.(*rel.SqlSelect).With != nil {
		params = m.TaskBase.Ctx.Stmt.(*rel.SqlSelect).With
	}
	
	if url, err := url.Parse(destination); err == nil {
		if newSink, ok := sinkFactories[url.Scheme]; !ok {
			m := fmt.Sprintf("scheme [%s] not registered!", url.Scheme)
			panic(m)
		} else {
			m.sink, err = newSink(ctx, destination, params)
		}
	} else { // First treat this as a output Table
		if newSink, ok := sinkFactories["table"]; !ok {
			m := fmt.Sprintf("INTO <TABLE> sink factory not found!")
			panic(m)
		} else {
			m.sink, err = newSink(ctx, destination, params)
		}
	}
	return
}


func (m *Into) Close() error {
	m.Lock()
	if m.Closed {
		m.Unlock()
		return nil
	}
	m.Closed = true
	m.sink.Close()	//FIX: handle error on close
	m.Unlock()

	// what should this be?
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	//u.Infof("%p into sink final Close() waiting for complete", m)
	select {
	case <-ticker.C:
		u.Warnf("into sink timeout???? ")
	case <-m.complete:
		//u.Warnf("%p got into sink complete", m)
	}

	return m.TaskBase.Close()
}

func (m *Into) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	//outCh := m.MessageOut()
	inCh := m.MessageIn()

    projCols := m.TaskBase.Ctx.Projection.Proj.Columns
	cols := make(map[string]int, len(projCols))
	for i, col := range projCols {
		//u.Debugf("aliasing: key():%-15q  As:%-15q   %-15q", col.Key(), col.As, col.String())
		cols[col.As] = i
	}

  	//m.colIndexes = m.TaskBase.Ctx.Stmt.(*rel.SqlSelect).ColIndexes()
  	m.colIndexes = cols
	if m.colIndexes == nil {
		u.Errorf("Cannot get column indexes for output !")
		return nil
	}

	// Open the output file sink
	if err := m.Open(m.Ctx, m.p.Stmt.Table); err != nil {
		u.Errorf("Open output sink failed! - %v", err)
		return err
	}

    var rowCount, lastMsgId int64

msgReadLoop:
	for {
		select {
		case <-m.SigChan():
			//u.Warnf("got signal quit")
			return nil
        case <-m.ErrChan():
			//u.Warnf("got err signal")
            m.sink.Cleanup()
			return nil
		case msg, ok := <-inCh:
			if !ok {
				//u.Warnf("NICE, got closed channel shutdown")
				//close(m.TaskBase.sigCh)
				break msgReadLoop
			} else {
				var sdm *datasource.SqlDriverMessageMap

				switch mt := msg.(type) {
				case *datasource.SqlDriverMessageMap:
					sdm = mt
					m.sink.Next(sdm.Values(), m.colIndexes)		// FIX: handle error return from Next()
                    rowCount++
                    lastMsgId = int64(mt.Id())
				default:

					msgReader, isContextReader := msg.(expr.ContextReader)
					if !isContextReader {
						err := fmt.Errorf("To use Into must use SqlDriverMessageMap but got %T", msg)
						u.Errorf("unrecognized msg %T", msg)
						close(m.TaskBase.sigCh)
						return err
					}

					sdm = datasource.NewSqlDriverMessageMapCtx(msg.Id(), msgReader, m.colIndexes)
					m.sink.Next(sdm.Values(), m.colIndexes)		// FIX: handle error return from Next()
                    rowCount++
                    lastMsgId = int64(msg.Id())
				}
			}
		}
	}
//u.Warnf("HERE 1 %#v, %p, LEN = %d", m.ErrChan(), m.ErrChan(), len(m.ErrChan()))
errLoop:
	for {
		select {
		case <-m.ErrChan():
//u.Warnf("HERE ERR")
			m.sink.Cleanup()
			break errLoop
		default:
		}
		select {
		case <-m.ErrChan():
//u.Warnf("HERE 3")
			m.sink.Cleanup()
			break errLoop
		case <-m.SigChan():
//u.Warnf("HERE 2")
			break errLoop
        case _, ok := <-inCh:
//u.Warnf("HERE 4")
            if !ok {
                break errLoop
			}
		}
	}
    vals := make([]driver.Value, 2)
    vals[0] = lastMsgId
    vals[1] = rowCount
	m.msgOutCh <- datasource.NewSqlDriverMessage(0, vals)
	m.isComplete = true
	close(m.complete)

	return nil
}


