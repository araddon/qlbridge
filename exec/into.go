package exec

import (
	"fmt"
	"net/url"
	"time"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

// Into - Write to output sink
type Into struct {
	*TaskBase
	p          *plan.Into
	complete   chan bool
	closed     bool
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


func (m *Into) Open(ctx *plan.Context, destination string) (err error) {

	params := make(map[string]interface{}, 0)
  	if m.TaskBase.Ctx.Stmt.(*rel.SqlSelect).With != nil {
		params = m.TaskBase.Ctx.Stmt.(*rel.SqlSelect).With
	}
	
	if url, err := url.Parse(destination); err == nil {
		switch url.Scheme {
			case "http":
				return fmt.Errorf("exec.Into http not implemented yet!")
			case "https":
				return fmt.Errorf("exec.Into https not implemented yet!")
			case "s3":
				m.sink, err = NewS3Sink(ctx, url.String(), params)
			default:
				return fmt.Errorf("exec.Into unrecognized scheme for %v\n", url)

		}
	} else { // First treat this as a output Table
		m.sink, err = NewTableSink(ctx, destination, params)
	}

	return
}


func (m *Into) Close() error {
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
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

  	m.colIndexes = m.TaskBase.Ctx.Stmt.(*rel.SqlSelect).ColIndexes()
	if m.colIndexes == nil {
		u.Errorf("Cannot get column indexes for output !")
		return nil
	}

	// Open the output file sink
	if err := m.Open(m.Ctx, m.p.Stmt.Table); err != nil {
		u.Errorf("Open output sink failed! - %v", err)
		return err
	}

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
					m.sink.Next(sdm.Values(), m.colIndexes)		// FIX: handle error return from Next()
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
				}
			}
		}
	}

	m.isComplete = true
	close(m.complete)

	return nil
}


