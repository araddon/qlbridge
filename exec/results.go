package exec

import (
	"database/sql/driver"
	"io"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY

	// ensure our resultwriter implements database/sql/driver `driver.Rows`
	_ driver.Rows = (*ResultWriter)(nil)

	// Ensure that we implement the Task Runner interface
	// to ensure this can run in exec engine
	_ TaskRunner = (*ResultExecWriter)(nil)
	_ TaskRunner = (*ResultWriter)(nil)
	_ TaskRunner = (*ResultBuffer)(nil)
)

type ResultExecWriter struct {
	*TaskBase
	err          error
	rowsAffected int64
	lastInsertId int64
}
type ResultWriter struct {
	*TaskBase
	cols []string
}
type ResultBuffer struct {
	*TaskBase
	cols []string
}

func NewResultExecWriter(ctx *plan.Context) *ResultExecWriter {
	m := &ResultExecWriter{
		TaskBase: NewTaskBase(ctx, "ResultExecWriter"),
	}
	m.Handler = func(ctx *plan.Context, msg schema.Message) bool {
		switch mt := msg.(type) {
		case *datasource.SqlDriverMessage:
			//u.Debugf("Result:  T:%T  vals:%#v", msg, mt.Vals)
			if len(mt.Vals) > 1 {
				m.lastInsertId = mt.Vals[0].(int64)
				m.rowsAffected = mt.Vals[1].(int64)
			}
		case nil:
			u.Warnf("got nil")
			// Signal to quit
			return false

		default:
			u.Errorf("could not convert to message reader: %T", msg)
		}

		return true
	}
	return m
}

func NewResultWriter(ctx *plan.Context) *ResultWriter {
	m := &ResultWriter{
		TaskBase: NewTaskBase(ctx, "ResultWriter"),
	}
	m.Handler = resultWrite(m)
	return m
}

func NewResultRows(ctx *plan.Context, cols []string) *ResultWriter {
	stepper := NewTaskStepper(ctx, "ResultRowWriter")
	m := &ResultWriter{
		TaskBase: stepper.TaskBase,
		cols:     cols,
	}
	return m
}

func NewResultBuffer(ctx *plan.Context, writeTo *[]datasource.Message) *ResultBuffer {
	m := &ResultBuffer{
		TaskBase: NewTaskBase(ctx, "ResultMemWriter"),
	}
	m.Handler = func(ctx *plan.Context, msg schema.Message) bool {
		*writeTo = append(*writeTo, msg)
		//u.Infof("write to msgs: %v", len(*writeTo))
		return true
	}
	return m
}

func (m *ResultExecWriter) Result() driver.Result {
	return &qlbResult{m.lastInsertId, m.rowsAffected, m.err}
}
func (m *ResultExecWriter) Copy() *ResultExecWriter { return NewResultExecWriter(m.Ctx) }
func (m *ResultExecWriter) Close() error            { return nil }
func (m *ResultWriter) Copy() *ResultWriter         { return NewResultWriter(m.Ctx) }
func (m *ResultWriter) Close() error                { return nil }
func (m *ResultBuffer) Copy() *ResultBuffer         { return NewResultBuffer(m.Ctx, nil) }
func (m *ResultBuffer) Close() error                { return nil }

// Note, this is implementation of the sql/driver Rows() Next() interface
func (m *ResultWriter) Next(dest []driver.Value) error {
	//u.Debugf("resultwriter.Next()")
	select {
	case <-m.SigChan():
		return ErrShuttingDown
	case err := <-m.ErrChan():
		return err
	case msg, ok := <-m.MessageIn():
		if !ok {
			return io.EOF
		}
		if msg == nil {
			//u.Warnf("nil message?")
			return io.EOF
			//return fmt.Errorf("Nil message error?")
		}
		//u.Infof("got msg: T:%T   v:%#v", msg, msg)
		return msgToRow(msg, m.cols, dest)
	}
}

// For ResultWriter, since we are are not paging through messages
//  using this mesage channel, instead using Next() as defined by sql/driver
//  we don't read the input channel, just watch stop channels
func (m *ResultWriter) Run() error {
	defer m.Ctx.Recover()
	defer func() {
		close(m.msgOutCh) // closing output channels is the signal to stop
		//u.Warnf("close taskbase: %v", m.Type())
	}()
	//u.Debugf("start Run() for ResultWriter")
	select {
	case err := <-m.errCh:
		u.Errorf("got error:  %v", err)
		return err
	case <-m.sigCh:
		return nil
	}
	return nil
}

func (m *ResultWriter) Columns() []string {
	return m.cols
}

func resultWrite(m *ResultWriter) MessageHandler {
	out := m.MessageOut()
	return func(ctx *plan.Context, msg schema.Message) bool {

		if msgReader, ok := msg.Body().(expr.ContextReader); ok {
			u.Debugf("got msg in result writer: %#v", msgReader)
		} else {
			u.Errorf("could not convert to message reader: %T", msg.Body())
		}

		select {
		case out <- msg:
			return true
		case <-m.SigChan():
			return false
		}
	}
}

func msgToRow(msg datasource.Message, cols []string, dest []driver.Value) error {

	//u.Debugf("msg? %v  %T \n%p %v", msg, msg, dest, dest)
	switch mt := msg.Body().(type) {
	/*
		case *datasource.ContextUrlValues:
			for i, key := range cols {
				if val, ok := mt.Get(key); ok && !val.Nil() {
					dest[i] = val.Value()
					//u.Infof("key=%v   val=%v", key, val)
				} else {
					u.Warnf("missing value? %v %T %v", key, val.Value(), val.Value())
				}
			}
			//u.Debugf("got msg in row result writer: %#v", mt)

		case *datasource.ContextSimple:
			for i, key := range cols {
				//u.Debugf("key=%v mt = nil? %v", key, mt)
				if val, ok := mt.Get(key); ok && val != nil && !val.Nil() {
					dest[i] = val.Value()
					//u.Infof("key=%v   val=%v", key, val)
				} else if val == nil {
					u.Errorf("could not evaluate? %v  %#v", key, mt)
				} else {
					u.Warnf("missing value? %v %T %v", key, val.Value(), val.Value())
				}
			}
			//u.Debugf("got msg in row result writer: %#v", dest)
	*/
	case *datasource.SqlDriverMessageMap:
		for i, key := range cols {
			//u.Debugf("key=%v mt = nil? %v", key, mt)
			if val, ok := mt.Get(key); ok && val != nil && !val.Nil() {
				dest[i] = val.Value()
				//u.Infof("key=%v   val=%v", key, val)
			} else if val == nil {
				u.Errorf("could not evaluate? %v  %#v", key, mt)
			} else {
				u.Warnf("missing value? %v %T %v", key, val.Value(), val.Value())
			}
		}
		//u.Debugf("got msg in row result writer: %#v", dest)
	default:
		u.Errorf("unknown message type: %T", mt)
	}
	return nil
}
