package exec

import (
	"database/sql/driver"
	"io"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

const (
	MaxAllowedPacket = 1024 * 1024
)

var (
	_ = u.EMPTY

	// ensure our resultwriter implements database/sql/driver `driver.Rows`
	_ driver.Rows = (*ResultWriter)(nil)

	// Ensure that we implement the Task Runner interface
	// required for usage as tasks in Executor
	_ TaskRunner = (*ResultExecWriter)(nil)
	_ TaskRunner = (*ResultWriter)(nil)
	_ TaskRunner = (*ResultBuffer)(nil)
)

type (
	// ResultExecWriter for writing tasks results
	ResultExecWriter struct {
		*TaskBase
		closed       bool
		err          error
		rowsAffected int64
		lastInsertID int64
	}
	// ResultWriter for writing tasks results
	ResultWriter struct {
		*TaskBase
		closed bool
		cols   []string
	}
	// ResultBuffer for writing tasks results
	ResultBuffer struct {
		*TaskBase
		closed bool
		cols   []string
	}
)

// NewResultExecWriter a result writer for exect task
func NewResultExecWriter(ctx *plan.Context) *ResultExecWriter {
	m := &ResultExecWriter{
		TaskBase: NewTaskBase(ctx),
	}
	m.Handler = func(ctx *plan.Context, msg schema.Message) bool {
		switch mt := msg.(type) {
		case *datasource.SqlDriverMessage:
			if len(mt.Vals) > 1 {
				m.lastInsertID = mt.Vals[0].(int64)
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

// NewResultWriter for a plan
func NewResultWriter(ctx *plan.Context) *ResultWriter {
	m := &ResultWriter{
		TaskBase: NewTaskBase(ctx),
	}
	m.Handler = resultWrite(m)
	return m
}

// NewResultRows a resultwriter
func NewResultRows(ctx *plan.Context, cols []string) *ResultWriter {
	stepper := NewTaskStepper(ctx)
	m := &ResultWriter{
		TaskBase: stepper.TaskBase,
		cols:     cols,
	}
	return m
}

// NewResultBuffer create a result buffer to write temp tasks into results.
func NewResultBuffer(ctx *plan.Context, writeTo *[]schema.Message) *ResultBuffer {
	m := &ResultBuffer{
		TaskBase: NewTaskBase(ctx),
	}
	m.Handler = func(ctx *plan.Context, msg schema.Message) bool {
		*writeTo = append(*writeTo, msg)
		//u.Infof("write to msgs: %v", len(*writeTo))
		return true
	}
	return m
}

// Result of exec task
func (m *ResultExecWriter) Result() driver.Result {
	return &qlbResult{m.lastInsertID, m.rowsAffected, m.err}
}

// Copy exec task
func (m *ResultExecWriter) Copy() *ResultExecWriter { return NewResultExecWriter(m.Ctx) }

// Close exect task
func (m *ResultExecWriter) Close() error {
	//u.Debugf("%p ResultExecWriter.Close()???? already closed?%v", m, m.closed)
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()
	return m.TaskBase.Close()
}

// Copy result writter
func (m *ResultWriter) Copy() *ResultWriter { return NewResultWriter(m.Ctx) }

// Close ResultWriter
func (m *ResultWriter) Close() error {
	//u.Debugf("%p ResultWriter.Close()???? already closed?%v", m, m.closed)
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()
	return m.TaskBase.Close()
}

// Copy the result buffer
func (m *ResultBuffer) Copy() *ResultBuffer { return NewResultBuffer(m.Ctx, nil) }

// Close the ResultBuffer
func (m *ResultBuffer) Close() error {
	//u.Debugf("%p ResultBuffer.Close()???? already closed?%v", m, m.closed)
	m.Lock()
	if m.closed {
		m.Unlock()
		return nil
	}
	m.closed = true
	m.Unlock()
	return m.TaskBase.Close()
}

// Next his is implementation of the sql/driver Rows() Next() interface
func (m *ResultWriter) Next(dest []driver.Value) error {
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
			return io.EOF
		}
		return msgToRow(msg, m.cols, dest)
	}
}

// Run For ResultWriter, since we are are not paging through messages
// using this mesage channel, instead using Next() as defined by sql/driver
// we don't read the input channel, just watch stop channels
func (m *ResultWriter) Run() error {
	defer m.Ctx.Recover()
	defer func() {
		close(m.msgOutCh) // closing output channels is the signal to stop
	}()
	select {
	case err := <-m.errCh:
		u.Errorf("got error:  %v", err)
		return err
	case <-m.sigCh:
		// u.Debugf("%p got resultwriter.Run() sigquit?", m)
		return nil
	}
}

// Columns list of column names
func (m *ResultWriter) Columns() []string {
	return m.cols
}

func resultWrite(m *ResultWriter) MessageHandler {
	out := m.MessageOut()
	return func(ctx *plan.Context, msg schema.Message) bool {

		// if _, ok := msg.Body().(expr.ContextReader); !ok {
		// 	u.Errorf("could not convert to message reader: %T", msg.Body())
		// }

		select {
		case out <- msg:
			return true
		case <-m.SigChan():
			return false
		}
	}
}

func msgToRow(msg schema.Message, cols []string, dest []driver.Value) error {

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
			val, ok := mt.Get(key)
			//u.Debugf("key=%v %T %v", key, val, val)
			if ok && val != nil && !val.Nil() {
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
