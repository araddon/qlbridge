package exec

import (
	"database/sql/driver"
	"fmt"
	"net/url"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	//"github.com/mdmarek/topo"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	// to ensure this can run in exec engine
	_ TaskRunner = (*Source)(nil)

	// Ensure that our source plan implements Subvisitor
	_ expr.SubVisitor = (*SourcePlan)(nil)
)

func NewSourcePlan(sql *expr.SqlSource) *SourcePlan {
	return &SourcePlan{SqlSource: sql}
}

type SourcePlan struct {
	SqlSource *expr.SqlSource
}

func (m *SourcePlan) Accept(sub expr.SubVisitor) (interface{}, error) {
	u.Debugf("Accept %+v", sub)
	return nil, expr.ErrNotImplemented
}
func (m *SourcePlan) VisitSubselect(stmt *expr.SqlSource) (interface{}, error) {
	u.Debugf("VisitSubselect %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *SourcePlan) VisitJoin(stmt *expr.SqlSource) (interface{}, error) {
	u.Debugf("VisitJoin %+v", stmt)
	return nil, expr.ErrNotImplemented
}

// Scan a data source for rows, feed into runner.  The source scanner being
//   a source is iter.Next() messages instead of sending them on input channel
//
//  1) table      -- FROM table
//  2) channels   -- FROM stream
//  3) join       -- SELECT t1.name, t2.salary
//                       FROM employee AS t1
//                       INNER JOIN info AS t2
//                       ON t1.name = t2.name;
//  4) sub-select -- SELECT * FROM (SELECT 1, 2, 3) AS t1;
//
type Source struct {
	*TaskBase
	from   *expr.SqlSource
	source datasource.Scanner
}

// A scanner to read from data source
func NewSource(from *expr.SqlSource, source datasource.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase("Source"),
		source:   source,
		from:     from,
	}
	s.TaskBase.TaskType = s.Type()
	return s
}

func (m *Source) Copy() *Source { return &Source{} }

func (m *Source) Close() error {
	if closer, ok := m.source.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Source) Run(context *Context) error {
	defer context.Recover() // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing input channels is the signal to stop

	// TODO:  Allow an alternate interface that allows Source to provide
	//        an output channel?
	scanner, ok := m.source.(datasource.Scanner)
	if !ok {
		return fmt.Errorf("Does not implement Scanner: %T", m.source)
	}
	//u.Debugf("scanner: %T %v", scanner, scanner)
	iter := scanner.CreateIterator(nil)
	//u.Debugf("iter in source: %T  %#v", iter, iter)

	for item := iter.Next(); item != nil; item = iter.Next() {

		//u.Infof("In source Scanner iter %#v", item)
		select {
		case <-m.SigChan():
			return nil
		case m.msgOutCh <- item:
			// continue
		}

	}
	u.Debugf("leaving source scanner")
	return nil
}

// Scan a data source for rows, feed into runner for join sources
//
//  1) join  SELECT t1.name, t2.salary
//               FROM employee AS t1
//               INNER JOIN info AS t2
//               ON t1.name = t2.name;
//
type SourceJoin struct {
	*TaskBase
	conf        *datasource.RuntimeSchema
	leftStmt    *expr.SqlSource
	rightStmt   *expr.SqlSource
	leftSource  datasource.Scanner
	rightSource datasource.Scanner
	colIndex    map[string]int
}

// A scanner to read from data source
func NewSourceJoin(builder expr.SubVisitor, leftFrom, rightFrom *expr.SqlSource, conf *datasource.RuntimeSchema) (*SourceJoin, error) {

	m := &SourceJoin{
		TaskBase: NewTaskBase("SourceJoin"),
		colIndex: make(map[string]int),
	}
	m.TaskBase.TaskType = m.Type()

	m.leftStmt = leftFrom
	m.rightStmt = rightFrom

	//u.Debugf("leftFrom.Name:'%v' : %v", leftFrom.Name, leftFrom.Source.StringAST())
	source := conf.Conn(leftFrom.Name)
	//u.Debugf("left source: %T", source)
	// Must provider either Scanner, SourcePlanner, Seeker interfaces
	if sourcePlan, ok := source.(datasource.SourcePlanner); ok {
		//  This is flawed, visitor pattern would have you pass in a object which implements interface
		//    but is one of many different objects that implement that interface so that the
		//    Accept() method calls the apppropriate method
		op, err := sourcePlan.Accept(NewSourcePlan(leftFrom))
		// plan := NewSourcePlan(leftFrom)
		// op, err := plan.Accept(sourcePlan)
		if err != nil {
			u.Errorf("Could not source plan for %v  %T %#v", leftFrom.Name, source, source)
		}
		//u.Debugf("got op: %T  %#v", op, op)
		if scanner, ok := op.(datasource.Scanner); !ok {
			u.Errorf("Could not create scanner for %v  %T %#v", leftFrom.Name, op, op)
			return nil, fmt.Errorf("Must Implement Scanner")
		} else {
			m.leftSource = scanner
		}
	} else {
		if scanner, ok := source.(datasource.Scanner); !ok {
			u.Errorf("Could not create scanner for %v  %T %#v", leftFrom.Name, source, source)
			return nil, fmt.Errorf("Must Implement Scanner")
		} else {
			m.leftSource = scanner
			//u.Debugf("got scanner: %T  %#v", scanner, scanner)
		}
	}

	//u.Debugf("right:  Name:'%v' : %v", rightFrom.Name, rightFrom.Source.String())
	source2 := conf.Conn(rightFrom.Name)
	//u.Debugf("source right: %T", source2)
	// Must provider either Scanner, and or Seeker interfaces

	// Must provider either Scanner, SourcePlanner, Seeker interfaces
	if sourcePlan, ok := source2.(datasource.SourcePlanner); ok {
		//  This is flawed, visitor pattern would have you pass in a object which implements interface
		//    but is one of many different objects that implement that interface so that the
		//    Accept() method calls the apppropriate method
		op, err := sourcePlan.Accept(NewSourcePlan(rightFrom))
		// plan := NewSourcePlan(rightFrom)
		// op, err := plan.Accept(sourcePlan)
		if err != nil {
			u.Errorf("Could not source plan for %v  %T %#v", rightFrom.Name, source2, source2)
		}
		//u.Debugf("got op: %T  %#v", op, op)
		if scanner, ok := op.(datasource.Scanner); !ok {
			u.Errorf("Could not create scanner for %v  %T %#v", rightFrom.Name, op, op)
			return nil, fmt.Errorf("Must Implement Scanner")
		} else {
			m.rightSource = scanner
		}
	} else {
		if scanner, ok := source2.(datasource.Scanner); !ok {
			u.Errorf("Could not create scanner for %v  %T %#v", rightFrom.Name, source2, source2)
			return nil, fmt.Errorf("Must Implement Scanner")
		} else {
			m.rightSource = scanner
			//u.Debugf("got scanner: %T  %#v", scanner, scanner)
		}
	}

	return m, nil
}

func (m *SourceJoin) Copy() *Source { return &Source{} }

func (m *SourceJoin) Close() error {
	if closer, ok := m.leftSource.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if closer, ok := m.rightSource.(datasource.DataSource); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *SourceJoin) Run(context *Context) error {
	defer context.Recover() // Our context can recover panics, save error msg
	defer close(m.msgOutCh) // closing input channels is the signal to stop

	//u.Infof("Run():  %T %#v", m.leftSource, m.leftSource)
	leftIn := m.leftSource.MesgChan(nil)
	rightIn := m.rightSource.MesgChan(nil)

	//u.Warnf("leftSource: %p  rightSource: %p", m.leftSource, m.rightSource)
	//u.Warnf("leftIn: %p  rightIn: %p", leftIn, rightIn)
	outCh := m.MessageOut()

	//u.Infof("Checking leftStmt:  %#v", m.leftStmt)
	//u.Infof("Checking rightStmt:  %#v", m.rightStmt)
	lhExpr, err := m.leftStmt.JoinValueExpr()
	if err != nil {
		return err
	}
	rhExpr, err := m.rightStmt.JoinValueExpr()
	if err != nil {
		return err
	}

	// Build an index of source to destination column indexing
	for _, col := range m.leftStmt.Columns {
		//u.Debugf("left col:  idx=%d  key=%q as=%q col=%v parentidx=%v", len(m.colIndex), col.Key(), col.As, col.String(), col.ParentIndex)
		m.colIndex[col.Key()] = col.ParentIndex
	}
	for _, col := range m.rightStmt.Columns {
		//u.Debugf("right col:  idx=%d  key=%q as=%q col=%v", len(m.colIndex), col.Key(), col.As, col.String())
		m.colIndex[col.Key()] = col.ParentIndex
	}
	lcols := m.leftStmt.UnAliasedColumns()
	rcols := m.rightStmt.UnAliasedColumns()

	// TODO:  This needs to be in Planner
	if colScanner, ok := m.leftSource.(datasource.Scanner); ok {
		for i, colName := range colScanner.Columns() {
			for _, col := range lcols {
				if col.SourceField == colName {
					//u.Debugf("found and re-indexing left col: %s  old:%d  new:%d", colName, col.Index, i)
					col.Index = i
					break
				}
			}
		}
	}
	if colScanner, ok := m.rightSource.(datasource.Scanner); ok {
		for i, colName := range colScanner.Columns() {
			for _, col := range rcols {
				if col.SourceField == colName {
					//u.Debugf("found and re-indexing right col: %s  old:%d  new:%d", colName, col.Index, i)
					col.Index = i
					break
				}
			}
		}
	}
	//u.Infof("lcols:  %#v for sql %s", lcols, m.leftStmt.Source.String())
	//u.Infof("rcols:  %#v for sql %v", rcols, m.rightStmt.Source.String())
	lh := make(map[string][]datasource.Message)
	rh := make(map[string][]datasource.Message)
	/*
			JOIN = INNER JOIN = Equal Join

			1)   we need to rewrite query for a source based on the Where + Join? + sort needed
			2)

		TODO:
			x get value for join ON to use in hash,  EvalJoinValues(msg) - this is similar to Projection?
			- manage the coordination of draining both/channels
			- evaluate hashes/output
	*/
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		for {
			//u.Infof("In source Scanner msg %#v", msg)
			select {
			case <-m.SigChan():
				u.Warnf("got signal quit")
				return
			case msg, ok := <-leftIn:
				if !ok {
					//u.Warnf("NICE, got left shutdown")
					wg.Done()
					return
				} else {
					if jv, ok := joinValue(nil, lhExpr, msg, lcols); ok {
						//u.Debugf("left eval?:%v     %#v", jv, msg.Body())
						lh[jv] = append(lh[jv], msg)
					} else {
						u.Warnf("Could not evaluate? %v msg=%v", lhExpr.String(), msg.Body())
					}
				}
			}

		}
	}()
	wg.Add(1)
	go func() {
		for {

			//u.Infof("In source Scanner iter %#v", item)
			select {
			case <-m.SigChan():
				u.Warnf("got signal quit")
				return
			case msg, ok := <-rightIn:
				if !ok {
					//u.Warnf("NICE, got right shutdown")
					wg.Done()
					return
				} else {
					if jv, ok := joinValue(nil, rhExpr, msg, rcols); ok {
						//u.Debugf("right val:%v     %#v", jv, msg.Body())
						rh[jv] = append(rh[jv], msg)
					} else {
						u.Warnf("Could not evaluate? %v msg=%v", rhExpr.String(), msg.Body())
					}
				}
			}

		}
	}()
	wg.Wait()
	//u.Info("leaving source scanner")
	i := uint64(0)
	for keyLeft, valLeft := range lh {
		//u.Infof("compare:  key:%v  left:%#v  right:%#v  rh: %#v", keyLeft, valLeft, rh[keyLeft], rh)
		if valRight, ok := rh[keyLeft]; ok {
			//u.Infof("found match?\n\t%d left=%#v\n\t%d right=%#v", len(valLeft), valLeft, len(valRight), valRight)
			msgs := m.mergeValueMessages(valLeft, valRight)
			//u.Infof("msgsct: %v   msgs:%#v", len(msgs), msgs)
			for _, msg := range msgs {
				//outCh <- datasource.NewUrlValuesMsg(i, msg)
				//u.Infof("i:%d   msg:%#v", i, msg.Row())
				msg.Id = i
				i++
				outCh <- msg
			}
		}
	}
	return nil
}

func joinValue(ctx *Context, node expr.Node, msg datasource.Message, cols map[string]*expr.Column) (string, bool) {

	if msg == nil {
		u.Warnf("got nil message?")
	}
	//u.Infof("got message: %T  %#v", msg, cols)
	switch mt := msg.(type) {
	case *datasource.SqlDriverMessage:
		msgReader := datasource.NewValueContextWrapper(mt, cols)
		joinVal, ok := vm.Eval(msgReader, node)
		//u.Debugf("msg: %#v", msgReader)
		//u.Infof("evaluating: ok?%v T:%T result=%v node '%v'", ok, joinVal, joinVal.ToString(), node.String())
		if !ok {
			u.Errorf("could not evaluate: %T %#v   %v", joinVal, joinVal, msg)
			return "", false
		}
		switch val := joinVal.(type) {
		case value.StringValue:
			return val.Val(), true
		default:
			u.Warnf("unknown type? %T", joinVal)
		}
	default:
		if msgReader, ok := msg.Body().(expr.ContextReader); ok {
			joinVal, ok := vm.Eval(msgReader, node)
			//u.Debugf("msg: %#v", msgReader)
			//u.Infof("evaluating: ok?%v T:%T result=%v node expr:%v", ok, joinVal, joinVal.ToString(), node.StringAST())
			if !ok {
				u.Errorf("could not evaluate: %v", msg)
				return "", false
			}
			switch val := joinVal.(type) {
			case value.StringValue:
				return val.Val(), true
			default:
				u.Warnf("unknown type? %T", joinVal)
			}
		} else {
			u.Errorf("could not convert to message reader: %T", msg.Body())
		}
	}

	return "", false
}

func mergeUvMsgs(lmsgs, rmsgs []datasource.Message, lcols, rcols map[string]*expr.Column) []*datasource.ContextUrlValues {
	out := make([]*datasource.ContextUrlValues, 0)
	for _, lm := range lmsgs {
		switch lmt := lm.Body().(type) {
		case *datasource.ContextUrlValues:
			for _, rm := range rmsgs {
				switch rmt := rm.Body().(type) {
				case *datasource.ContextUrlValues:
					// for k, val := range rmt.Data {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					newMsg := datasource.NewContextUrlValues(url.Values{})
					newMsg = reAlias(newMsg, lmt.Data, lcols)
					newMsg = reAlias(newMsg, rmt.Data, rcols)
					//u.Debugf("pre:  %#v", lmt.Data)
					//u.Debugf("post:  %#v", newMsg.Data)
					out = append(out, newMsg)
				default:
					u.Warnf("uknown type: %T", rm)
				}
			}
		default:
			u.Warnf("uknown type: %T   %T", lmt, lm)
		}
	}
	return out
}

func (m *SourceJoin) mergeValueMessages(lmsgs, rmsgs []datasource.Message) []*datasource.SqlDriverMessageMap {
	// m.leftStmt.Columns, m.rightStmt.Columns, nil
	//func mergeValuesMsgs(lmsgs, rmsgs []datasource.Message, lcols, rcols []*expr.Column, cols map[string]*expr.Column) []*datasource.SqlDriverMessageMap {
	out := make([]*datasource.SqlDriverMessageMap, 0)
	//u.Infof("merge values: %v:%v", len(lcols), len(rcols))
	for _, lm := range lmsgs {
		switch lmt := lm.(type) {
		case *datasource.SqlDriverMessage:
			//u.Warnf("got sql driver message: %#v", lmt)
			for _, rm := range rmsgs {
				switch rmt := rm.(type) {
				case *datasource.SqlDriverMessage:
					// for k, val := range rmt.Vals {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					newMsg := datasource.NewSqlDriverMessageMapEmpty()
					newMsg = reAlias2(newMsg, lmt.Vals, m.leftStmt.Columns)
					newMsg = reAlias2(newMsg, rmt.Vals, m.rightStmt.Columns)
					//u.Debugf("pre:  %#v", lmt.Row())
					//u.Debugf("newMsg:  %#v", newMsg.Row())
					out = append(out, newMsg)
				case *datasource.SqlDriverMessageMap:
					// for k, val := range rmt.Row() {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					newMsg := datasource.NewSqlDriverMessageMapEmpty()
					newMsg = reAlias2(newMsg, lmt.Vals, m.leftStmt.Columns)
					newMsg = reAlias2(newMsg, rmt.Values(), m.rightStmt.Columns)
					//u.Debugf("pre:  %#v", lmt.Row())
					//u.Debugf("newMsg:  %#v", newMsg.Row())
					out = append(out, newMsg)
				default:
					u.Warnf("uknown type: %T", rm)
				}
			}
		case *datasource.SqlDriverMessageMap:
			for _, rm := range rmsgs {
				switch rmt := rm.(type) {
				case *datasource.SqlDriverMessage:
					// for k, val := range rmt.Row() {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					u.Warnf("not implemented")
					//newMsg := datasource.NewSqlDriverMessageMapEmpty()
					//newMsg = m.reAlias(newMsg, lmt.Values(), m.leftStmt.Columns)
					//newMsg = m.reAlias(newMsg, rmt.Values(), m.rightStmt.Columns)
					//u.Debugf("pre:  %#v", lmt.Row())
					//u.Debugf("newMsg:  %#v", newMsg.Row())
					//out = append(out, newMsg)
				case *datasource.SqlDriverMessageMap:
					// for k, val := range rmt.Row() {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					vals := make([]driver.Value, len(m.colIndex))
					vals = m.valIndexing(vals, lmt.Values(), m.leftStmt.Columns)
					vals = m.valIndexing(vals, rmt.Values(), m.rightStmt.Columns)
					newMsg := datasource.NewSqlDriverMessageMap(0, vals, m.colIndex)
					out = append(out, newMsg)
				default:
					u.Warnf("uknown type: %T", rm)
				}
			}
		default:
			u.Warnf("uknown type: %T   %T", lmt, lm)
		}
	}
	return out
}

func (m *SourceJoin) valIndexing(valOut, valSource []driver.Value, cols []*expr.Column) []driver.Value {
	for _, col := range cols {
		if col.ParentIndex >= len(valOut) {
			u.Warnf("not enough values to read col? i=%v len(vals)=%v  %#v", col.ParentIndex, len(valOut), valOut)
			continue
		}
		//u.Infof("found: i=%v as=%v   val=%v", col.Index, col.As, vals[col.Index])
		valOut[col.ParentIndex] = valSource[col.Index]
	}
	return valOut
}
func reAlias2(msg *datasource.SqlDriverMessageMap, vals []driver.Value, cols []*expr.Column) *datasource.SqlDriverMessageMap {

	// for _, col := range cols {
	// 	if col.Index >= len(vals) {
	// 		u.Warnf("not enough values to read col? i=%v len(vals)=%v  %#v", col.Index, len(vals), vals)
	// 		continue
	// 	}
	// 	//u.Infof("found: i=%v as=%v   val=%v", col.Index, col.As, vals[col.Index])
	// 	m.Vals[col.As] = vals[col.Index]
	// }
	msg.SetRow(vals)
	return msg
}

func mergeUv(m1, m2 *datasource.ContextUrlValues) *datasource.ContextUrlValues {
	out := datasource.NewContextUrlValues(m1.Data)
	for k, val := range m2.Data {
		//u.Debugf("k=%v v=%v", k, val)
		out.Data[k] = val
	}
	return out
}
func reAlias(m *datasource.ContextUrlValues, vals url.Values, cols map[string]*expr.Column) *datasource.ContextUrlValues {
	for k, val := range vals {
		if col, ok := cols[k]; !ok {
			u.Warnf("Should not happen? missing %v  ", k)
		} else {
			//u.Infof("found: k=%v as=%v   val=%v", k, col.As, val)
			m.Data[col.As] = val
		}
	}
	return m
}

func reAliasMap(m *datasource.SqlDriverMessageMap, vals map[string]driver.Value, cols []*expr.Column) *datasource.SqlDriverMessageMap {
	row := make([]driver.Value, len(cols))
	for _, col := range cols {
		//u.Infof("found: i=%v as=%v   val=%v", col.Index, col.As, vals[col.Index])
		//m.Vals[col.As] = vals[col.Key()]
		row[col.Index] = vals[col.Key()]
	}
	m.SetRow(row)
	return m
}
