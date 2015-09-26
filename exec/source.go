package exec

import (
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	// to ensure this can run in exec engine
	_ TaskRunner = (*Source)(nil)

	// Ensure that our source plan implements Subvisitor
	_ expr.SubVisitor = (*SourcePlan)(nil)
)

type KeyEvaluator func(msg datasource.Message) driver.Value

func NewSourcePlan(sql *expr.SqlSource) *SourcePlan {
	return &SourcePlan{SqlSource: sql}
}

type SourcePlan struct {
	SqlSource *expr.SqlSource
}

func (m *SourcePlan) Accept(sub expr.SubVisitor) (expr.Task, error) {
	u.Debugf("Accept %+v", sub)
	return nil, expr.ErrNotImplemented
}
func (m *SourcePlan) VisitSubselect(stmt *expr.SqlSource) (expr.Task, error) {
	u.Debugf("VisitSubselect %+v", stmt)
	return nil, expr.ErrNotImplemented
}

func (m *SourcePlan) VisitJoin(stmt *expr.SqlSource) (expr.Task, error) {
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
	from    *expr.SqlSource
	source  datasource.Scanner
	JoinKey KeyEvaluator
}

// A scanner to read from data source
func NewSource(from *expr.SqlSource, source datasource.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase("Source"),
		source:   source,
		from:     from,
	}
	return s
}

// A scanner to read from sub-query data source (join, sub-query)
func NewSourceJoin(from *expr.SqlSource, source datasource.Scanner) *Source {
	s := &Source{
		TaskBase: NewTaskBase("SourceJoin"),
		source:   source,
		from:     from,
	}
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

func (m *Source) Run(context *expr.Context) error {
	defer context.Recover()
	defer close(m.msgOutCh)

	scanner, ok := m.source.(datasource.Scanner)
	if !ok {
		return fmt.Errorf("Does not implement Scanner: %T", m.source)
	}
	//u.Debugf("scanner: %T %v", scanner, scanner)
	iter := scanner.CreateIterator(nil)
	//u.Debugf("iter in source: %T  %#v", iter, iter)
	sigChan := m.SigChan()

	for item := iter.Next(); item != nil; item = iter.Next() {

		//u.Infof("In source Scanner iter %#v", item)
		select {
		case <-sigChan:
			return nil
		case m.msgOutCh <- item:
			// continue
		}

	}
	//u.Debugf("leaving source scanner")
	return nil
}

// Scan a data source for rows, feed into runner for join sources
//
//  1) join  SELECT t1.name, t2.salary
//               FROM employee AS t1
//               INNER JOIN info AS t2
//               ON t1.name = t2.name;
//
type JoinMerge struct {
	*TaskBase
	conf        *datasource.RuntimeSchema
	leftStmt    *expr.SqlSource
	rightStmt   *expr.SqlSource
	leftSource  datasource.Scanner
	rightSource datasource.Scanner
	ltask       TaskRunner
	rtask       TaskRunner
	colIndex    map[string]int
}

// A very stupid naive parallel join merge, essentially
//    scanning both sources.
//
//   source1   ->
//                \
//                  --  join  -->
//                /
//   source2   ->
//
func NewJoinNaiveMerge(ltask, rtask TaskRunner, lfrom, rfrom *expr.SqlSource, conf *datasource.RuntimeSchema) (*JoinMerge, error) {

	m := &JoinMerge{
		TaskBase: NewTaskBase("JoinNaiveMerge"),
		colIndex: make(map[string]int),
	}

	m.ltask = ltask
	m.rtask = rtask
	m.leftStmt = lfrom
	m.rightStmt = rfrom

	return m, nil
}

func (m *JoinMerge) Copy() *Source { return &Source{} }

func (m *JoinMerge) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *JoinMerge) Run(context *expr.Context) error {
	defer context.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()

	leftIn := m.ltask.MessageOut()
	rightIn := m.rtask.MessageOut()

	u.Infof("left? %s", m.leftStmt)
	lhNodes := m.leftStmt.JoinNodes()
	rhNodes := m.rightStmt.JoinNodes()

	// Build an index of source to destination column indexing
	for _, col := range m.leftStmt.Source.Columns {
		//u.Debugf("left col:  idx=%d  key=%q as=%q col=%v parentidx=%v", len(m.colIndex), col.Key(), col.As, col.String(), col.ParentIndex)
		m.colIndex[col.Key()] = col.ParentIndex
	}
	for _, col := range m.rightStmt.Source.Columns {
		//u.Debugf("right col:  idx=%d  key=%q as=%q col=%v", len(m.colIndex), col.Key(), col.As, col.String())
		m.colIndex[col.Key()] = col.ParentIndex
	}
	// lcols := m.leftStmt.UnAliasedColumns()
	// rcols := m.rightStmt.UnAliasedColumns()

	//u.Infof("lcols:  %#v for sql %s", lcols, m.leftStmt.Source.String())
	//u.Infof("rcols:  %#v for sql %v", rcols, m.rightStmt.Source.String())
	lh := make(map[string][]datasource.Message)
	rh := make(map[string][]datasource.Message)

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
					if jv, ok := joinValue(nil, lhNodes, msg); ok {
						//u.Debugf("left eval?:%v     %#v", jv, msg.Body())
						lh[jv] = append(lh[jv], msg)
					} else {
						u.Warnf("Could not evaluate? %v msg=%v", lhNodes, msg.Body())
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
				u.Warnf("got quit signal join source 1")
				return
			case msg, ok := <-rightIn:
				if !ok {
					//u.Warnf("NICE, got right shutdown")
					wg.Done()
					return
				} else {
					if jv, ok := joinValue(nil, rhNodes, msg); ok {
						//u.Debugf("right val:%v     %#v", jv, msg.Body())
						rh[jv] = append(rh[jv], msg)
					} else {
						u.Warnf("Could not evaluate? %v msg=%v", rhNodes, msg.Body())
					}
				}
			}

		}
	}()
	wg.Wait()
	//u.Info("leaving source scanner")
	i := uint64(0)
	for keyLeft, valLeft := range lh {
		//u.Debugf("compare:  key:%v  left:%#v  right:%#v  rh: %#v", keyLeft, valLeft, rh[keyLeft], rh)
		if valRight, ok := rh[keyLeft]; ok {
			//u.Debugf("found match?\n\t%d left=%#v\n\t%d right=%#v", len(valLeft), valLeft, len(valRight), valRight)
			msgs := m.mergeValueMessages(valLeft, valRight)
			//u.Debugf("msgsct: %v   msgs:%#v", len(msgs), msgs)
			for _, msg := range msgs {
				//outCh <- datasource.NewUrlValuesMsg(i, msg)
				//u.Debugf("i:%d   msg:%#v", i, msg.Row())
				msg.IdVal = i
				i++
				outCh <- msg
			}
		}
	}
	return nil
}

func joinValue(ctx *expr.Context, nodes []expr.Node, msg datasource.Message) (string, bool) {

	if msg == nil {
		u.Warnf("got nil message?")
	}
	u.Infof("joinValue msg T:%T Body %#v", msg, msg.Body())
	//switch mt := msg.(type) {
	// case *datasource.SqlDriverMessage:
	// 	msgReader := datasource.NewValueContextWrapper(mt, cols)
	// 	vals := make([]string, len(nodes))
	// 	for i, node := range nodes {
	// 		joinVal, ok := vm.Eval(msgReader, node)
	// 		//u.Debugf("msg: %#v", msgReader)
	// 		//u.Debugf("evaluating: ok?%v T:%T result=%v node '%v'", ok, joinVal, joinVal.ToString(), node.String())
	// 		if !ok {
	// 			u.Errorf("could not evaluate: %T %#v   %v", joinVal, joinVal, msg)
	// 			return "", false
	// 		}
	// 		vals[i] = joinVal.ToString()
	// 	}
	// 	return strings.Join(vals, string(byte(0))), true
	//default:
	if msgReader, ok := msg.Body().(expr.ContextReader); ok {
		vals := make([]string, len(nodes))
		for i, node := range nodes {
			joinVal, ok := vm.Eval(msgReader, node)
			//u.Debugf("msg: %#v", msgReader)
			//u.Debugf("evaluating: ok?%v T:%T result=%v node '%v'", ok, joinVal, joinVal.ToString(), node.String())
			if !ok {
				u.Errorf("could not evaluate: %T %#v   %v", joinVal, joinVal, msg)
				return "", false
			}
			vals[i] = joinVal.ToString()
		}
		return strings.Join(vals, string(byte(0))), true
	} else {
		u.Errorf("could not convert to message reader: %T", msg.Body())
	}
	//}

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

func (m *JoinMerge) mergeValueMessages(lmsgs, rmsgs []datasource.Message) []*datasource.SqlDriverMessageMap {
	// m.leftStmt.Columns, m.rightStmt.Columns, nil
	//func mergeValuesMsgs(lmsgs, rmsgs []datasource.Message, lcols, rcols []*expr.Column, cols map[string]*expr.Column) []*datasource.SqlDriverMessageMap {
	out := make([]*datasource.SqlDriverMessageMap, 0)
	//u.Infof("merge values: %v:%v", len(lcols), len(rcols))
	for _, lm := range lmsgs {
		switch lmt := lm.(type) {
		case *datasource.SqlDriverMessage:
			u.Warnf("got sql driver message: %#v", lmt)
			for _, rm := range rmsgs {
				switch rmt := rm.(type) {
				case *datasource.SqlDriverMessage:
					// for k, val := range rmt.Vals {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					// newMsg := datasource.NewSqlDriverMessageMapEmpty()
					// newMsg = reAlias2(newMsg, lmt.Vals, m.leftStmt.Columns)
					// newMsg = reAlias2(newMsg, rmt.Vals, m.rightStmt.Columns)
					vals := make([]driver.Value, len(m.colIndex))
					vals = m.valIndexing(vals, lmt.Vals, m.leftStmt.Source.Columns)
					vals = m.valIndexing(vals, rmt.Vals, m.rightStmt.Source.Columns)
					newMsg := datasource.NewSqlDriverMessageMap(0, vals, m.colIndex)
					//u.Debugf("pre:  left:%#v  right:%#v", lmt.Vals, rmt.Vals)
					//u.Debugf("newMsg:  %#v", newMsg.Row())
					out = append(out, newMsg)
				case *datasource.SqlDriverMessageMap:
					// for k, val := range rmt.Row() {
					// 	u.Debugf("k=%v v=%v", k, val)
					// }
					newMsg := datasource.NewSqlDriverMessageMapEmpty()
					newMsg = reAlias2(newMsg, lmt.Vals, m.leftStmt.Source.Columns)
					newMsg = reAlias2(newMsg, rmt.Values(), m.rightStmt.Source.Columns)
					//u.Debugf("pre:  %#v", lmt.Row())
					//u.Debugf("newMsg:  %#v", newMsg.Row())
					out = append(out, newMsg)
				default:
					u.Warnf("uknown type: %T", rm)
				}
			}
		case *datasource.SqlDriverMessageMap:
			u.Warnf("nice SqlDriverMessageMap: %#v", lmt)
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
					vals = m.valIndexing(vals, lmt.Values(), m.leftStmt.Source.Columns)
					vals = m.valIndexing(vals, rmt.Values(), m.rightStmt.Source.Columns)
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

func (m *JoinMerge) valIndexing(valOut, valSource []driver.Value, cols []*expr.Column) []driver.Value {
	for _, col := range cols {
		if col.ParentIndex >= len(valOut) {
			u.Warnf("not enough values to read col? i=%v len(vals)=%v  %#v", col.ParentIndex, len(valOut), valOut)
			continue
		}
		u.Infof("found: i=%v pi:%v as=%v	val=%v	source:%v", col.Index, col.ParentIndex, col.As, valSource[col.Index], valSource)
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
