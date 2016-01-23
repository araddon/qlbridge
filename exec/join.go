package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*JoinMerge)(nil)
)

type KeyEvaluator func(msg schema.Message) driver.Value

// Evaluate messages to create JoinKey based message, where the
//    Join Key (composite of each value in join expr) hashes consistently
//
type JoinKey struct {
	*TaskBase
	sp       *plan.SourcePlan
	colIndex map[string]int
}

// A JoinKey task that evaluates the compound JoinKey to allow
//  for parallelized join's
//
//   source1   ->  JoinKey  ->  hash-route
//                                         \
//                                          --  join  -->
//                                         /
//   source2   ->  JoinKey  ->  hash-route
//
func NewJoinKey(sp *plan.SourcePlan) (*JoinKey, error) {
	m := &JoinKey{
		TaskBase: NewTaskBase(sp.Ctx, "JoinKey"),
		colIndex: make(map[string]int),
		sp:       sp,
	}
	return m, nil
}

func (m *JoinKey) Copy() *JoinKey { return &JoinKey{} }

func (m *JoinKey) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *JoinKey) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()
	inCh := m.MessageIn()
	joinNodes := m.sp.From.JoinNodes()

	for {

		select {
		case <-m.SigChan():
			//u.Debugf("got signal quit")
			return nil
		case msg, ok := <-inCh:
			if !ok {
				//u.Debugf("NICE, got msg shutdown")
				return nil
			} else {
				//u.Infof("In joinkey msg %#v", msg)
			msgTypeSwitch:
				switch mt := msg.(type) {
				case *datasource.SqlDriverMessageMap:
					vals := make([]string, len(joinNodes))
					for i, node := range joinNodes {
						joinVal, ok := vm.Eval(mt, node)
						//u.Debugf("evaluating: ok?%v T:%T result=%v node '%v'", ok, joinVal, joinVal.ToString(), node.String())
						if !ok {
							u.Errorf("could not evaluate: %T %#v   %v", joinVal, joinVal, msg)
							break msgTypeSwitch
						}
						vals[i] = joinVal.ToString()
					}
					//u.Infof("joinkey: %v row:%v", vals, mt)
					key := strings.Join(vals, string(byte(0)))
					mt.SetKeyHashed(key)
					outCh <- mt
				default:
					return fmt.Errorf("To use JoinKey must use SqlDriverMessageMap but got %T", msg)
				}
			}
		}
	}
	return nil
}

// Scans 2 source tasks for rows, evaluate keys, use for join
//
type JoinMerge struct {
	*TaskBase
	leftStmt  *rel.SqlSource
	rightStmt *rel.SqlSource
	ltask     TaskRunner
	rtask     TaskRunner
	colIndex  map[string]int
}

// A very stupid naive parallel join merge, uses Key() as value to merge
//   two different input channels
//
//   source1   ->
//                \
//                  --  join  -->
//                /
//   source2   ->
//
// Distributed:
//
//   source1a  ->                |-> --  join  -->
//   source1b  -> key-hash-route |-> --  join  -->  reduce ->
//   source1n  ->                |-> --  join  -->
//                               |-> --  join  -->
//   source2a  ->                |-> --  join  -->
//   source2b  -> key-hash-route |-> --  join  -->
//   source2n  ->                |-> --  join  -->
//
func NewJoinNaiveMerge(ctx *plan.Context, ltask, rtask TaskRunner, lfrom, rfrom *rel.SqlSource) (*JoinMerge, error) {

	m := &JoinMerge{
		TaskBase: NewTaskBase(ctx, "JoinNaiveMerge"),
		colIndex: make(map[string]int),
	}

	m.ltask = ltask
	m.rtask = rtask
	m.leftStmt = lfrom
	m.rightStmt = rfrom

	return m, nil
}

func (m *JoinMerge) Copy() *JoinMerge { return &JoinMerge{} }

func (m *JoinMerge) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

func (m *JoinMerge) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()

	leftIn := m.ltask.MessageOut()
	rightIn := m.rtask.MessageOut()

	//u.Infof("left? %s", m.leftStmt)
	// lhNodes := m.leftStmt.JoinNodes()
	// rhNodes := m.rightStmt.JoinNodes()

	// Build an index of source to destination column indexing
	for _, col := range m.leftStmt.Source.Columns {
		//u.Debugf("left col:  idx=%d  key=%q as=%q col=%v parentidx=%v", len(m.colIndex), col.Key(), col.As, col.String(), col.ParentIndex)
		m.colIndex[m.leftStmt.Alias+"."+col.Key()] = col.ParentIndex
		//u.Debugf("left  colIndex:  %15q : idx:%d sidx:%d pidx:%d", m.leftStmt.Alias+"."+col.Key(), col.Index, col.SourceIndex, col.ParentIndex)
	}
	for _, col := range m.rightStmt.Source.Columns {
		//u.Debugf("right col:  idx=%d  key=%q as=%q col=%v", len(m.colIndex), col.Key(), col.As, col.String())
		m.colIndex[m.rightStmt.Alias+"."+col.Key()] = col.ParentIndex
		//u.Debugf("right colIndex:  %15q : idx:%d sidx:%d pidx:%d", m.rightStmt.Alias+"."+col.Key(), col.Index, col.SourceIndex, col.ParentIndex)
	}

	// lcols := m.leftStmt.Source.AliasedColumns()
	// rcols := m.rightStmt.Source.AliasedColumns()

	//u.Infof("lcols:  %#v for sql %s", lcols, m.leftStmt.Source.String())
	//u.Infof("rcols:  %#v for sql %v", rcols, m.rightStmt.Source.String())
	lh := make(map[string][]*datasource.SqlDriverMessageMap)
	rh := make(map[string][]*datasource.SqlDriverMessageMap)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	var fatalErr error
	go func() {
		for {
			//u.Infof("In source Scanner msg %#v", msg)
			select {
			case <-m.SigChan():
				u.Debugf("got signal quit")
				wg.Done()
				wg.Done()
				return
			case msg, ok := <-leftIn:
				if !ok {
					//u.Debugf("NICE, got left shutdown")
					wg.Done()
					return
				} else {
					switch mt := msg.(type) {
					case *datasource.SqlDriverMessageMap:
						key := mt.Key()
						if key == "" {
							fatalErr = fmt.Errorf(`To use Join msgs must have keys but got "" for %+v`, mt)
							u.Errorf("no key? %#v  %v", mt, fatalErr)
							close(m.TaskBase.sigCh)
							return
						}
						lh[key] = append(lh[key], mt)
					default:
						fatalErr = fmt.Errorf("To use Join must use SqlDriverMessageMap but got %T", msg)
						u.Errorf("unrecognized msg %T", msg)
						close(m.TaskBase.sigCh)
						return
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
				u.Debugf("got quit signal join source 1")
				wg.Done()
				wg.Done()
				return
			case msg, ok := <-rightIn:
				if !ok {
					//u.Debugf("NICE, got right shutdown")
					wg.Done()
					return
				} else {
					switch mt := msg.(type) {
					case *datasource.SqlDriverMessageMap:
						key := mt.Key()
						if key == "" {
							fatalErr = fmt.Errorf(`To use Join msgs must have keys but got "" for %+v`, mt)
							u.Errorf("no key? %#v  %v", mt, fatalErr)
							close(m.TaskBase.sigCh)
							return
						}
						rh[key] = append(rh[key], mt)
					default:
						fatalErr = fmt.Errorf("To use Join must use SqlDriverMessageMap but got %T", msg)
						u.Errorf("unrecognized msg %T", msg)
						close(m.TaskBase.sigCh)
						return
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
				//u.Debugf("i:%d   msg:%#v", i, msg)
				msg.IdVal = i
				i++
				outCh <- msg
			}
		}
	}
	return nil
}

func (m *JoinMerge) mergeValueMessages(lmsgs, rmsgs []*datasource.SqlDriverMessageMap) []*datasource.SqlDriverMessageMap {
	// m.leftStmt.Columns, m.rightStmt.Columns, nil
	//func mergeValuesMsgs(lmsgs, rmsgs []datasource.Message, lcols, rcols []*rel.Column, cols map[string]*rel.Column) []*datasource.SqlDriverMessageMap {
	out := make([]*datasource.SqlDriverMessageMap, 0)
	//u.Infof("merge values: %v:%v", len(lcols), len(rcols))
	for _, lm := range lmsgs {
		//u.Warnf("nice SqlDriverMessageMap: %#v", lmt)
		for _, rm := range rmsgs {
			vals := make([]driver.Value, len(m.colIndex))
			vals = m.valIndexing(vals, lm.Values(), m.leftStmt.Source.Columns)
			vals = m.valIndexing(vals, rm.Values(), m.rightStmt.Source.Columns)
			newMsg := datasource.NewSqlDriverMessageMap(0, vals, m.colIndex)
			//u.Infof("out: %+v", newMsg)
			out = append(out, newMsg)
		}
	}
	return out
}

func (m *JoinMerge) valIndexing(valOut, valSource []driver.Value, cols []*rel.Column) []driver.Value {
	for _, col := range cols {
		if col.ParentIndex < 0 {
			continue
		}
		if col.ParentIndex >= len(valOut) {
			u.Warnf("not enough values to read col? i=%v len(vals)=%v  %#v", col.ParentIndex, len(valOut), valOut)
			continue
		}
		if col.ParentIndex < 0 {
			// Negative parent index means the parent query doesn't use this field, ie used
			// as where, or join key, but not projected
			u.Errorf("negative parentindex? %s", col)
			continue
		}
		if col.Index < 0 || col.Index >= len(valSource) {
			u.Errorf("source index out of range? idx:%v of %d  source: %#v  \n\tcol=%#v", col.Index, len(valSource), valSource, col)
		}
		//u.Infof("found: si=%v pi:%v idx:%d as=%v vals:%v len(out):%v", col.SourceIndex, col.ParentIndex, col.Index, col.As, valSource, len(valOut))
		valOut[col.ParentIndex] = valSource[col.Index]
	}
	return valOut
}

/*

func joinValue(nodes []expr.Node, msg datasource.Message) (string, bool) {

	if msg == nil {
		u.Warnf("got nil message?")
	}
	//u.Infof("joinValue msg T:%T Body %#v", msg, msg.Body())
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
*/
