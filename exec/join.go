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
	p        *plan.JoinKey
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
func NewJoinKey(ctx *plan.Context, p *plan.JoinKey) *JoinKey {
	m := &JoinKey{
		TaskBase: NewTaskBase(ctx),
		colIndex: make(map[string]int),
		p:        p,
	}
	return m
}

func (m *JoinKey) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()
	inCh := m.MessageIn()
	joinNodes := m.p.Source.Stmt.JoinNodes()

	for {

		select {
		case <-m.SigChan():
			//u.Debugf("got signal quit")
			return nil
		case msg, ok := <-inCh:
			if !ok {
				//u.Debugf("NICE, got msg shutdown")
				return nil
			}

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
func NewJoinNaiveMerge(ctx *plan.Context, l, r TaskRunner, p *plan.JoinMerge) *JoinMerge {

	m := &JoinMerge{
		TaskBase: NewTaskBase(ctx),
		colIndex: p.ColIndex,
	}

	m.ltask = l
	m.rtask = r
	m.leftStmt = p.LeftFrom
	m.rightStmt = p.RightFrom

	return m
}

func (m *JoinMerge) Run() error {
	defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	outCh := m.MessageOut()

	leftIn := m.ltask.MessageOut()
	rightIn := m.rtask.MessageOut()

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
