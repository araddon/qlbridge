package vm

import (
	"github.com/araddon/qlbridge/ast"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/value"
)

const (
	ItemDefaultChannelSize = 1500
)

// a partial runner for statement evaluation
//  used for sub-components such as index-scan, projections, etc
// type Processor interface {
// 	Start()
// 	Stop()
// 	Name() string
// 	C() <-chan map[string]interface{}
// }

// The runtime engine visitor
type Visitor interface {
	// Source Scanners, iterate data source
	VisitSourceKeyScan(op *SourceKey) (interface{}, error)
	/*
		VisitPrimaryScan(op *PrimaryScan) (interface{}, error)
		VisitParentScan(op *ParentScan) (interface{}, error)
		VisitIndexScan(op *IndexScan) (interface{}, error)
		VisitKeyScan(op *KeyScan) (interface{}, error)
		VisitValueScan(op *ValueScan) (interface{}, error)
		VisitDummyScan(op *DummyScan) (interface{}, error)
		VisitCountScan(op *CountScan) (interface{}, error)
		VisitIntersectScan(op *IntersectScan) (interface{}, error)
		VisitUnionScan(op *UnionScan) (interface{}, error)

		// Fetch
		VisitFetch(op *Fetch) (interface{}, error)

		// Join
		VisitJoin(op *Join) (interface{}, error)

		// Filter
		VisitFilter(op *Filter) (interface{}, error)

		// Group
		VisitInitialGroup(op *InitialGroup) (interface{}, error)
		VisitIntermediateGroup(op *IntermediateGroup) (interface{}, error)
		VisitFinalGroup(op *FinalGroup) (interface{}, error)

		// Project
		VisitInitialProject(op *InitialProject) (interface{}, error)
		VisitFinalProject(op *FinalProject) (interface{}, error)

		// Distinct
		VisitDistinct(op *Distinct) (interface{}, error)

		// Set operators
		VisitUnionAll(op *UnionAll) (interface{}, error)
		VisitIntersectAll(op *IntersectAll) (interface{}, error)
		VisitExceptAll(op *ExceptAll) (interface{}, error)

		// Order
		VisitOrder(op *Order) (interface{}, error)

		// Offset
		VisitOffset(op *Offset) (interface{}, error)
		VisitLimit(op *Limit) (interface{}, error)

		// Insert
		VisitSendInsert(op *SendInsert) (interface{}, error)

		// Insert
		VisitSendUpsert(op *SendUpsert) (interface{}, error)

		// Delete
		VisitSendDelete(op *SendDelete) (interface{}, error)

		// Update
		VisitClone(op *Clone) (interface{}, error)
		VisitSet(op *Set) (interface{}, error)
		VisitUnset(op *Unset) (interface{}, error)
		VisitSendUpdate(op *SendUpdate) (interface{}, error)

		// Merge
		VisitMerge(op *Merge) (interface{}, error)
		VisitAlias(op *Alias) (interface{}, error)

		// Framework
		VisitParallel(op *Parallel) (interface{}, error)
		VisitSequence(op *Sequence) (interface{}, error)
		VisitDiscard(op *Discard) (interface{}, error)
		VisitStream(op *Stream) (interface{}, error)
		VisitCollect(op *Collect) (interface{}, error)
		VisitChannel(op *Channel) (interface{}, error)
	*/

}
type Runner interface {
	ChildChan() QuitChan
}

type PartialRunner interface {
	Accept(visitor Visitor) (interface{}, error)
	Run(context *Context, parent value.Value)
}

type Context struct {
	errRecover interface{}
	id         string
	prefix     string
}

func (m *Context) Recover() {
	if r := recover(); r != nil {
		m.errRecover = r
	}
}

// ExecEngine represents the implementation of a runtime-evaluator
// for resolving data-sources, and the sequential sub-pieces
// of the statement
type ExecEngine struct {
	sources datasource.DataSource // datasources
	stmt    ast.SqlStatement      // original statement
	runners []PartialRunner       // sub
}

type QuitChan chan bool
type ItemChan chan interface{}

type runBase struct {
	itemCh ItemChan
	quitCh QuitChan // notify of quit/stop
	input  PartialRunner
	output PartialRunner
}

func newRunBase() runBase {
	return runBase{
		itemCh: make(ItemChan, ItemDefaultChannelSize),
		quitCh: make(QuitChan, 1),
	}
}
