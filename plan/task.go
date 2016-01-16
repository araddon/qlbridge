package plan

import (
	"github.com/araddon/qlbridge/rel"
)

// Plan Tasks are inherently DAG's of task's implementing
//  a rel.Task interface
type Task interface {
	rel.Task          // rel.Task{ Run(), Close()} ie runnable
	Children() []Task // children sub-tasks
	Add(Task) error   // Add a child to this dag
}

// an execution Plan
type ExecutionPlan interface {
	Task
	Sequential(name string) Task
	Parallel(name string) Task
}

type ExecutionPlanner func(*Context) ExecutionPlan
