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

// Planner factory creates a task allowing different execution environments
//     do different task run times
//
// - single server channel oriented
// - in process message passing (not channel)
// - multi-server message oriented
// - multi-server file-passing
type TaskPlanner interface {
	// Create a source visitior, aka sub-job-builder
	SourceVisitorMaker(*SourcePlan) rel.SourceVisitor
	Sequential(name string) Task
	Parallel(name string) Task
}
