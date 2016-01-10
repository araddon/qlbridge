package plan

import (
	"github.com/araddon/qlbridge/expr"
)

type Task interface {
	expr.Task
	Children() []Task
	Add(Task) error
}

type ExecutionPlan interface {
	Task
	Sequential(name string) Task
	Parallel(name string) Task
}

type ExecutionPlanner func(*Context) ExecutionPlan

// type Tasks struct {
// 	tasks []Task
// }

// // Add a child Task
// func (m *Tasks) Add(task Task) {
// 	//u.Debugf("add task: %T", task)
// 	m.tasks = append(m.tasks, task)
// }
