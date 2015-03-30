package exec

import (
//"github.com/araddon/qlbridge/expr"
)

// exec.Visitor defines standard Sql Visit() pattern to create
//   a job plan from sql statements
//
// An implementation of Visitor() will be be able to execute/run a Statement
//  - inproc:   ie, in process
//  - distributed:  ie, run this job across multiple servers
type Visitor interface {
	VisitScan(v interface{}) (interface{}, error)
}
