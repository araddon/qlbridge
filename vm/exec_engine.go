package vm

import (
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlJob(rtConf *RuntimeConfig, sqlText string) (Tasks, error) {

	stmt, err := expr.ParseSqlVm(sqlText)
	if err != nil {
		return nil, err
	}

	builder := NewJobBuilder(rtConf)
	ex, err := stmt.Accept(builder)

	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, fmt.Errorf("No job runner? %v", sqlText)
	}
	tasks, ok := ex.(Tasks)
	if !ok {
		return nil, fmt.Errorf("expected tasks but got: %T", ex)
	}
	return tasks, nil
}

func RunJob(tasks Tasks) error {

	u.Debugf("in RunJob exec")
	ctx := new(Context)

	var wg sync.WaitGroup

	for i, task := range tasks {
		if i == 0 {
			// we don't setup on this one
		} else {
			// plumbing
			task.MessageInSet(tasks[i-1].MessageOut())
		}
	}

	// start them in reverse order
	for i := len(tasks) - 1; i >= 0; i-- {
		wg.Add(1)
		u.Debugf("taskid: %v  %T", i, tasks[i])
		go func(taskId int) {
			task := tasks[taskId]
			if taskId > 0 {
				task.MessageInSet(tasks[taskId-1].MessageOut())
			}
			task.Run(ctx)
			wg.Done()
		}(i)
	}

	wg.Wait()
	u.Infof("After Wait()")

	return nil
}

type RuntimeConfig struct {
	Sources *datasource.DataSources
}

func NewRuntimeConfig() *RuntimeConfig {
	c := &RuntimeConfig{
		Sources: datasource.DataSourcesRegistry(),
	}

	return c
}

// given from name, find datasource
func (m *RuntimeConfig) DataSource(from string) datasource.DataSource {
	// if  mysql.tablename allow that convention
	u.Debugf("get datasource: %v", from)
	parts := strings.SplitN(from, ".", 2)
	if len(parts) > 1 {
		sourceType := strings.ToLower(parts[0])
		if source := m.Sources.Get(sourceType); source != nil {
			dataSource, err := source.Open(parts[1])
			if err != nil {
				u.Errorf("could not open data source: %v  %v", parts[1], err)
				return nil
			}
			return dataSource
		}
		u.Errorf("Datasource not found: %v", sourceType)
	} else {
		sourceType := strings.ToLower(parts[0])
		if source := m.Sources.Get(sourceType); source != nil {
			return source
		}
		u.Errorf("Datasource not found: %v", sourceType)
	}
	// TODO??
	return nil
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

// A scanner to filter by where clause
type Where struct {
	*TaskBase
	where expr.Node
}

func NewWhere(where expr.Node) *Where {
	s := &Where{
		TaskBase: NewTaskBase(),
		where:    where,
	}
	s.Handler = whereFilter(where, s)
	return s
}

func whereFilter(where expr.Node, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	evaluator := Evaluator(where)
	return func(ctx *Context, msg datasource.Message) bool {
		if msgReader, ok := msg.Body().(datasource.ContextReader); ok {
			whereValue, ok := evaluator(msgReader)
			if !ok {
				u.Errorf("could not evaluate: %v", msg)
				return false
			}
			switch whereVal := whereValue.(type) {
			case value.BoolValue:
				if whereVal == value.BoolValueFalse {
					u.Debugf("Filtering out")
					return true
				}
			}
		} else {
			u.Errorf("could not convert to message reader: %T", msg.Body())
		}

		select {
		case out <- msg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}

type ResultWriter struct {
	*TaskBase
	msgs []datasource.Message
}

func NewResultWriter(writeTo []datasource.Message) *ResultWriter {
	s := &ResultWriter{
		TaskBase: NewTaskBase(),
	}
	s.msgs = writeTo
	return s
}
