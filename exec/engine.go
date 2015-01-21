package exec

import (
	"fmt"
	"strings"
	"sync"

	"database/sql/driver"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	ShuttingDownError = fmt.Errorf("Received Shutdown Signal")
)

// Create a multiple error type
type errList []error

func (e *errList) append(err error) {
	if err != nil {
		*e = append(*e, err)
	}
}

func (e errList) error() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

func (e errList) Error() string {
	a := make([]string, len(e))
	for i, v := range e {
		a[i] = v.Error()
	}
	return strings.Join(a, "\n")
}

func params(args []driver.Value) []interface{} {
	r := make([]interface{}, len(args))
	for i, v := range args {
		r[i] = interface{}(v)
	}
	return r
}

type SqlJob struct {
	Tasks Tasks
	Stmt  expr.SqlStatement
}

func (m *SqlJob) Run() error {
	return RunJob(m.Tasks)
}
func (m *SqlJob) Close() error {
	errs := make(errList, 0)
	for _, task := range m.Tasks {
		if err := task.Close(); err != nil {
			errs.append(err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// The drain is the last out channel, on last task
func (m *SqlJob) DrainChan() MessageChan {
	return m.Tasks[len(m.Tasks)-1].MessageOut()
}

// Create Job made up of sub-tasks in DAG that is the
//  plan for execution of this query/job
func BuildSqlJob(rtConf *RuntimeConfig, connInfo, sqlText string) (*SqlJob, error) {

	stmt, err := expr.ParseSqlVm(sqlText)
	if err != nil {
		return nil, err
	}

	builder := NewJobBuilder(rtConf, connInfo)
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
	return &SqlJob{tasks, stmt}, nil
}

func RunJob(tasks Tasks) error {

	//u.Debugf("in RunJob exec %v", len(tasks))
	ctx := new(Context)

	var wg sync.WaitGroup

	for i, task := range tasks {
		if i == 0 {
			// we don't setup on this one as it is source
		} else {
			// plumbing
			task.MessageInSet(tasks[i-1].MessageOut())
		}
	}

	// start them in reverse order
	for i := len(tasks) - 1; i >= 0; i-- {
		wg.Add(1)
		//u.Debugf("taskid: %v  %T", i, tasks[i])
		go func(taskId int) {
			task := tasks[taskId]
			if taskId > 0 {
				//u.Infof("set message in: %v", taskId)
				task.MessageInSet(tasks[taskId-1].MessageOut())
			}
			task.Run(ctx)
			wg.Done()
		}(i)
	}

	wg.Wait()
	//u.Infof("After Wait()")

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

// given connection info, get datasource
//  @connInfo =    csv:///dev/stdin
//                 mockcsv
//  @from      database name
func (m *RuntimeConfig) DataSource(connInfo, from string) datasource.DataSource {
	// if  mysql.tablename allow that convention
	u.Debugf("get datasource: conn=%v from=%v  ", connInfo, from)
	//parts := strings.SplitN(from, ".", 2)
	sourceType, fileOrDb := "", ""
	if len(connInfo) > 0 {
		switch {
		// case strings.HasPrefix(name, "file://"):
		// 	name = name[len("file://"):]
		case strings.HasPrefix(connInfo, "csv://"):
			sourceType = "csv"
			fileOrDb = connInfo[len("csv://"):]
		case strings.Contains(connInfo, "://"):
			strIdx := strings.Index(connInfo, "://")
			sourceType = connInfo[0:strIdx]
			fileOrDb = connInfo[strIdx+3:]
		default:
			sourceType = connInfo
			fileOrDb = from
		}
	}

	sourceType = strings.ToLower(sourceType)
	u.Debugf("source: %v  db=%v", sourceType, fileOrDb)
	if source := m.Sources.Get(sourceType); source != nil {
		u.Debugf("source: %T", source)
		dataSource, err := source.Open(fileOrDb)
		if err != nil {
			u.Errorf("could not open data source: %v  %v", fileOrDb, err)
			return nil
		}
		return dataSource
	} else {
		u.Errorf("source was not found: %v", sourceType)
	}

	return nil
}

type Context struct {
	errRecover interface{}
	id         string
	prefix     string
}

func (m *Context) Recover() {
	if r := recover(); r != nil {
		u.Errorf("context recover: %v", r)
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
		TaskBase: NewTaskBase("Where"),
		where:    where,
	}
	s.Handler = whereFilter(where, s)
	return s
}

func whereFilter(where expr.Node, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	evaluator := vm.Evaluator(where)
	return func(ctx *Context, msg datasource.Message) bool {
		u.Debugf("got msg in where?:")
		// defer func() {
		// 	if r := recover(); r != nil {
		// 		u.Errorf("crap, %v", r)
		// 	}
		// }()
		if msgReader, ok := msg.Body().(datasource.ContextReader); ok {

			whereValue, ok := evaluator(msgReader)
			u.Infof("evaluating: %v", ok)
			if !ok {
				u.Errorf("could not evaluate: %v", msg)
				return false
			}
			switch whereVal := whereValue.(type) {
			case value.BoolValue:
				if whereVal == value.BoolValueFalse {
					//u.Debugf("Filtering out")
					return true
				} else {
					//u.Debugf("NOT FILTERED OUT")
				}
			default:
				u.Warnf("unknown type? %T", whereVal)
			}
		} else {
			u.Errorf("could not convert to message reader: %T", msg.Body())
		}

		//u.Debug("about to send from where to forward")
		select {
		case out <- msg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}

type Projection struct {
	*TaskBase
	sql *expr.SqlSelect
}

func NewProjection(sqlSelect *expr.SqlSelect) *Projection {
	s := &Projection{
		TaskBase: NewTaskBase("Projection"),
		sql:      sqlSelect,
	}
	s.Handler = projectionEvaluator(sqlSelect, s)
	return s
}

func projectionEvaluator(sql *expr.SqlSelect, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	//evaluator := vm.Evaluator(where)
	return func(ctx *Context, msg datasource.Message) bool {
		defer func() {
			if r := recover(); r != nil {
				u.Errorf("crap, %v", r)
			}
		}()

		var outMsg datasource.Message
		// uv := msg.Body().(url.Values)
		switch mt := msg.Body().(type) {
		case *datasource.ContextUrlValues:
			// readContext := datasource.NewContextUrlValues(uv)
			// use our custom write context for example purposes
			writeContext := datasource.NewContextSimple()
			outMsg = writeContext
			//u.Infof("about to evaluate:  %T", outMsg)
			for _, col := range sql.Columns {
				if col.Guard != nil {
					// TODO:  evaluate if guard
				}
				if col.Star {
					for k, v := range mt.Row() {
						writeContext.Put(&expr.Column{As: k}, nil, v)
					}
				} else {
					//u.Debugf("tree.Root: as?%v %#v", col.As, col.Tree.Root)
					v, ok := vm.Eval(mt, col.Tree.Root)
					//u.Debugf("evaled: ok?%v key=%v  val=%v", ok, col.Key(), v)
					if ok {
						writeContext.Put(col, mt, v)
					}
				}

			}
		}

		//u.Debugf("about to send msg: %T", outMsg)
		select {
		case out <- outMsg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}
