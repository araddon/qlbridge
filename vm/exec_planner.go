package vm

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	//"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the interfaces we expect
	//_ JobRunner = (*JobBuilder)(nil)
)

type JobBuilder struct {
	//datastore       datastore.Datastore
	//systemstore     datastore.Datastore
	conf      *RuntimeConfig
	namespace string
	where     expr.Node // filter sources
	//order       *algebra.Order // Used to collect aggregates from ORDER BY
	distinct bool
	children Tasks
}

func NewJobBuilder(rtConf *RuntimeConfig) *JobBuilder {
	b := JobBuilder{}
	b.conf = rtConf
	return &b
}

func (m *JobBuilder) VisitSelect(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSelect %+v", stmt)

	tasks := make(Tasks, 0)

	// Create our Source Scanner
	source := m.conf.DataSource(stmt.From)
	u.Debugf("source: %T", source)
	in := NewSourceScanner(stmt.From, source)
	tasks = append(tasks, in)
	if stmt.Where != nil {
		where := NewWhere(stmt.Where)
		tasks.Add(where)
	}

	return tasks, nil
}

func (m *JobBuilder) VisitInsert(stmt *expr.SqlInsert) (interface{}, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, nil
}

func (m *JobBuilder) VisitDelete(stmt *expr.SqlDelete) (interface{}, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, nil
}

func (m *JobBuilder) VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, nil
}

func (m *JobBuilder) VisitShow(stmt *expr.SqlShow) (interface{}, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, nil
}

func (m *JobBuilder) VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, nil
}
