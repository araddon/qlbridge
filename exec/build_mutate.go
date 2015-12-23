package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY
)

func (m *JobBuilder) VisitInsert(stmt *expr.SqlInsert) (expr.Task, expr.VisitStatus, error) {

	u.Debugf("VisitInsert %s", stmt)
	tasks := make(Tasks, 0)

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("%p schema %v", m.Ctx.Schema, err)
		return nil, expr.VisitError, datasource.ErrNotFound
	}

	mutatorSource, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewInsertUpsert(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "insert", tasks), expr.VisitContinue, nil
		}
	}

	if upsertDs, isUpsert := conn.(datasource.Upsert); isUpsert {
		insertTask := NewInsertUpsert(m.Ctx, stmt, upsertDs)
		//u.Debugf("adding insert: %#v", insertTask)
		tasks.Add(insertTask)
	} else {
		u.Warnf("doesn't implement upsert? %T", conn)
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Upsert or SourceMutation", conn)
	}

	return NewSequential(m.Ctx, "insert", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitUpdate(stmt *expr.SqlUpdate) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	//u.Debugf("VisitUpdate %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(Tasks, 0)

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}

	mutatorSource, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpdateUpsert(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := conn.(datasource.Upsert)
	if !hasUpdate {
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", conn)
	}
	task := NewUpdateUpsert(m.Ctx, stmt, updateSource)
	tasks.Add(task)
	//u.Debugf("adding update conn %#v", conn)
	return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitUpsert(stmt *expr.SqlUpsert) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitUpsert %+v", stmt)
	//u.Debugf("VisitUpsert %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(Tasks, 0)

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}

	mutatorConn, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorConn.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpsertUpsert(m.Ctx, stmt, mutator)
			//u.Debugf("adding delete conn %#v", conn)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := conn.(datasource.Upsert)
	if !hasUpdate {
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", conn)
	}
	task := NewUpsertUpsert(m.Ctx, stmt, updateSource)
	tasks.Add(task)
	//u.Debugf("adding update conn %#v", conn)
	return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitDelete(stmt *expr.SqlDelete) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitDelete %+v", stmt)
	tasks := make(Tasks, 0)

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}

	mutatorConn, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorConn.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewDelete(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "delete", tasks), expr.VisitContinue, nil
		}
	}
	deletionSource, hasDeletion := conn.(datasource.Deletion)
	if !hasDeletion {
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Deletion or SourceMutation", conn)
	}
	task := NewDelete(m.Ctx, stmt, deletionSource)
	//u.Infof("adding delete task: %#v", task)
	tasks.Add(task)
	return NewSequential(m.Ctx, "delete", tasks), expr.VisitContinue, nil
}
