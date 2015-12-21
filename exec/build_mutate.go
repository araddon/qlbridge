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
	//u.Debugf("VisitInsert %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(Tasks, 0)

	ds, _ := m.Ctx.Schema.Source(stmt.Table)
	if ds == nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}
	source := ds.DS

	mutatorSource, hasMutator := source.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(stmt)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewInsertUpsert(m.Ctx, stmt, mutator)
			//u.Debugf("adding delete source %#v", source)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "insert", tasks), expr.VisitContinue, nil
		}
	}

	if upsertDs, isUpsert := source.(datasource.Upsert); isUpsert {
		//upsertDs := ds.DataSource.(datasource.Upsert)
		insertTask := NewInsertUpsert(m.Ctx, stmt, upsertDs)
		u.Debugf("adding insert source %#v", upsertDs)
		u.Debugf("adding insert: %#v", insertTask)
		tasks.Add(insertTask)
	} else {
		u.Warnf("doesn't implement upsert? %T", source)
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Upsert or SourceMutation", source)
	}

	return NewSequential(m.Ctx, "insert", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitUpdate(stmt *expr.SqlUpdate) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	//u.Debugf("VisitUpdate %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(Tasks, 0)

	ds, _ := m.Ctx.Schema.Source(stmt.Table)
	if ds == nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}
	source := ds.DS

	mutatorSource, hasMutator := source.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(stmt)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpdateUpsert(m.Ctx, stmt, mutator)
			//u.Debugf("adding delete source %#v", source)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := source.(datasource.Upsert)
	if !hasUpdate {
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", source)
	}
	task := NewUpdateUpsert(m.Ctx, stmt, updateSource)
	tasks.Add(task)
	//u.Debugf("adding update source %#v", source)
	return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitUpsert(stmt *expr.SqlUpsert) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitUpsert %+v", stmt)
	//u.Debugf("VisitUpsert %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(Tasks, 0)

	ds, _ := m.Ctx.Schema.Source(stmt.Table)
	if ds == nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}
	source := ds.DS

	mutatorSource, hasMutator := source.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(stmt)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpsertUpsert(m.Ctx, stmt, mutator)
			//u.Debugf("adding delete source %#v", source)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := source.(datasource.Upsert)
	if !hasUpdate {
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", source)
	}
	task := NewUpsertUpsert(m.Ctx, stmt, updateSource)
	tasks.Add(task)
	//u.Debugf("adding update source %#v", source)
	return NewSequential(m.Ctx, "update", tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitDelete(stmt *expr.SqlDelete) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitDelete %+v", stmt)
	tasks := make(Tasks, 0)

	ds, _ := m.Ctx.Schema.Source(stmt.Table)
	if ds == nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, expr.VisitError, datasource.ErrNotFound
	}
	source := ds.DS

	mutatorSource, hasMutator := source.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(stmt)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewDelete(m.Ctx, stmt, mutator)
			//u.Debugf("adding delete source %#v", source)
			//u.Infof("adding delete: %#v", task)
			tasks.Add(task)
			return NewSequential(m.Ctx, "delete", tasks), expr.VisitContinue, nil
		}
	}
	deletionSource, hasDeletion := source.(datasource.Deletion)
	if !hasDeletion {
		return nil, expr.VisitError, fmt.Errorf("%T Must Implement Deletion or SourceMutation", source)
	}
	task := NewDelete(m.Ctx, stmt, deletionSource)
	//u.Debugf("adding delete source %#v", source)
	//u.Infof("adding delete: %#v", task)
	tasks.Add(task)
	return NewSequential(m.Ctx, "delete", tasks), expr.VisitContinue, nil
}
