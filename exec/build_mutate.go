package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = u.EMPTY
)

func (m *JobBuilder) VisitInsert(stmt *rel.SqlInsert) (rel.Task, rel.VisitStatus, error) {

	u.Debugf("VisitInsert %s", stmt)
	planner := m.TaskMaker.Sequential("insert")

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("%p schema %v", m.Ctx.Schema, err)
		return nil, rel.VisitError, datasource.ErrNotFound
	}

	mutatorSource, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewInsertUpsert(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}

	if upsertDs, isUpsert := conn.(datasource.Upsert); isUpsert {
		insertTask := NewInsertUpsert(m.Ctx, stmt, upsertDs)
		//u.Debugf("adding insert: %#v", insertTask)
		planner.Add(insertTask)
	} else {
		u.Warnf("doesn't implement upsert? %T", conn)
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Upsert or SourceMutation", conn)
	}

	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitUpdate(stmt *rel.SqlUpdate) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	//u.Debugf("VisitUpdate %T  %s\n%#v", stmt, stmt.String(), stmt)
	planner := m.TaskMaker.Sequential("update")

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, rel.VisitError, datasource.ErrNotFound
	}

	mutatorSource, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorSource.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewUpdateUpsert(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := conn.(datasource.Upsert)
	if !hasUpdate {
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", conn)
	}
	task := NewUpdateUpsert(m.Ctx, stmt, updateSource)
	planner.Add(task)
	//u.Debugf("adding update conn %#v", conn)
	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitUpsert(stmt *rel.SqlUpsert) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitUpsert %+v", stmt)
	//u.Debugf("VisitUpsert %T  %s\n%#v", stmt, stmt.String(), stmt)
	planner := m.TaskMaker.Sequential("upsert")

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, rel.VisitError, datasource.ErrNotFound
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
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}
	updateSource, hasUpdate := conn.(datasource.Upsert)
	if !hasUpdate {
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Update or SourceMutation", conn)
	}
	task := NewUpsertUpsert(m.Ctx, stmt, updateSource)
	planner.Add(task)
	//u.Debugf("adding update conn %#v", conn)
	return planner, rel.VisitContinue, nil
}

func (m *JobBuilder) VisitDelete(stmt *rel.SqlDelete) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("VisitDelete %+v", stmt)
	planner := m.TaskMaker.Sequential("delete")

	conn, err := m.Ctx.Schema.Open(stmt.Table)
	if err != nil {
		u.Warnf("error finding table %v", stmt.Table)
		return nil, rel.VisitError, datasource.ErrNotFound
	}

	mutatorConn, hasMutator := conn.(datasource.SourceMutation)
	if hasMutator {
		mutator, err := mutatorConn.CreateMutator(m.Ctx)
		if err != nil {
			u.Errorf("could not create mutator %v", err)
		} else {
			task := NewDelete(m.Ctx, stmt, mutator)
			//u.Infof("adding delete: %#v", task)
			planner.Add(task)
			return planner, rel.VisitContinue, nil
		}
	}
	deletionSource, hasDeletion := conn.(datasource.Deletion)
	if !hasDeletion {
		return nil, rel.VisitError, fmt.Errorf("%T Must Implement Deletion or SourceMutation", conn)
	}
	task := NewDelete(m.Ctx, stmt, deletionSource)
	//u.Infof("adding delete task: %#v", task)
	planner.Add(task)
	return planner, rel.VisitContinue, nil
}
