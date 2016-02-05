package exec

import (
	"testing"

	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/plan"
)

func testSerJob(t *testing.T, ctx *plan.Context) *serexec {
	job := NewExecutor(ctx, plan.NewPlanner(ctx))
	se := &serexec{job, t, nil}
	se.Executor = se
	task, err := BuildSqlJobPlanned(job.Planner, job.Executor, ctx)
	assert.T(t, err == nil)
	taskRunner, ok := task.(TaskRunner)
	assert.T(t, ok, "must be taskrunner")
	se.RootTask = taskRunner
	return se
}

type serexec struct {
	*JobExecutor
	t *testing.T
	p *plan.Select
}

func (m *serexec) WalkSelect(p *plan.Select) (Task, error) {
	m.p = p
	return m.JobExecutor.WalkSelect(p)
}

func TestSelectSerialization(t *testing.T) {

	ctx := testContext("SELECT count(*), sum(stuff) FROM orders GROUP BY category")
	job := testSerJob(t, ctx)
	//assert.Tf(t, err == nil, "expected no error but got %v", err)
	assert.T(t, job != nil)
	assert.T(t, job.p != nil)
	pb, err := job.p.Marshal()
	assert.Tf(t, err == nil, "expected no error but got %v", err)
	assert.T(t, len(pb) > 10)
}
