package plan

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
)

func (m *PlannerDefault) WalkPreparedStatement(p *PreparedStatement) error {
	u.Debugf("VisitPreparedStatement %+v", p.Stmt)
	return ErrNotImplemented
}

func (m *PlannerDefault) WalkSelect(p *Select) error {

	//u.Debugf("VisitSelect %+v", p.Stmt)

	if len(p.Stmt.From) == 0 {
		if p.Stmt.SystemQry() {
			return m.WalkSelectSystemInfo(p)
		}
		return m.WalkLiteralQuery(p)
	}

	needsFinalProject := true
	//planner := m.TaskMaker.Sequential("select")

	if len(p.Stmt.From) == 1 {

		p.Stmt.From[0].Source = p.Stmt // TODO:   move to a Finalize() in query planner
		srcPlan, err := NewSource(m.Ctx, p.Stmt.From[0], true)
		if err != nil {
			return nil
		}

		//task, status, err := m.TaskMaker.SourcePlannerMaker(srcPlan).WalkSourceSelect(srcPlan)
		u.Debugf("planner? %#v", m.Planner)
		status, err := m.Planner.WalkSourceSelect(srcPlan)
		if err != nil {
			return err
		}
		if status != WalkContinue {
			//u.Debugf("subselect visit final returning job.Ctx.Projection: %p", m.Ctx.Projection)
			return nil
		}

	} else {

		var prevTask Task
		var prevSource *Source

		for i, from := range p.Stmt.From {

			// Need to rewrite the From statement to ensure all fields necessary to support
			//  joins, wheres, etc exist but is standalone query
			from.Rewrite(p.Stmt)
			srcPlan, err := NewSource(m.Ctx, from, false)
			if err != nil {
				return nil
			}
			//sourceMaker := m.TaskMaker.SourcePlannerMaker(srcPlan)
			_, err = m.Planner.WalkSourceSelect(srcPlan)
			if err != nil {
				u.Errorf("Could not visitsubselect %v  %s", err, from)
				return err
			}

			// now fold into previous task
			curTask := srcPlan
			if i != 0 {
				from.Seekable = true
				// fold this source into previous
				curMergeTask := NewJoinMerge(prevSource, curTask)
				prevTask.Add(curMergeTask)
			}
			prevTask = curTask
			prevSource = srcPlan
			//u.Debugf("got task: %T", prevSource)
		}
	}

	if p.Stmt.Where != nil {
		switch {
		case p.Stmt.Where.Source != nil:
			// SELECT id from article WHERE id in (select article_id from comments where comment_ct > 50);
			u.Warnf("Found un-supported subquery: %#v", p.Stmt.Where)
			return ErrNotImplemented
		case p.Stmt.Where.Expr != nil:
			p.Add(NewWhere(p.Stmt))
		default:
			u.Warnf("Found un-supported where type: %#v", p.Stmt.Where)
			return fmt.Errorf("Unsupported Where Type")
		}
	}

	if p.Stmt.IsAggQuery() {
		u.Debugf("Adding aggregate/group by? %#v", m.Planner)
		p.Add(NewGroupBy(p.Stmt))
		needsFinalProject = false
	}

	if p.Stmt.Having != nil {
		p.Add(NewHaving(p.Stmt))
	}

	//u.Debugf("needs projection? %v", needsFinalProject)
	if needsFinalProject {
		// Add a Final Projection to choose the columns for results
		//u.Debugf("exec.projection: %p job.proj: %p added  %s", projection, m.Ctx.Projection, stmt.String())
		proj, err := NewProjectionFinal(m.Ctx, p.Stmt)
		if err != nil {
			return err
		}
		p.Add(proj)
	}

	return nil
}

// Build Column Name to Position index for given *source* (from) used to interpret
// positional []driver.Value args, mutate the *from* itself to hold this map
func buildColIndex(colSchema schema.SchemaColumns, sp *Source) error {
	if sp.From.Source == nil {
		u.Errorf("Couldnot build colindex bc no source %#v", sp)
		return nil
	}
	sp.From.BuildColIndex(colSchema.Columns())
	return nil
}

// SourceSelect is a single source select
func (m *PlannerDefault) WalkSourceSelect(p *Source) (WalkStatus, error) {

	if p.From.Source != nil {
		u.Debugf("VisitSubselect from.source = %q", p.From.Source)
	} else {
		u.Debugf("VisitSubselect from=%q", p)
	}

	// All of this is plan info, ie needs JoinKey
	needsJoinKey := false
	if p.From.Source != nil && len(p.From.JoinNodes()) > 0 {
		needsJoinKey = true
	}

	// We need to build a ColIndex of source column/select/projection column
	u.Debugf("datasource? %#v", p.DataSource)
	source, err := p.DataSource.Open(p.From.SourceName())
	if err != nil {
		return WalkError, err
	}
	defer source.Close()

	if schemaCols, ok := source.(schema.SchemaColumns); ok {
		//u.Debugf("schemaCols: %T  ", schemaCols)
		if err = buildColIndex(schemaCols, p); err != nil {
			return WalkError, err
		}
	} else {
		return WalkError, fmt.Errorf("%q Didn't implement schema source: %T", p.From.SourceName(), source)
	}

	if p.From.Source != nil && p.From.Source.Where != nil {
		switch {
		case p.From.Source.Where.Expr != nil:
			p.Add(NewWhere(p.From.Source))
		default:
			u.Warnf("Found un-supported where type: %#v", p.From.Source)
			return WalkError, fmt.Errorf("Unsupported Where clause:  %q", p.From)
		}
	}

	// Add a Non-Final Projection to choose the columns for results
	if !p.Final {
		projection := NewProjectionInProcess(p.From.Source)
		u.Debugf("source projection: %p added  %s", projection, p.From.Source.String())
		p.Add(projection)
	}

	if needsJoinKey {
		joinKey := NewJoinKey(p)
		p.Add(joinKey)
	}

	return WalkContinue, nil
}

// queries for internal schema/variables such as:
//
//    select @@max_allowed_packets
//    select current_user()
//    select connection_id()
//    select timediff(curtime(), utc_time())
//
func (m *PlannerDefault) WalkSelectSystemInfo(p *Select) error {
	return nil
}

// Handle Literal queries such as "SELECT 1, @var;"
func (m *PlannerDefault) WalkLiteralQuery(sp *Select) error {
	//u.Debugf("VisitSelectDatabase %+v", sp.Stmt)
	return nil
}

func (m *PlannerDefault) WalkSelectDatabase(s *Select) error {
	//u.Debugf("VisitSelectDatabase %+v", s.Stmt)
	return nil
}

func (m *PlannerDefault) WalkSysQuery(s *Select) error {
	return nil
}
