package plan

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

func (m *PlannerDefault) WalkPreparedStatement(p *PreparedStatement) error {
	u.Debugf("VisitPreparedStatement %+v", p.Stmt)
	return ErrNotImplemented
}

func (m *PlannerDefault) WalkSelect(p *Select) error {

	u.Debugf("VisitSelect %+v", p.Stmt)

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
		u.Infof("%p srcPlan", srcPlan)
		if err != nil {
			return nil
		}
		p.Add(srcPlan)

		//task, status, err := m.TaskMaker.SourcePlannerMaker(srcPlan).WalkSourceSelect(srcPlan)
		u.Debugf("planner? %#v", m.Planner)
		err = m.Planner.WalkSourceSelect(srcPlan)
		if err != nil {
			return err
		}

	} else {

		var prevSource *Source
		var prevTask Task

		for i, from := range p.Stmt.From {

			/*
				// Need to rewrite the From statement to ensure all fields necessary to support
				//  joins, wheres, etc exist but is standalone query
				from.Rewrite(sp.Stmt)
				srcPlan, err := plan.NewSource(m.Ctx, from, false)
				if err != nil {
					return nil, rel.VisitError, err
				}

				sourceMaker := m.TaskMaker.SourceVisitorMaker(srcPlan)
				sourceTask, status, err := sourceMaker.VisitSourceSelect(srcPlan)
				if err != nil {
					u.Errorf("Could not visitsubselect %v  %s", err, from)
					return nil, status, err
				}

				// now fold into previous task
				curTask := sourceTask.(TaskRunner)
				if i != 0 {
					from.Seekable = true
					curMergeTask := m.TaskMaker.Parallel("select-sources")
					curMergeTask.Add(prevTask)
					curMergeTask.Add(curTask)
					planner.Add(curMergeTask)

					// fold this source into previous
					in, err := NewJoinNaiveMerge(m.Ctx, prevTask, curTask, prevFrom, from)
					if err != nil {
						return nil, rel.VisitError, err
					}
					planner.Add(in)
				}
				prevTask = curTask
				prevFrom = from
				//u.Debugf("got task: %T", prevTask)
			*/

			// Need to rewrite the From statement to ensure all fields necessary to support
			//  joins, wheres, etc exist but is standalone query
			from.Rewrite(p.Stmt)
			srcPlan, err := NewSource(m.Ctx, from, false)
			if err != nil {
				return nil
			}
			//sourceMaker := m.TaskMaker.SourcePlannerMaker(srcPlan)
			err = m.Planner.WalkSourceSelect(srcPlan)
			if err != nil {
				u.Errorf("Could not visitsubselect %v  %s", err, from)
				return err
			}

			// now fold into previous task
			if i != 0 {
				from.Seekable = true
				// fold this source into previous
				curMergeTask := NewJoinMerge(prevTask, srcPlan, prevSource.Stmt, srcPlan.Stmt)
				prevTask = curMergeTask
			} else {
				prevTask = srcPlan
			}
			prevSource = srcPlan
			//u.Debugf("got task: %T", lastSource)
		}
		p.Add(prevTask)

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
		err := m.WalkProjectionFinal(p)
		if err != nil {
			return err
		}
	}
	if m.Ctx.Projection == nil {
		u.Warnf("%p source plan Nil Projection?", p)
		proj, err := NewProjectionFinal(m.Ctx, p.Stmt)
		if err != nil {
			return err
		}
		u.Warnf("should i do it?")
		m.Ctx.Projection = proj
	}

	return nil
}

func (m *PlannerDefault) WalkProjectionFinal(p *Select) error {
	// Add a Final Projection to choose the columns for results
	//u.Debugf("exec.projection: %p job.proj: %p added  %s", projection, m.Ctx.Projection, stmt.String())
	proj, err := NewProjectionFinal(m.Ctx, p.Stmt)
	if err != nil {
		return err
	}
	p.Add(proj)
	//if m.Ctx.Projection == nil {
	//u.Warnf("should i do it?")
	m.Ctx.Projection = proj
	//}
	return nil
}

// Build Column Name to Position index for given *source* (from) used to interpret
// positional []driver.Value args, mutate the *from* itself to hold this map
func buildColIndex(colSchema schema.SchemaColumns, p *Source) error {
	if p.Stmt.Source == nil {
		u.Errorf("Couldnot build colindex bc no source %#v", p)
		return nil
	}
	p.Stmt.BuildColIndex(colSchema.Columns())
	return nil
}

// SourceSelect is a single source select
func (m *PlannerDefault) WalkSourceSelect(p *Source) error {

	if p.Stmt.Source != nil {
		u.Debugf("%p VisitSubselect from.source = %q", p, p.Stmt.Source)
	} else {
		u.Debugf("%p VisitSubselect from=%q", p, p)
	}

	// All of this is plan info, ie needs JoinKey
	needsJoinKey := false
	if p.Stmt.Source != nil && len(p.Stmt.JoinNodes()) > 0 {
		needsJoinKey = true
	}

	// We need to build a ColIndex of source column/select/projection column
	//u.Debugf("datasource? %#v", p.DataSource)
	if p.Conn == nil {
		source, err := p.DataSource.Open(p.Stmt.SourceName())
		if err != nil {
			return err
		}
		p.Conn = source
		//u.Infof("source? %#v", source)
		//defer source.Close()
	}

	if sourcePlanner, hasSourcePlanner := p.Conn.(SourcePlanner); hasSourcePlanner {
		// Can do our own planning
		t, err := sourcePlanner.WalkSourceSelect(m.Planner, p)
		if err != nil {
			return err
		}
		if t != nil {
			u.Debugf("source plan? %#v", t)
			p.Add(t)
		}

	} else {
		if schemaCols, ok := p.Conn.(schema.SchemaColumns); ok {
			//u.Debugf("schemaCols: %T  ", schemaCols)
			if err := buildColIndex(schemaCols, p); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%q Didn't implement schema source: %T", p.Stmt.SourceName(), p.Conn)
		}

		if p.Stmt.Source != nil && p.Stmt.Source.Where != nil {
			switch {
			case p.Stmt.Source.Where.Expr != nil:
				p.Add(NewWhere(p.Stmt.Source))
			default:
				u.Warnf("Found un-supported where type: %#v", p.Stmt.Source)
				return fmt.Errorf("Unsupported Where clause:  %q", p.Stmt)
			}
		}

		// Add a Non-Final Projection to choose the columns for results
		if !p.Final {
			err := m.WalkProjectionSource(p)
			if err != nil {
				return err
			}
		}
	}

	if needsJoinKey {
		joinKey := NewJoinKey(p)
		p.Add(joinKey)
	}

	return nil
}

func (m *PlannerDefault) WalkProjectionSource(p *Source) error {
	// Add a Non-Final Projection to choose the columns for results
	//u.Debugf("exec.projection: %p job.proj: %p added  %s", projection, m.Ctx.Projection, stmt.String())
	proj := NewProjectionInProcess(p.Stmt.Source)
	u.Debugf("source projection: %p added  %s", proj, p.Stmt.Source.String())
	p.Add(proj)
	m.Ctx.Projection = proj
	return nil
}

// queries for internal schema/variables such as:
//
//    select @@max_allowed_packets
//    select current_user()
//    select connection_id()
//    select timediff(curtime(), utc_time())
//
func (m *PlannerDefault) WalkSelectSystemInfo(p *Select) error {
	u.Warnf("WalkSelectSystemInfo %+v", p.Stmt)
	if p.Stmt.IsSysQuery() {
		return m.WalkSysQuery(p)
	} else if len(p.Stmt.From) == 0 && len(p.Stmt.Columns) == 1 && strings.ToLower(p.Stmt.Columns[0].As) == "database" {
		// SELECT database;
		return m.WalkSelectDatabase(p)
	}
	return ErrNotImplemented
}

// Handle Literal queries such as "SELECT 1, @var;"
func (m *PlannerDefault) WalkLiteralQuery(p *Select) error {
	u.Warnf("WalkLiteralQuery %+v", p.Stmt)
	return ErrNotImplemented
}

func (m *PlannerDefault) WalkSelectDatabase(p *Select) error {
	u.Warnf("WalkSelectDatabase %+v", p.Stmt)
	return ErrNotImplemented
}

func (m *PlannerDefault) WalkSysQuery(p *Select) error {
	u.Warnf("WalkSysQuery %+v", p.Stmt)

	//u.Debugf("Ctx.Projection: %#v", m.Ctx.Projection)
	//u.Debugf("Ctx.Projection.Proj: %#v", m.Ctx.Projection.Proj)
	proj := rel.NewProjection()
	cols := make([]string, len(p.Stmt.Columns))
	row := make([]driver.Value, len(cols))
	for i, col := range p.Stmt.Columns {
		if col.Expr == nil {
			return fmt.Errorf("no column info? %#v", col.Expr)
		}
		switch n := col.Expr.(type) {
		case *expr.IdentityNode:
			coln := strings.ToLower(n.Text)
			cols[i] = col.As
			if strings.HasPrefix(coln, "@@") {
				//u.Debugf("m.Ctx? %#v", m.Ctx)
				//u.Debugf("m.Ctx.Session? %#v", m.Ctx.Session)
				val, ok := m.Ctx.Session.Get(coln)
				//u.Debugf("got session var? %v=%#v", col.As, val)
				if ok {
					proj.AddColumnShort(col.As, val.Type())
					row[i] = val.Value()
				} else {
					proj.AddColumnShort(col.As, value.NilType)
				}
			} else {
				u.Infof("columns?  as=%q    rel=%q", col.As, coln)
			}
			// SELECT current_user
		case *expr.FuncNode:
			// SELECT current_user()
			// n.String()
		}
	}

	m.Ctx.Projection = NewProjectionStatic(proj)
	//u.Debugf("%p=plan.projection  rel.Projection=%p", m.Projection, p)
	sourcePlan := NewSourceStaticPlan(m.Ctx)
	p.Add(sourcePlan)
	return nil
}
