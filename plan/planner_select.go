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

	//u.Debugf("VisitSelect ctx:%p  %+v", p.Ctx, p.Stmt)

	needsFinalProject := true

	if len(p.Stmt.From) == 0 {

		return m.WalkLiteralQuery(p)

	} else if len(p.Stmt.From) == 1 {

		p.Stmt.From[0].Source = p.Stmt // TODO:   move to a Finalize() in query planner

		srcPlan, err := NewSource(m.Ctx, p.Stmt.From[0], true)
		if err != nil {
			u.Errorf("no source? %v", err)
			return err
		}
		p.From = append(p.From, srcPlan)
		p.Add(srcPlan)

		err = m.Planner.WalkSourceSelect(srcPlan)
		if err != nil {
			u.Errorf("no source? %v", err)
			return err
		}

		if srcPlan.Complete {
			//u.Debugf("subselect visit final returning source plan: %+v", srcPlan)
			goto finalProjection
		}

	} else {

		var prevSource *Source
		var prevTask Task

		for i, from := range p.Stmt.From {

			// Need to rewrite the From statement to ensure all fields necessary to support
			//  joins, wheres, etc exist but is standalone query
			from.Rewrite(p.Stmt)
			srcPlan, err := NewSource(m.Ctx, from, false)
			if err != nil {
				return nil
			}
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
		//u.Debugf("Adding aggregate/group by? %#v", m.Planner)
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

finalProjection:
	if m.Ctx.Projection == nil {
		//u.Debugf("%p source plan Nil Projection?", p)
		proj, err := NewProjectionFinal(m.Ctx, p)
		if err != nil {
			u.Errorf("projection error? %v", err)
			return err
		}
		//u.Warnf("should i do it?")
		m.Ctx.Projection = proj
	}

	return nil
}

func (m *PlannerDefault) WalkProjectionFinal(p *Select) error {
	// Add a Final Projection to choose the columns for results
	//u.Debugf("projection: %p ctx.Projection: %p added  %s", p, m.Ctx.Projection, p.Stmt.String())
	proj, err := NewProjectionFinal(m.Ctx, p)
	if err != nil {
		return err
	}
	p.Add(proj)
	if m.Ctx.Projection == nil {
		m.Ctx.Projection = proj
	} else {
		// Not entirely sure we should be over-writing the projection?
	}
	return nil
}

// Build Column Name to Position index for given *source* (from) used to interpret
// positional []driver.Value args, mutate the *from* itself to hold this map
func buildColIndex(colSchema schema.ConnColumns, p *Source) error {
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
		//u.Debugf("%p VisitSubselect from.source = %q", p, p.Stmt.Source)
	} else {
		//u.Debugf("%p VisitSubselect from=%q", p, p)
	}

	// All of this is plan info, ie needs JoinKey
	needsJoinKey := false
	if p.Stmt.Source != nil && len(p.Stmt.JoinNodes()) > 0 {
		needsJoinKey = true
	}

	// We need to build a ColIndex of source column/select/projection column
	//u.Debugf("datasource? %#v", p.Conn)
	if p.Conn == nil {
		err := p.LoadConn()
		if err != nil {
			u.Errorf("no conn? %v", err)
			return err
		}
		if p.Conn == nil {
			if p.Stmt != nil {
				if p.Stmt.IsLiteral() {
					// this is fine
				} else {
					u.Warnf("no data source and not literal query? %s", p.Stmt.String())
					return ErrNoDataSource
				}
			} else {
				u.Warnf("hm  no conn, no stmt?....")
				return ErrNoDataSource
			}
		}
	}

	if sourcePlanner, hasSourcePlanner := p.Conn.(SourcePlanner); hasSourcePlanner {
		// Can do our own planning
		t, err := sourcePlanner.WalkSourceSelect(m.Planner, p)
		if err != nil {
			return err
		}
		if t != nil {
			p.Add(t)
		}

	} else {

		if schemaCols, ok := p.Conn.(schema.ConnColumns); ok {
			//u.Debugf("schemaCols: %T  %T", schemaCols, p.Conn)
			if err := buildColIndex(schemaCols, p); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%q Didn't implement schema.ConnColumns: %T", p.Stmt.SourceName(), p.Conn)
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
	//u.Debugf("exec.projection: %p job.proj: %p added  %s", p, m.Ctx.Projection, p.Stmt.String())
	proj := NewProjectionInProcess(p.Stmt.Source)
	//u.Debugf("source projection: %p added  %s", proj, p.Stmt.Source.String())
	p.Add(proj)
	m.Ctx.Projection = proj
	return nil
}

// Handle Literal queries such as "SELECT 1, @var;"
func (m *PlannerDefault) WalkLiteralQuery(p *Select) error {
	//u.Debugf("WalkLiteralQuery %+v", p.Stmt)
	// Must project and possibly where

	if p.Stmt.Where != nil {
		u.Warnf("select literal where not implemented")
		// the reason this is wrong is that the Source task gets
		// added in the WalkProjectionFinal below and the Where would need to be in the
		// middle of the Source -> Where -> Projection tasks
	}

	err := m.WalkProjectionFinal(p)

	//u.Debugf("m.Ctx: %p  m.Ctx.Projection.Proj:%p ", m.Ctx, m.Ctx.Projection.Proj)
	if err != nil {
		u.Errorf("error projecting literal? %#v", err)
		return err
	}
	return nil
}
