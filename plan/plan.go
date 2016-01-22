package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

type (
	// Sources can often do their own planning for sub-select statements
	//  ie mysql can do its own select, projection mongo can as well
	// - provide interface to allow passing down selection to source
	SourceSelectPlanner interface {
		// given our plan, turn that into a Task.
		// - if VisitStatus is not Final then we need to poly-fill
		VisitSourceSelect(plan *SourcePlan) (rel.Task, rel.VisitStatus, error)
	}
)

type (
	// Within a Select query, it optionally has multiple sources such
	//   as sub-select, join, etc this is the plan for a each source
	SourcePlan struct {
		// Request Information, if distributed must be serialized
		Ctx   *Context        // query context, shared across all parts of this request
		From  *rel.SqlSource  // The sub-query statement (may have been rewritten)
		Proj  *rel.Projection // projection for this sub-query
		Final bool            // Is this final or not?   if sub-query = false, if single from then True

		// Schema and underlying Source provider info, not serialized/transported
		DataSource   schema.DataSource    // The data source for this From
		SourceSchema *schema.SourceSchema // Schema for this source/from
		Tbl          *schema.Table        // Table schema for this From
	}
)

func NewSourcePlan(ctx *Context, src *rel.SqlSource, isFinal bool) (*SourcePlan, error) {
	sp := &SourcePlan{From: src, Ctx: ctx, Final: isFinal}
	err := sp.load(ctx)
	if err != nil {
		return nil, err
	}
	return sp, nil
}
func NewSourceStaticPlan(ctx *Context) *SourcePlan {
	return &SourcePlan{Ctx: ctx, Final: true}
}

func (m *SourcePlan) load(ctx *Context) error {
	//u.Debugf("SourcePlan.load()")
	if m.From == nil {
		// Certain queries don't have from, literal queries, @@session queries etc
		return nil
	}
	fromName := strings.ToLower(m.From.SourceName())
	ss, err := ctx.Schema.Source(fromName)
	if err != nil {
		return err
	}
	if ss == nil {
		u.Warnf("%p Schema %s not found", ctx.Schema, fromName)
		return fmt.Errorf("Could not find source for %v", m.From.SourceName())
	}
	m.SourceSchema = ss
	m.DataSource = ss.DS

	tbl, err := ctx.Schema.Table(fromName)
	if err != nil {
		u.Warnf("%p Schema %v", ctx.Schema, fromName)
		u.Errorf("could not get table: %v", err)
		return err
	}
	// if tbl == nil {
	// 	u.Warnf("wat, no table? %v", fromName)
	// 	return fmt.Errorf("No table found for %s", fromName)
	// }
	m.Tbl = tbl
	//u.Debugf("tbl %#v", tbl)
	err = projecectionForSourcePlan(m)
	return nil
}

var _ = u.EMPTY
