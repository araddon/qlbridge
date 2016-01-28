package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
)

var _ = u.EMPTY

func NewSource(ctx *Context, src *rel.SqlSource, isFinal bool) (*Source, error) {
	sp := &Source{From: src, Ctx: ctx, Final: isFinal}
	err := sp.load(ctx)
	if err != nil {
		return nil, err
	}
	return sp, nil
}
func NewSourceStaticPlan(ctx *Context) *Source {
	return &Source{Ctx: ctx, Final: true}
}

func (m *Source) Accept(visitor SourceVisitor) (Task, rel.VisitStatus, error) {
	return visitor.VisitSourceSelect(m)
}
func (m *Source) load(ctx *Context) error {

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
	m.Tbl = tbl
	err = projecectionForSourcePlan(m)
	return nil
}
