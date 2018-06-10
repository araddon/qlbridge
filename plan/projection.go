package plan

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

// NewProjectionStatic create A static projection for literal query.
// IT has already had its column/types defined and doesn't need to use internal
// schema to find it, often internal SHOW/DESCRIBE.
func NewProjectionStatic(proj *rel.Projection) *Projection {
	return &Projection{Proj: proj, PlanBase: NewPlanBase(false)}
}

// NewProjectionFinal project final select columns for result-writing
func NewProjectionFinal(ctx *Context, p *Select) (*Projection, error) {
	s := &Projection{
		P:        p,
		Stmt:     p.Stmt,
		PlanBase: NewPlanBase(false),
		Final:    true,
	}
	var err error
	u.Debugf("NewProjectionFinal")
	if len(p.Stmt.From) == 0 {
		u.Warnf("literal projection")
		err = s.loadLiteralProjection(ctx)
	} else if len(p.From) == 1 && p.From[0].Proj != nil {
		s.Proj = p.From[0].Proj
		u.Warnf("used the projection from From[0] %#v", s.Proj.Columns)
	} else {
		err = s.loadFinal(ctx, true)
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

// NewProjectionInProcess create a projection for a non-final
// projection for source.
func NewProjectionInProcess(stmt *rel.SqlSelect) *Projection {
	s := &Projection{
		Stmt:     stmt,
		PlanBase: NewPlanBase(false),
	}
	return s
}

func (m *Projection) loadLiteralProjection(ctx *Context) error {

	//u.Debugf("creating plan.Projection literal %s", ctx.Stmt.String())
	proj := rel.NewProjection()
	m.Proj = proj
	cols := make([]string, len(m.P.Stmt.Columns))
	row := make([]driver.Value, len(cols))
	for _, col := range m.P.Stmt.Columns {
		if col.Expr == nil {
			return fmt.Errorf("no column info? %#v", col.Expr)
		}
		//u.Debugf("col.As=%q  col.Expr %#v", col.As, col.Expr)
		as := col.As
		if col.As == "" {
			as = col.Expr.String()
		}
		switch et := col.Expr.(type) {
		case *expr.NumberNode:
			// number?
			if et.IsInt {
				proj.AddColumnShort(as, value.IntType, true)
			} else {
				proj.AddColumnShort(as, value.NumberType, true)
			}
		default:
			proj.AddColumnShort(as, value.StringType, true)
		}

	}

	ctx.Projection = NewProjectionStatic(proj)
	sourcePlan := NewSourceStaticPlan(ctx)
	sourcePlan.Static = row
	sourcePlan.Cols = cols
	m.P.Add(sourcePlan)
	return nil
}

func (m *Projection) loadFinal(ctx *Context, isFinal bool) error {

	u.Debugf("creating plan.Projection final %s", m.Stmt.String())

	m.Proj = rel.NewProjection()

	for fromi, from := range m.Stmt.From {

		fromName := strings.ToLower(from.SourceName())
		tbl, err := ctx.Schema.Table(fromName)
		if err != nil {
			u.Errorf("could not get table: %v", err)
			return err
		} else if tbl == nil {
			u.Errorf("unexepcted nil table? %v", from.Name)
			return fmt.Errorf("Table not found %q", from.Name)
		} else {

			//u.Debugf("getting cols? %v   cols=%v", from.ColumnPositions())
			for _, col := range from.Source.Columns {
				//_, right, _ := col.LeftRight()
				u.Infof("%d from:%s col %s", fromi, from.Name, col)
				if col.Star {
					for _, f := range tbl.Fields {
						m.Proj.AddColumnShort(f.Name, f.ValueType(), true)
					}
				} else {
					if schemaCol, ok := tbl.FieldMap[col.SourceField]; ok {
						m.Proj.AddColumnShort(col.As, schemaCol.ValueType(), col.InFinalProjection())
					} else {
						u.Infof("schema col not found: final?%v col: %#v InFinal?%v", isFinal, col, col.InFinalProjection())
						m.Proj.AddColumnShort(col.As, value.StringType, col.InFinalProjection())
					}
				}
			}
		}
	}

	for i, col := range m.Proj.Columns {
		u.Debugf("%d  %#v", i, col)
	}
	return nil
}

func projectionForSourcePlan(plan *Source) error {

	plan.Proj = rel.NewProjection()
	u.Infof("projection. tbl?%v plan.Final?%v  source: %s", plan.Tbl != nil, plan.Final, plan.Stmt.Source)

	// Not all Execution run-times support schema.  ie, csv files and other "ad-hoc" structures
	// do not have to have pre-defined data in advance, in which case the schema output
	// will not be deterministic on the sql []driver.values

	for _, col := range plan.Stmt.Source.Columns {

		u.Debugf("%2d col: %#v  star?%v inFinal?%v", len(plan.Proj.Columns), col, col.Star, col.InFinalProjection())
		if plan.Tbl == nil {
			plan.Proj.AddColumn(col, value.StringType, col.InFinalProjection())

		} else if schemaCol, ok := plan.Tbl.FieldMap[col.SourceField]; ok {

			plan.Proj.AddColumn(col, schemaCol.ValueType(), col.InFinalProjection())

		} else if col.Star {
			if plan.Tbl == nil {
				u.Warnf("no table?? %v", plan)
			} else {
				u.Infof("star cols? %v fields: %v", plan.Tbl.FieldPositions, plan.Tbl.Fields)
				for _, f := range plan.Tbl.Fields {
					//u.Infof("  add col %v  %+v", f.Name, f)
					plan.Proj.AddColumnShort(f.Name, f.ValueType(), true)
				}
			}

		} else {
			u.Warnf("WTF  %#v", plan.Tbl.FieldMap)
			if col.Expr != nil && strings.ToLower(col.Expr.String()) == "count(*)" {
				//u.Warnf("count(*) as=%v", col.As)
				plan.Proj.AddColumn(col, value.IntType, true)
			} else if col.Expr != nil {
				// A column was included in projection that does not exist in source.
				// TODO:  Should we allow sources to have settings that specify wether
				//  we enforce schema validation on parse?  or on execution?  many no-sql stores
				//  this is fine
				switch nt := col.Expr.(type) {
				case *expr.IdentityNode, *expr.StringNode:
					plan.Proj.AddColumnShort(col.As, value.StringType, col.InFinalProjection())
				case *expr.NumberNode:
					if nt.IsInt {
						plan.Proj.AddColumnShort(col.As, value.IntType, col.InFinalProjection())
					} else {
						plan.Proj.AddColumnShort(col.As, value.NumberType, col.InFinalProjection())
					}
				case *expr.FuncNode, *expr.BinaryNode:
					// Probably not string?
					plan.Proj.AddColumnShort(col.As, value.StringType, col.InFinalProjection())
				default:
					u.Warnf("schema col not found:  SourceField=%q   vals=%#v", col.SourceField, col)
				}

			} else {
				u.Errorf("schema col not found:  SourceField=%q   vals=%#v", col.SourceField, col)
			}

		}
	}
	for _, c := range plan.Proj.Columns {
		u.Debugf("col %+v", c)
	}
	//u.Infof("plan.Projection %p  cols: %d", plan.Proj, len(plan.Proj.Columns))
	return nil
}
