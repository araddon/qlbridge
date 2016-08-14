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

// A static projection has already had its column/types defined
//  and doesn't need to use internal schema to find it, often internal SHOW/DESCRIBE
func NewProjectionStatic(proj *rel.Projection) *Projection {
	return &Projection{Proj: proj, PlanBase: NewPlanBase(false)}
}

// Final Projections project final select columns for result-writing
func NewProjectionFinal(ctx *Context, p *Select) (*Projection, error) {
	s := &Projection{
		P:        p,
		Stmt:     p.Stmt,
		PlanBase: NewPlanBase(false),
		Final:    true,
	}
	var err error
	if len(p.Stmt.From) == 0 {
		err = s.loadLiteralProjection(ctx)
	} else {
		err = s.loadFinal(ctx, true)
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}
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
				proj.AddColumnShort(as, value.IntType)
			} else {
				proj.AddColumnShort(as, value.NumberType)
			}
			//u.Infof("number? %#v", et)
		default:
			//u.Infof("type? %#v", et)
			proj.AddColumnShort(as, value.StringType)
		}

	}

	ctx.Projection = NewProjectionStatic(proj)
	//u.Debugf("cols %#v", proj.Columns)
	//u.Debugf("ctx: %p ctx.Project.Proj: %p", ctx, ctx.Projection.Proj)
	sourcePlan := NewSourceStaticPlan(ctx)
	sourcePlan.Static = row
	sourcePlan.Cols = cols
	m.P.Add(sourcePlan)
	return nil
}

func (m *Projection) loadFinal(ctx *Context, isFinal bool) error {

	//u.Debugf("creating plan.Projection final %s", m.Stmt.String())

	m.Proj = rel.NewProjection()

	for _, from := range m.Stmt.From {

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
				//u.Infof("col %s", col)
				if col.Star {
					for _, f := range tbl.Fields {
						m.Proj.AddColumnShort(f.Name, f.Type)
					}
				} else {
					if schemaCol, ok := tbl.FieldMap[col.SourceField]; ok {
						if isFinal {
							if col.InFinalProjection() {
								//u.Debugf("in plan final %s", col.As)
								m.Proj.AddColumnShort(col.As, schemaCol.Type)
							}
						} else {
							//u.Debugf("not final %s", col.As)
							m.Proj.AddColumnShort(col.As, schemaCol.Type)
						}
						//u.Debugf("projection: %p add col: %v %v", m.Proj, col.As, schemaCol.Type.String())
					} else {
						//u.Infof("schema col not found: final?%v col: %#v InFinal?%v", isFinal, col, col.InFinalProjection())
						if isFinal {
							if col.InFinalProjection() {
								m.Proj.AddColumnShort(col.As, value.StringType)
							} else {
								u.Warnf("not adding to projection? %s", col)
							}
						} else {
							m.Proj.AddColumnShort(col.As, value.StringType)
						}
					}
				}

			}
		}
	}
	return nil
}

func projectionForSourcePlan(plan *Source) error {

	plan.Proj = rel.NewProjection()

	// Not all Execution run-times support schema.  ie, csv files and other "ad-hoc" structures
	// do not have to have pre-defined data in advance, in which case the schema output
	// will not be deterministic on the sql []driver.values

	for _, col := range plan.Stmt.Source.Columns {
		//_, right, _ := col.LeftRight()
		//u.Debugf("projection final?%v tblnil?%v  col:%s", plan.Final, plan.Tbl == nil, col)
		if plan.Tbl == nil {
			if plan.Final {
				if col.InFinalProjection() {
					plan.Proj.AddColumn(col, value.StringType)
				}
			} else {
				plan.Proj.AddColumn(col, value.StringType)
			}
		} else if schemaCol, ok := plan.Tbl.FieldMap[col.SourceField]; ok {
			if plan.Final {
				if col.InFinalProjection() {
					//u.Infof("col add %v for %s", schemaCol.Type.String(), col)
					plan.Proj.AddColumn(col, schemaCol.Type)
				} else {
					//u.Infof("not in final? %#v", col)
				}
			} else {
				plan.Proj.AddColumn(col, schemaCol.Type)
			}
			//u.Debugf("projection: %p add col: %v %v", plan.Proj, col.As, schemaCol.Type.String())
		} else if col.Star {
			if plan.Tbl == nil {
				u.Warnf("no table?? %v", plan)
			} else {
				for _, f := range plan.Tbl.Fields {
					//u.Infof("%d  add col %v  %+v", i, f.Name, f)
					plan.Proj.AddColumnShort(f.Name, f.Type)
				}
			}

		} else {
			if col.Expr != nil && strings.ToLower(col.Expr.String()) == "count(*)" {
				//u.Warnf("count(*) as=%v", col.As)
				plan.Proj.AddColumn(col, value.IntType)
			} else {
				// A column was included in projection that does not exist in source.
				// TODO:  Should we allow sources to have settings that specify wether
				//  we enforce schema validation on parse?  or on execution?  many no-sql stores
				//  this is fine
				u.Warnf("schema col not found:  SourceField=%q   vals=%#v", col.SourceField, col)
				plan.Proj.AddColumnShort(col.As, value.StringType)
			}

		}
	}

	if len(plan.Proj.Columns) == 0 {
		// see note above, not all sources have schema
		//u.Debugf("plan no columns?   Is star? %v", plan.SqlSource.Source.CountStar())
	}
	return nil
}
