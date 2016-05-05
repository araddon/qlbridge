package plan

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

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
		//u.Debugf("col.As=%q  col %#v", col.As, col)
		if col.As == "" {
			proj.AddColumnShort(col.Expr.String(), value.StringType)
		} else {
			proj.AddColumnShort(col.As, value.StringType)
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
				//u.Infof("col %#v", col)
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
						//u.Warnf("schema col not found: final?%v col: %#v", isFinal, col)
						if isFinal {
							if col.InFinalProjection() {
								m.Proj.AddColumnShort(col.As, value.StringType)
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
				//u.Errorf("schema col not found:  vals=%#v", col)
			}

		}
	}

	if len(plan.Proj.Columns) == 0 {
		// see note above, not all sources have schema
		//u.Debugf("plan no columns?   Is star? %v", plan.SqlSource.Source.CountStar())
	}
	return nil
}
