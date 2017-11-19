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
	} else if len(p.From) == 1 && p.From[0].Proj != nil {
		s.Proj = p.From[0].Proj
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
						m.Proj.AddColumnShort(f.Name, f.ValueType())
					}
				} else {
					if schemaCol, ok := tbl.FieldMap[col.SourceField]; ok {
						if isFinal {
							if col.InFinalProjection() {
								//u.Debugf("in plan final %s", col.As)
								m.Proj.AddColumnShort(col.As, schemaCol.ValueType())
							}
						} else {
							//u.Debugf("not final %s", col.As)
							m.Proj.AddColumnShort(col.As, schemaCol.ValueType())
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

	// u.Debugf("created plan.Proj  *rel.Projection %p", plan.Proj)
	// Not all Execution run-times support schema.  ie, csv files and other "ad-hoc" structures
	// do not have to have pre-defined data in advance, in which case the schema output
	// will not be deterministic on the sql []driver.values

	for _, col := range plan.Stmt.Source.Columns {

		//u.Debugf("col: %v  star?%v", col, col.Star)
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
					plan.Proj.AddColumn(col, schemaCol.ValueType())
				} else {
					//u.Infof("not in final? %#v", col)
				}
			} else {
				plan.Proj.AddColumn(col, schemaCol.ValueType())
			}
			//u.Debugf("projection: %p add col: %v %v", plan.Proj, col.As, schemaCol.Type.String())
		} else if col.Star {
			if plan.Tbl == nil {
				u.Warnf("no table?? %v", plan)
			} else {
				//u.Infof("star cols? %v fields: %v", plan.Tbl.FieldPositions, plan.Tbl.Fields)
				for _, f := range plan.Tbl.Fields {
					//u.Infof("  add col %v  %+v", f.Name, f)
					plan.Proj.AddColumnShort(f.Name, f.ValueType())
				}
			}

		} else {
			if col.Expr != nil && strings.ToLower(col.Expr.String()) == "count(*)" {
				//u.Warnf("count(*) as=%v", col.As)
				plan.Proj.AddColumn(col, value.IntType)
			} else if col.Expr != nil {
				// A column was included in projection that does not exist in source.
				// TODO:  Should we allow sources to have settings that specify wether
				//  we enforce schema validation on parse?  or on execution?  many no-sql stores
				//  this is fine
				switch nt := col.Expr.(type) {
				case *expr.IdentityNode, *expr.StringNode:
					plan.Proj.AddColumnShort(col.As, value.StringType)
				case *expr.NumberNode:
					if nt.IsInt {
						plan.Proj.AddColumnShort(col.As, value.IntType)
					} else {
						plan.Proj.AddColumnShort(col.As, value.NumberType)
					}
				case *expr.FuncNode, *expr.BinaryNode:
					// Probably not string?
					plan.Proj.AddColumnShort(col.As, value.StringType)
				default:
					u.Warnf("schema col not found:  SourceField=%q   vals=%#v", col.SourceField, col)
				}

			} else {
				u.Errorf("schema col not found:  SourceField=%q   vals=%#v", col.SourceField, col)
			}

		}
	}
	//u.Infof("plan.Projection %p  cols: %d", plan.Proj, len(plan.Proj.Columns))
	return nil
}
