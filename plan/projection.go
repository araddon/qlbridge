package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Projection struct {
	Sql  *expr.SqlSelect
	Proj *expr.Projection
}

/*
type ProjectionSource struct {
	Source *expr.SqlSource
	Table  *datasource.Table
	Proj   *expr.Projection
}
*/
func NewProjectionStatic(proj *expr.Projection) *Projection {
	return &Projection{Proj: proj}
}

// Final Projections project final select columns for result-writing
func NewProjectionFinal(ctx *Context, sqlSelect *expr.SqlSelect) (*Projection, error) {
	s := &Projection{
		Sql: sqlSelect,
	}
	err := s.loadFinal(ctx, true)
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (m *Projection) loadFinal(ctx *Context, isFinal bool) error {

	if len(m.Sql.From) == 0 {
		return fmt.Errorf("no projection bc no from?")
	}
	//u.Debugf("creating plan.Projection final %s", m.Sql.String())

	m.Proj = expr.NewProjection()

	//m.Sql.Rewrite()

	for _, from := range m.Sql.From {
		//u.Infof("info: %#v", from)
		fromName := strings.ToLower(from.SourceName())
		tbl, err := ctx.Schema.Table(fromName)
		if err != nil {
			u.Errorf("could not get table: %v", err)
			return err
		} else if tbl == nil {
			u.Errorf("no table? %v", from.Name)
			return fmt.Errorf("Table not found %q", from.Name)
		} else {

			//u.Debugf("getting cols? %v   cols=%v", from.ColumnPositions(), len(cols))
			for _, col := range from.Source.Columns {
				//_, right, _ := col.LeftRight()
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
					u.Debugf("schema col not found:  vals=%#v", col)
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
	return nil
}

func projecectionForSourcePlan(plan *SourcePlan) error {

	plan.Proj = expr.NewProjection()

	//u.Debugf("getting cols? %v   cols=%v", from.ColumnPositions(), len(cols))
	for _, col := range plan.Source.Columns {
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
			//
		} else {
			u.Errorf("schema col not found:  vals=%#v", col)
		}
	}

	return nil
}
