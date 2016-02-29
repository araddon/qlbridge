package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
)

var _ = u.EMPTY

func RewriteShowAsSelect(stmt *rel.SqlShow, ctx *Context) (*rel.SqlSelect, error) {

	raw := strings.ToLower(stmt.Raw)
	u.Debugf("attempting to rewrite %s", raw)
	sel := rel.SqlSelect{}
	switch {
	case strings.ToLower(stmt.ShowType) == "tables" || strings.ToLower(stmt.Identity) == ctx.SchemaName:
		if stmt.Full {
			// SHOW FULL TABLES;    = select name, table_type from tables;
		} else {
			// show tables
			//sel.From = append(sel.From, &rel.SqlSource{Name: "tables"})
			s2, err := rel.ParseSqlSelect("select Table from tables;")
			if err != nil {
				return nil, err
			}
			sel = *s2
		}
		//case stmt.Create && strings.ToLower(stmt.CreateWhat) == "table":
		// SHOW CREATE TABLE
		//case strings.ToLower(stmt.Identity) == "databases":
		// SHOW databases;  ->  select name from databases;
	default:
		u.Warnf("unhandled %s", raw)
		return nil, fmt.Errorf("Unrecognized:   %s", raw)
	}
	if stmt.Like != nil {
		u.Debugf("like? %v", stmt.Like)
		sel.Where = &rel.SqlWhere{Expr: stmt.Like}
	} else if stmt.Where != nil {
		sel.Where = &rel.SqlWhere{Expr: stmt.Where}
	}
	if ctx.Schema == nil {
		u.Warnf("missing schema")
		return nil, fmt.Errorf("Must have schema")
	}
	//originalSchema := ctx.Schema
	ctx.Schema = ctx.Schema.InfoSchema
	if ctx.Schema == nil {
		u.Warnf("WAT?  Still nil info schema?")
	}
	//u.Infof("new info schema: %p replacing: %p", ctx.Schema, originalSchema)
	for _, tbl := range ctx.Schema.Tables() {
		u.Infof("info schema table: %v", tbl)
	}
	return &sel, nil
}
func RewriteDescribeAsSelect(stmt *rel.SqlDescribe, ctx *Context) (*rel.SqlSelect, error) {

	raw := strings.ToLower(stmt.Raw)
	u.Warnf("unhandled %s", raw)
	return nil, fmt.Errorf("Unrecognized:   %s", raw)
}
