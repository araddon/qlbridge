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

	sel := rel.SqlSelect{}
	showType := strings.ToLower(stmt.ShowType)
	u.Debugf("%s  attempting to rewrite %s", showType, raw)
	switch {
	// case strings.ToLower(stmt.Identity) == ctx.SchemaName:
	// 	u.Warnf("what?   %s == %s", stmt.Identity, ctx.SchemaName)
	case showType == "tables":
		if stmt.Full {
			// SHOW FULL TABLES;    = select name, table_type from tables;
			// TODO:  note the stupid "_in_mysql", assuming i don't have to implement
			/*
			   mysql> show full tables;
			   +---------------------------+------------+
			   | Tables_in_mysql           | Table_type |
			   +---------------------------+------------+
			   | columns_priv              | BASE TABLE |

			*/
			s2, err := rel.ParseSqlSelect("select Table, Table_Type from tables;")
			if err != nil {
				return nil, err
			}
			sel = *s2
		} else {
			// show tables
			s2, err := rel.ParseSqlSelect("select Table from tables;")
			if err != nil {
				return nil, err
			}
			sel = *s2
		}
		//case stmt.Create && strings.ToLower(stmt.CreateWhat) == "table":
		// SHOW CREATE TABLE
	case showType == "databases":
		// SHOW databases;  ->  select Database from databases;
		s2, err := rel.ParseSqlSelect("select Database from databases;")
		if err != nil {
			u.Warnf("could not parse: %v", err)
			return nil, err
		}
		sel = *s2
	case showType == "variables":
		// SHOW [GLOBAL | SESSION] VARIABLES [like_or_where]
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
	u.Debugf("schema: %T  new stmt: %s", ctx.Schema, sel.String())
	return &sel, nil
}
func RewriteDescribeAsSelect(stmt *rel.SqlDescribe, ctx *Context) (*rel.SqlSelect, error) {

	raw := strings.ToLower(stmt.Raw)
	u.Warnf("unhandled %s", raw)
	return nil, fmt.Errorf("Unrecognized:   %s", raw)
}
