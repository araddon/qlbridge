package plan

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

var fr = expr.NewFuncRegistry()

func init() {
	fr.Add("typewriter", defaultTypeWriter)
}

// defaultTypeWriter Convert a qlbridge value type to qlbridge value type
//
func defaultTypeWriter(ctx expr.EvalContext, val value.Value) (value.StringValue, bool) {
	switch sv := val.(type) {
	case value.StringValue:
		return sv, true
	}
	return value.NewStringValue(""), false
}

// RewriteShowAsSelect Rewrite Schema SHOW Statements AS SELECT statements
//  so we only need a Select Planner, not separate planner for show statements
func RewriteShowAsSelect(stmt *rel.SqlShow, ctx *Context) (*rel.SqlSelect, error) {

	raw := strings.ToLower(stmt.Raw)
	if ctx.Funcs == nil {
		ctx.Funcs = fr
	}

	showType := strings.ToLower(stmt.ShowType)
	//u.Debugf("showType=%q create=%q from=%q rewrite: %s", showType, stmt.CreateWhat, stmt.From, raw)
	sqlStatement := ""
	from := "tables"
	if stmt.Db != "" {
		from = fmt.Sprintf("%s.%s", stmt.Db, expr.IdentityMaybeQuote('`', from))
	}
	switch showType {
	case "tables":
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
			sqlStatement = fmt.Sprintf("select Table, Table_Type from %s;", from)

		} else {
			// show tables;
			sqlStatement = fmt.Sprintf("select Table from %s;", from)
		}
	case "create":
		// SHOW CREATE {TABLE | DATABASE | EVENT | VIEW }
		switch strings.ToLower(stmt.CreateWhat) {
		case "table":
			sqlStatement = fmt.Sprintf("select Table , mysql_create as `Create Table` FROM `schema`.`%s`", from)
			vn := expr.NewStringNode(stmt.Identity)
			lh := expr.NewIdentityNodeVal("Table")
			stmt.Where = expr.NewBinaryNode(lex.Token{T: lex.TokenEqual, V: "="}, lh, vn)
		default:
			return nil, fmt.Errorf("Unsupported show create %q", stmt.CreateWhat)
		}
	case "databases":
		// SHOW databases;  ->  select Database from databases;
		sqlStatement = "select Database from databases;"
	case "columns":
		if stmt.Full {
			/*
				mysql> show full columns from user;
				+------------------------+-----------------------------------+-----------------+------+-----+-----------------------+-------+---------------------------------+---------+
				| Field                  | Type                              | Collation       | Null | Key | Default               | Extra | Privileges                      | Comment |

			*/
			sqlStatement = fmt.Sprintf("select Field, typewriter(Type) AS Type, Collation, `Null`, Key, Default, Extra, Privileges, Comment from `schema`.`%s`;", stmt.Identity)

		} else {
			/*
				mysql> show columns from user;
				+------------------------+-----------------------------------+------+-----+-----------------------+-------+
				| Field                  | Type                              | Null | Key | Default               | Extra |
				+------------------------+-----------------------------------+------+-----+-----------------------+-------+
			*/
			sqlStatement = fmt.Sprintf("select Field, typewriter(Type) AS Type, `Null`, Key, Default, Extra from `schema`.`%s`;", stmt.Identity)
		}
	case "keys", "indexes", "index":
		/*
			mysql> show keys from user;
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
			| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
			| user  |          0 | PRIMARY  |            1 | Host        | A         |        NULL |     NULL | NULL   |      | BTREE      |         |               |
			| user  |          0 | PRIMARY  |            2 | User        | A         |           7 |     NULL | NULL   |      | BTREE      |         |               |
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		*/
		sqlStatement = fmt.Sprintf("select Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, Sub_part, Packed, `Null`, Index_type, Index_comment from `schema`.`%s`;", stmt.Identity)

	case "variables":
		// SHOW [GLOBAL | SESSION] VARIABLES [like_or_where]
		scope := stmt.Scope
		if scope == "" {
			scope = "session"
		} else {

		}
		sqlStatement = fmt.Sprintf("select Variable_name, Value from `context`.`%s_variables`;", scope)
		/*
		   mysql> show variables LIKE 'version';
		   +---------------+----------+
		   | Variable_name | Value    |
		   +---------------+----------+
		   | version       | 5.7.10-3 |
		   +---------------+----------+
		*/

	default:
		u.Warnf("unhandled %s", raw)
		return nil, fmt.Errorf("Unrecognized:   %s", raw)
	}
	sel, err := rel.ParseSqlSelectResolver(sqlStatement, ctx.Funcs)
	if err != nil {
		return nil, err
	}
	sel.SetSystemQry()
	if stmt.Like != nil {
		sel.Where = &rel.SqlWhere{Expr: stmt.Like}
		bn, ok := stmt.Like.(*expr.BinaryNode)
		if ok {
			rhn, ok := bn.Args[1].(*expr.StringNode)
			if ok && rhn.Text == "%" {
				//sel.Where = nil
				rhn.Text = strings.Replace(rhn.Text, "%", "*", -1)
			}
		}

	} else if stmt.Where != nil {
		//u.Debugf("add where: %s", stmt.Where)
		sel.Where = &rel.SqlWhere{Expr: stmt.Where}
	}
	if ctx.Schema == nil {
		u.Warnf("missing schema")
		return nil, fmt.Errorf("Must have schema")
	}

	ctx.Schema = ctx.Schema.InfoSchema
	if ctx.Schema == nil {
		//u.Warnf("WAT?  Still nil info schema?")
	}
	u.Debugf("SHOW rewrite: %q  ==> %s", stmt.Raw, sel.String())
	return sel, nil
}
func RewriteDescribeAsSelect(stmt *rel.SqlDescribe, ctx *Context) (*rel.SqlSelect, error) {
	s := &rel.SqlShow{ShowType: "columns", Identity: stmt.Identity, Raw: stmt.Raw}
	return RewriteShowAsSelect(s, ctx)
}
