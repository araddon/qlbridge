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
	fr.Add("typewriter", &defaultTypeWriter{})
}

type defaultTypeWriter struct{}

// defaultTypeWriter Convert a qlbridge value type to qlbridge value type
func (m *defaultTypeWriter) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 || vals[0] == nil || vals[0].Nil() {
		return nil, false
	}
	switch sv := vals[0].(type) {
	case value.StringValue:
		return sv, true
	}
	return value.NewStringValue(""), false
}

func (m *defaultTypeWriter) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for typewriter(arg) but got %s", n)
	}
	return m.Eval, nil
}
func (m *defaultTypeWriter) IsAgg() bool           { return false }
func (m *defaultTypeWriter) Type() value.ValueType { return value.StringType }

// RewriteShowAsSelect Rewrite Schema SHOW Statements AS SELECT statements
// so we only need a Select Planner, not separate planner for show statements
func RewriteShowAsSelect(stmt *rel.SqlShow, ctx *Context) (*rel.SqlSelect, error) {

	raw := strings.ToLower(stmt.Raw)
	if ctx.Funcs == nil {
		ctx.Funcs = fr
	}

	showType := strings.ToLower(stmt.ShowType)
	u.Debugf("showType=%q create=%q from=%q rewrite: %s", showType, stmt.CreateWhat, stmt.From, raw)
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
			sqlStatement = "select Table, Table_Type from `schema`.`tables`;"
		} else {
			// show tables;
			sqlStatement = "select Table from `schema`.`tables`;"
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
		sqlStatement = "select Database from schema.databases;"
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
			mysql> show keys from `user` from `mysql`;
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
			| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
			| user  |          0 | PRIMARY  |            1 | Host        | A         |        NULL |     NULL | NULL   |      | BTREE      |         |               |
			| user  |          0 | PRIMARY  |            2 | User        | A         |           3 |     NULL | NULL   |      | BTREE      |         |               |
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+

			mysql> show indexes from `user` from `mysql`;
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
			| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
			| user  |          0 | PRIMARY  |            1 | Host        | A         |        NULL |     NULL | NULL   |      | BTREE      |         |               |
			| user  |          0 | PRIMARY  |            2 | User        | A         |           3 |     NULL | NULL   |      | BTREE      |         |               |
			+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+

		*/
		sqlStatement = fmt.Sprintf("select Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, Sub_part, Packed, `Null`, Index_type, Index_comment from `schema`.`indexes`;")

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

	case "status":
		// Status is a subset of just some variables
		// http://dev.mysql.com/doc/refman/5.7/en/server-status-variables.html

		// SHOW [GLOBAL | SESSION | SLAVE ] STATUS [like_or_where]
		scope := stmt.Scope
		switch scope {
		case "session", "":
			scope = "session"
		}
		sqlStatement = fmt.Sprintf("select Variable_name, Value from `context`.`%s_variables`;", scope)
		/*
			mysql> show global status;
			+--------------------------------+-----------------+
			| Variable_name                  | Value
			+--------------------------------+------------------
			| Aborted_clients                | 0
			| Aborted_connects               | 0
			| Binlog_snapshot_file           |
			| Binlog_snapshot_position       | 0
		*/
	case "engines":
		sqlStatement = fmt.Sprintf("select Engine, Support, Comment, Transactions, XA, Savepoints from `context`.`engines`;")
		/*
			show engines;
			mysql> show engines;
			+--------------------+---------+----------------------------------------------------------------------------+--------------+------+------------+
			| Engine             | Support | Comment                                                                    | Transactions | XA   | Savepoints |
			+--------------------+---------+----------------------------------------------------------------------------+--------------+------+------------+
			| InnoDB             | DEFAULT | Percona-XtraDB, Supports transactions, row-level locking, and foreign keys | YES          | YES  | YES        |
			| CSV                | YES     | CSV storage engine                                                         | NO           | NO   | NO         |
			| MyISAM             | YES     | MyISAM storage engine                                                      | NO           | NO   | NO         |
			| BLACKHOLE          | YES     | /dev/null storage engine (anything you write to it disappears)             | NO           | NO   | NO         |
			| PERFORMANCE_SCHEMA | YES     | Performance Schema                                                         | NO           | NO   | NO         |
			| MEMORY             | YES     | Hash based, stored in memory, useful for temporary tables                  | NO           | NO   | NO         |
			| ARCHIVE            | YES     | Archive storage engine                                                     | NO           | NO   | NO         |
			| MRG_MYISAM         | YES     | Collection of identical MyISAM tables                                      | NO           | NO   | NO         |
			| FEDERATED          | NO      | Federated MySQL storage engine                                             | NULL         | NULL | NULL       |
			+--------------------+---------+----------------------------------------------------------------------------+--------------+------+------------+
		*/
	case "procedure", "function":
		/*
			show procuedure status;
			show function status;

				| Db  | Name | Type | Definer | Modified | Created | Security_type | Comment| character_set_client | collation_connection | Database Collation |
		*/
		sqlStatement = fmt.Sprintf("SELECT Db, Name, Type, Definer, Modified, Created, Security_type, Comment, character_set_client, `collation_connection`, `Database Collation` from `context`.`%ss`;", showType)

	default:
		u.Warnf("unhandled sql rewrite statement %s", raw)
		return nil, fmt.Errorf("Unrecognized:   %s", raw)
	}
	sel, err := rel.ParseSqlSelectResolver(sqlStatement, ctx.Funcs)
	if err != nil {
		u.Errorf("could not reparse %s  err=%v", sqlStatement, err)
		return nil, err
	}
	sel.SetSystemQry()
	if stmt.Like != nil {
		// We are going to ReWrite LIKE clause to WHERE clause
		sel.Where = &rel.SqlWhere{Expr: stmt.Like}
		bn, ok := stmt.Like.(*expr.BinaryNode)
		if ok {
			rhn, ok := bn.Args[1].(*expr.StringNode)
			// See if the Like Clause has wildcard matching, if so
			// our internal vm uses * not %
			if ok && rhn.Text == "%" {
				rhn.Text = strings.Replace(rhn.Text, "%", "*", -1)
			}
		}

	} else if stmt.Where != nil {
		//u.Debugf("add where: %s", stmt.Where)
		sel.Where = &rel.SqlWhere{Expr: stmt.Where}
	}
	if ctx.Schema == nil {
		u.Warnf("missing schema for %s", stmt.Raw)
		return nil, fmt.Errorf("Must have schema")
	}
	if ctx.Schema.InfoSchema == nil {
		u.Warnf("WAT?  Information Schema Nil?")
		return nil, fmt.Errorf("Must have Info schema")
	}

	ctx.Schema = ctx.Schema.InfoSchema
	u.Debugf("SHOW rewrite: %q  ==> %s", stmt.Raw, sel.String())
	return sel, nil
}
func RewriteDescribeAsSelect(stmt *rel.SqlDescribe, ctx *Context) (*rel.SqlSelect, error) {
	s := &rel.SqlShow{ShowType: "columns", Identity: stmt.Identity, Raw: stmt.Raw}
	return RewriteShowAsSelect(s, ctx)
}
