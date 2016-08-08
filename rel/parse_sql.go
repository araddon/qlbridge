package rel

import (
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	SqlKeywords = []string{"select", "insert", "update", "delete", "from", "where", "as", "into", "limit",
		"exists", "in", "contains", "include", "not", "and", "having", "or", "null", "group", "order"}
)

// ParseSql Parses SqlStatement and returns a statement or error
//  - does not parse more than one statement
func ParseSql(sqlQuery string) (SqlStatement, error) {
	return parseSqlResolver(sqlQuery, nil)
}
func parseSqlResolver(sqlQuery string, fr expr.FuncResolver) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	m := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), funcs: fr, buildVm: false}
	return m.parse()
}
func ParseSqlSelect(sqlQuery string) (*SqlSelect, error) {
	stmt, err := ParseSql(sqlQuery)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*SqlSelect)
	if !ok {
		return nil, fmt.Errorf("Expected SqlSelect but got %T", stmt)
	}
	return sel, nil
}
func ParseSqlSelectResolver(sqlQuery string, fr expr.FuncResolver) (*SqlSelect, error) {
	stmt, err := parseSqlResolver(sqlQuery, fr)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*SqlSelect)
	if !ok {
		return nil, fmt.Errorf("Expected SqlSelect but got %T", stmt)
	}
	return sel, nil
}
func ParseSqlVm(sqlQuery string) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	m := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), buildVm: true}
	return m.parse()
}
func ParseSqlStatements(sqlQuery string) ([]SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	m := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), buildVm: false}
	stmts := make([]SqlStatement, 0)
	for {
		stmt, err := m.parse()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, stmt)
		sqlRemaining, hasMore := l.Remainder()
		if !hasMore {
			break
		}
		l = lex.NewSqlLexer(sqlRemaining)
		m = Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), buildVm: false}
	}
	return stmts, nil
}

// Sqlbridge generic SQL parser evaluates should be sufficient for most
//  sql compatible languages
type Sqlbridge struct {
	buildVm bool
	l       *lex.Lexer
	comment string
	*SqlTokenPager
	firstToken lex.Token
	funcs      expr.FuncResolver
}

// parse the request
func (m *Sqlbridge) parse() (SqlStatement, error) {
	m.comment = m.initialComment()
	m.firstToken = m.Cur()
	//u.Infof("firsttoken: %v", m.firstToken)
	switch m.firstToken.T {
	case lex.TokenPrepare:
		return m.parsePrepare()
	case lex.TokenSelect:
		return m.parseSqlSelect()
	case lex.TokenInsert, lex.TokenReplace:
		return m.parseSqlInsert()
	case lex.TokenUpdate:
		return m.parseSqlUpdate()
	case lex.TokenUpsert:
		return m.parseSqlUpsert()
	case lex.TokenDelete:
		return m.parseSqlDelete()
	case lex.TokenShow:
		//u.Infof("parse show: %v", m.l.RawInput())
		return m.parseShow()
	case lex.TokenExplain, lex.TokenDescribe, lex.TokenDesc:
		return m.parseDescribe()
	case lex.TokenSet, lex.TokenUse:
		return m.parseCommand()
	case lex.TokenRollback, lex.TokenCommit:
		return m.parseTransaction()
	}
	u.Warnf("Could not parse?  %v   peek=%v", m.l.RawInput(), m.l.PeekX(40))
	return nil, fmt.Errorf("Unrecognized request type: %v", m.l.PeekWord())
}

func (m *Sqlbridge) initialComment() string {

	comment := ""

	for {
		// We are going to loop until we find the first Non-Comment Token
		switch m.Cur().T {
		case lex.TokenComment, lex.TokenCommentML:
			comment += m.Cur().V
		case lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd, lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
		default:
			// first non-comment token
			return comment
		}
		m.Next()
	}
	return comment
}

func discardComments(m expr.TokenPager) {

	for {
		// We are going to loop until we find the first Non-Comment Token
		switch m.Cur().T {
		case lex.TokenComment, lex.TokenCommentML,
			lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd,
			lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// discard
			m.Next()
		default:
			// first non-comment token
			return
		}

	}
	panic("unreachable")
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Sqlbridge) parseSqlSelect() (*SqlSelect, error) {

	req := NewSqlSelect()
	req.Raw = m.l.RawInput()
	m.Next() // Consume Select?

	// Optional DISTINCT keyword always immediately after SELECT KW
	if m.Cur().T == lex.TokenDistinct {
		m.Next()
		req.Distinct = true
	}

	// columns
	if err := parseColumns(m, m.funcs, m.buildVm, req); err != nil {
		u.Debug(err)
		return nil, err
	}

	//u.Debugf("cur? %v", m.Cur())
	// select @@myvar limit 1
	if m.Cur().T == lex.TokenLimit {
		if err := m.parseLimit(req); err != nil {
			return req, nil
		}
		if m.isEnd() {
			return req, nil
		}
	}

	// SPECIAL END CASE for simple selects
	// SELECT last_insert_id();
	if m.Cur().T == lex.TokenEOS || m.Cur().T == lex.TokenEOF {
		// valid end
		return req, nil
	}

	// INTO
	discardComments(m)
	if errreq := m.parseInto(req); errreq != nil {
		return nil, errreq
	}

	// FROM
	discardComments(m)
	if errreq := m.parseSources(req); errreq != nil {
		return nil, errreq
	}

	// WHERE
	discardComments(m)
	if errreq := m.parseWhereSelect(req); errreq != nil {
		return nil, errreq
	}

	// GROUP BY
	discardComments(m)
	if errreq := m.parseGroupBy(req); errreq != nil {
		return nil, errreq
	}

	// HAVING
	discardComments(m)
	if errreq := m.parseHaving(req); errreq != nil {
		return nil, errreq
	}

	// ORDER BY
	discardComments(m)
	if errreq := m.parseOrderBy(req); errreq != nil {
		return nil, errreq
	}

	// LIMIT
	discardComments(m)
	if err := m.parseLimit(req); err != nil {
		return nil, err
	}

	// WITH
	discardComments(m)
	with, err := ParseWith(m.SqlTokenPager)
	if err != nil {
		return nil, err
	}
	req.With = with

	// ALIAS
	discardComments(m)
	if err := m.parseAlias(req); err != nil {
		return nil, err
	}

	if m.Cur().T == lex.TokenEOF || m.Cur().T == lex.TokenEOS || m.Cur().T == lex.TokenRightParenthesis {

		if err := req.Finalize(); err != nil {
			u.Errorf("Could not finalize: %v", err)
			return nil, err
		}

		// we are good
		return req, nil
	}

	u.Warnf("Could not complete parsing, return error: %v %v", m.Cur(), m.l.PeekWord())
	return nil, fmt.Errorf("Did not complete parsing input: %v", m.LexTokenPager.Cur().V)
}

// First keyword was INSERT, REPLACE
func (m *Sqlbridge) parseSqlInsert() (*SqlInsert, error) {

	// insert into mytable (id, str) values (0, "a")
	req := NewSqlInsert()
	req.kw = m.Cur().T
	m.Next() // Consume Insert or Replace

	// INTO
	if m.Cur().T != lex.TokenInto {
		return nil, fmt.Errorf("expected INTO but got: %v", m.Cur())
	}
	m.Next() // Consume INTO

	// table name
	switch m.Cur().T {
	case lex.TokenTable:
		req.Table = m.Cur().V
		m.Next()
	default:
		return nil, fmt.Errorf("expected table name but got : %v", m.Cur().V)
	}

	// list of fields
	cols, err := m.parseFieldList()
	if err != nil {
		u.Error(err)
		return nil, err
	}
	req.Columns = cols

	m.Next() // left paren starts lisf of values
	switch m.Cur().T {
	case lex.TokenValues:
		m.Next() // Consume Values keyword
	case lex.TokenSelect:
		u.Infof("What is cur?%v", m.Cur())
		sel, err := m.parseSqlSelect()
		if err != nil {
			return nil, err
		}
		req.Select = sel
		return req, nil
	default:
		return nil, fmt.Errorf("expected values but got : %v", m.Cur().V)
	}

	//u.Debugf("found ?  %v", m.Cur())
	colVals, err := m.parseValueList()
	if err != nil {
		u.Error(err)
		return nil, err
	}
	req.Rows = colVals
	// we are good
	return req, nil
}

// First keyword was UPDATE
func (m *Sqlbridge) parseSqlUpdate() (*SqlUpdate, error) {

	req := NewSqlUpdate()
	m.Next() // Consume UPDATE token

	//u.Debugf("token:  %v", m.Cur())
	switch m.Cur().T {
	case lex.TokenTable, lex.TokenIdentity:
		req.Table = m.Cur().V
	default:
		return nil, fmt.Errorf("expected table name but got : %v", m.Cur().V)
	}
	m.Next()
	if m.Cur().T != lex.TokenSet {
		return nil, fmt.Errorf("expected SET after table name but got : %v", m.Cur().V)
	}

	// list of name=value pairs
	m.Next()
	cols, err := m.parseUpdateList()
	if err != nil {
		u.Error(err)
		return nil, err
	}
	req.Values = cols

	// WHERE
	req.Where, err = m.parseWhere()
	if err != nil {
		return nil, err
	}

	return req, nil
}

// First keyword was UPSERT
func (m *Sqlbridge) parseSqlUpsert() (*SqlUpsert, error) {

	var err error
	req := NewSqlUpsert()
	m.Next() // Consume UPSERT token

	if m.Cur().T == lex.TokenInto {
		m.Next() // consume Into
	}

	switch m.Cur().T {
	case lex.TokenTable, lex.TokenIdentity:
		req.Table = m.Cur().V
		m.Next()
	default:
		return nil, fmt.Errorf("expected table name but got : %v", m.Cur().V)
	}

	switch m.Cur().T {
	case lex.TokenSet:
		m.Next() // Consume Set
		// list of name=value pairs
		cols, err := m.parseUpdateList()
		if err != nil {
			u.Error(err)
			return nil, err
		}
		req.Values = cols
	case lex.TokenLeftParenthesis:

		// list of fields
		cols, err := m.parseFieldList()
		if err != nil {
			u.Error(err)
			return nil, err
		}
		req.Columns = cols

		m.Next() // left paren starts lisf of values
		switch m.Cur().T {
		case lex.TokenValues:
			m.Next() // Consume Values keyword
		default:
			return nil, fmt.Errorf("expected values but got : %v", m.Cur().V)
		}

		//u.Debugf("found ?  %v", m.Cur())
		colVals, err := m.parseValueList()
		if err != nil {
			u.Error(err)
			return nil, err
		}
		req.Rows = colVals
	default:
		return nil, fmt.Errorf("expected SET name=value, or (col1,col2) after table name but got : %v", m.Cur().V)
	}

	// WHERE
	req.Where, err = m.parseWhere()
	if err != nil {
		return nil, err
	}

	return req, nil
}

// First keyword was DELETE
func (m *Sqlbridge) parseSqlDelete() (*SqlDelete, error) {

	req := NewSqlDelete()
	m.Next() // Consume Delete

	// from
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenFrom {
		return nil, fmt.Errorf("expected FROM but got: %v", m.Cur())
	} else {
		// table name
		m.Next()
		//u.Debugf("found table?  %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenTable:
			req.Table = m.Cur().V
		default:
			return nil, fmt.Errorf("expected table name but got : %v", m.Cur().V)
		}
	}

	m.Next()
	//u.Debugf("cur lex.Token: %s", m.Cur().T.String())
	if errreq := m.parseWhereDelete(req); errreq != nil {
		return nil, errreq
	}
	// we are good
	return req, nil
}

// First keyword was PREPARE
func (m *Sqlbridge) parsePrepare() (*PreparedStatement, error) {

	req := NewPreparedStatement()
	m.Next() // Consume Prepare

	// statement name/alias
	//u.Debugf("found table?  %v", m.Cur())
	switch m.Cur().T {
	case lex.TokenTable, lex.TokenIdentity:
		req.Alias = m.Cur().V
	default:
		return nil, fmt.Errorf("expected statement name but got : %v", m.Cur().V)
	}

	// from
	m.Next()
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenFrom {
		return nil, fmt.Errorf("expected FROM but got: %v", m.Cur())
	}

	m.Next()
	if m.Cur().T != lex.TokenValue {
		return nil, fmt.Errorf("expected statement value but got: %v", m.Cur())
	}
	stmt, err := ParseSql(m.Cur().V)
	if err != nil {
		return nil, err
	}
	req.Statement = stmt
	// we are good
	return req, nil
}

// First keyword was DESCRIBE
func (m *Sqlbridge) parseDescribe() (SqlStatement, error) {

	req := &SqlDescribe{Raw: m.l.RawInput()}
	req.Tok = m.Cur()
	m.Next() // Consume Describe

	//u.Debugf("token:  %v", m.Cur())
	switch nextWord := strings.ToLower(m.Cur().V); nextWord {
	case "select":
		// TODO:  make the lexer handle this
		sqlText := strings.Replace(m.l.RawInput(), req.Tok.V, "", 1)
		sqlSel, err := ParseSql(sqlText)
		if err != nil {
			return nil, err
		}
		req.Stmt = sqlSel
		return req, nil
	case "extended":
		sqlText := strings.Replace(m.l.RawInput(), req.Tok.V, "", 1)
		sqlText = strings.Replace(sqlText, m.Cur().V, "", 1)
		sqlSel, err := ParseSql(sqlText)
		if err != nil {
			return nil, err
		}
		req.Stmt = sqlSel
		return req, nil
	default:
		if lex.TokenIdentity == m.Cur().T {
			req.Identity = m.Cur().V
		} else {
			return nil, fmt.Errorf("expected idenity but got: %v", m.Cur())
		}

	}

	return req, nil
}

// First keyword was SHOW
func (m *Sqlbridge) parseShow() (*SqlShow, error) {

	/*
		don't currently support all these
		http://dev.mysql.com/doc/refman/5.7/en/show.html

		SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
		SHOW CREATE DATABASE db_name
		SHOW CREATE TABLE tbl_name
		SHOW CREATE TRIGGER trigger_name
		SHOW CREATE VIEW view_name
		SHOW DATABASES [like_or_where]
		SHOW ENGINE engine_name {STATUS | MUTEX}
		SHOW [STORAGE] ENGINES
		SHOW INDEX FROM tbl_name [FROM db_name]
		SHOW [FULL] TABLES [FROM db_name] [like_or_where]
		SHOW TRIGGERS [FROM db_name] [like_or_where]
		SHOW [GLOBAL | SESSION] VARIABLES [like_or_where]
		SHOW [GLOBAL | SESSION | SLAVE] STATUS [like_or_where]
		SHOW WARNINGS [LIMIT [offset,] row_count]
	*/
	likeLhs := "Table"
	req := &SqlShow{}
	req.Raw = m.l.RawInput()
	m.Next() // Consume Show

	//u.Infof("cur: %v", m.Cur())
	switch strings.ToLower(m.Cur().V) {
	case "full":
		req.Full = true
		m.Next()
	case "global", "session", "slave":
		req.Scope = strings.ToLower(m.Next().V)
		//u.Infof("scope:%q   next:%v", req.Scope, m.Cur())
	case "create":
		// SHOW CREATE TABLE `temp_schema`.`users`
		req.ShowType = "create"
		m.Next() // consume create
		req.Create = true
		//u.Debugf("create what %v", m.Cur())
		req.CreateWhat = m.Next().V // {TABLE | DATABASE | EVENT ...}
		//u.Debugf("create which %v", m.Cur())
		if m.Cur().T == lex.TokenIdentity {
			req.Identity = m.Next().V
			return req, nil
		}
		return nil, fmt.Errorf("Expected IDENTITY for SHOW CREATE {TABLE | DATABASE | EVENT} IDENTITY but got %s", m.Cur())
	}

	//u.Debugf("show %v", m.Cur())
	objectType := strings.ToLower(m.Cur().V)
	switch objectType {
	case "databases":
		req.ShowType = "databases"
		m.Next()
	case "indexes", "keys":
		req.ShowType = "indexes"
		m.Next()
	case "variables":
		req.ShowType = "variables"
		likeLhs = "Variable_name"
		m.Next()
	case "status":
		req.ShowType = "status"
		likeLhs = "Variable_name"
		m.Next()
	case "engine":
		req.ShowType = "status"
		likeLhs = "Engine"
		m.Next()
	case "engines":
		req.ShowType = "status"
		likeLhs = "Engine"
		m.Next()
	case "procedure", "function":
		req.ShowType = objectType
		likeLhs = "Name"
		m.Next()
	case "columns":
		m.Next() // consume columns
		likeLhs = "Field"
		req.ShowType = "columns"
		//SHOW [FULL] COLUMNS {FROM | IN} tbl_name [{FROM | IN} db_name]  [LIKE 'pattern' | WHERE expr]
		// | Field      | Type     | Null | Key | Default | Extra          |
		if err := m.parseShowFromTable(req); err != nil {
			return nil, err
		}
		if err := m.parseShowFromDatabase(req); err != nil {
			return nil, err
		}
	case "tables":
		req.ShowType = objectType
		m.Next() // consume Tables
		// SHOW [FULL] TABLES [FROM db_name] [like_or_where]
		if err := m.parseShowFromDatabase(req); err != nil {
			return nil, err
		}
	}

	//u.Debugf("show %v", m.Cur())
	switch m.Cur().T {
	case lex.TokenEOF, lex.TokenEOS:
		return req, nil
	case lex.TokenLike:
		// SHOW TABLES LIKE '%'
		//u.Debugf("doing Like: %v %v", m.Cur(), m.Peek())
		m.Next() // Consume Like
		ex, err := expr.ParseExpression(fmt.Sprintf("%s LIKE %q", likeLhs, m.Cur().V))
		m.Next()
		if err != nil {
			u.Errorf("Error parsing fake expression: %v", err)
		} else {
			req.Like = ex.Root
		}
		//u.Debugf("doing Like: %v %v", m.Cur(), m.Peek())
	case lex.TokenWhere:
		m.Next() // consume where
		//u.Debugf("doing where: %v %v", m.Cur(), m.Peek())
		tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
		if err := m.parseNode(tree); err != nil {
			u.Errorf("could not parse: %v", err)
			return nil, err
		}
		req.Where = tree.Root
	}

	return req, nil
}

// First keyword was SET, USE
func (m *Sqlbridge) parseCommand() (*SqlCommand, error) {

	/*
		- SET CHARACTER SET utf8
		- SET NAMES utf8
	*/
	req := &SqlCommand{Columns: make(CommandColumns, 0)}
	req.kw = m.Next().T // USE, SET

	// USE `baseball`;
	if req.kw == lex.TokenUse {
		req.Identity = m.Next().V
		return req, nil
	}

	cur := m.Cur()
	peek := m.Peek()
	// Look for special cases for mysql weird SET syntax
	switch {
	case cur.T == lex.TokenIdentity && strings.ToLower(cur.V) == "names":
		//SET NAMES utf8
		m.Next() // consume NAMES
		col := &CommandColumn{Name: fmt.Sprintf("%s %s", cur.V, m.Next().V)}
		req.Columns = append(req.Columns, col)
		return req, nil
	case cur.T == lex.TokenIdentity && strings.ToLower(cur.V) == "character" && strings.ToLower(peek.V) == "set":
		m.Next() // consume character
		m.Next() // consume set
		col := &CommandColumn{Name: fmt.Sprintf("character set %s", m.Next().V)}
		req.Columns = append(req.Columns, col)
		return req, nil
	}
	return req, m.parseCommandColumns(req)
}

func (m *Sqlbridge) parseTransaction() (*SqlCommand, error) {

	// rollback, commit
	req := &SqlCommand{Columns: make(CommandColumns, 0)}
	req.kw = m.Next().T // rollback, commit

	return req, nil
}

func parseColumns(m expr.TokenPager, fr expr.FuncResolver, buildVm bool, stmt ColumnsStatement) error {

	var col *Column

	discardComments(m)

	for {

		//u.Debug(m.Cur())
		switch m.Cur().T {
		case lex.TokenStar, lex.TokenMultiply:
			col = &Column{Star: true}
			m.Next()
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			col = NewColumnFromToken(m.Cur())
			funcName := strings.ToLower(m.Cur().V)
			tree := expr.NewTreeFuncs(m, fr)
			if err := tree.BuildTree(buildVm); err != nil {
				u.Errorf("could not parse: %v", err)
				return err
			}
			col.Expr = tree.Root
			col.SourceField = expr.FindIdentityField(col.Expr)
			if strings.Contains(col.SourceField, ".") {
				if _, right, hasLeft := expr.LeftRight(col.SourceField); hasLeft {
					col.SourceField = right
				}
			}
			col.Agg = expr.IsAgg(funcName)

			if m.Cur().T != lex.TokenAs {
				switch n := col.Expr.(type) {
				case *expr.FuncNode:
					// lets lowercase name
					n.Name = funcName
					col.As = expr.FindIdentityName(0, n, "")
					//u.Infof("col %#v", col)
					if col.As == "" {
						if strings.ToLower(n.Name) == "count" {
							//u.Warnf("count*")
							col.As = "count(*)"
						} else {
							col.As = n.Name
						}
					}
				case *expr.BinaryNode:
					//u.Debugf("udf? %T ", col.Expr)
					col.As = expr.FindIdentityName(0, n, "")
					if col.As == "" {
						u.Errorf("could not find as name: %#v", n)
					}
				}
			} else {
				switch n := col.Expr.(type) {
				case *expr.FuncNode:
					n.Name = funcName
				}
			}
			//u.Debugf("next? %v", m.Cur())

		case lex.TokenIdentity:
			col = NewColumnFromToken(m.Cur())
			tree := expr.NewTreeFuncs(m, fr)
			if err := tree.BuildTree(buildVm); err != nil {
				u.Errorf("could not parse: %v", err)
				return err
			}
			col.Expr = tree.Root
		case lex.TokenValue, lex.TokenInteger:
			// Value Literal
			col = NewColumnValue(m.Cur())
			tree := expr.NewTreeFuncs(m, fr)
			if err := tree.BuildTree(buildVm); err != nil {
				u.Errorf("could not parse: %v", err)
				return err
			}
			col.Expr = tree.Root
		}
		//u.Debugf("after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAs:
			m.Next()
			switch m.Cur().T {
			case lex.TokenIdentity, lex.TokenValue:
				col.As = m.Cur().V
				col.originalAs = col.As
				col.asQuoteByte = m.Cur().Quote
				m.Next()
				continue
			}
			return fmt.Errorf("expected identity but got: %v", m.Cur().String())
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			stmt.AddColumn(*col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenIf:
			// If guard
			m.Next()
			//u.Infof("if guard: %v", m.Cur())
			tree := expr.NewTreeFuncs(m, fr)
			if err := tree.BuildTree(buildVm); err != nil {
				u.Errorf("could not parse: %v", err)
				return err
			}
			col.Guard = tree.Root
			// Hm, we need to backup here?  Parse Node went to deep?
			continue
			//u.Infof("if guard 2: %v", m.Cur())
			//u.Debugf("after if guard?:   %v  ", m.Cur())
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			//u.Infof("? %#v", stmt)
			//u.Infof("col?%+v", col)
			stmt.AddColumn(*col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

func (m *Sqlbridge) parseFieldList() (Columns, error) {

	if m.Cur().T != lex.TokenLeftParenthesis {
		return nil, fmt.Errorf("Expecting opening paren ( but got %v", m.Cur())
	}
	m.Next()

	cols := make(Columns, 0)
	var col *Column

	for {

		//u.Debug(m.Cur().String())
		switch m.Cur().T {
		case lex.TokenIdentity:
			col = NewColumnFromToken(m.Cur())
			m.Next()
		}
		//u.Debugf("after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF,
			lex.TokenRightParenthesis:
			cols = append(cols, col)
			return cols, nil
		case lex.TokenComma:
			cols = append(cols, col)
		default:
			return nil, fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	panic("unreachable")
}

func (m *Sqlbridge) parseUpdateList() (map[string]*ValueColumn, error) {

	cols := make(map[string]*ValueColumn)
	lastColName := ""
	for {

		//u.Debugf("col:%v    cur:%v", lastColName, m.Cur().String())
		switch m.Cur().T {
		case lex.TokenWhere, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			return cols, nil
		case lex.TokenValue:
			cols[lastColName] = &ValueColumn{Value: value.NewStringValue(m.Cur().V)}
		case lex.TokenInteger:
			iv, _ := strconv.ParseInt(m.Cur().V, 10, 64)
			cols[lastColName] = &ValueColumn{Value: value.NewIntValue(iv)}
		case lex.TokenComma, lex.TokenEqual:
			// don't need to do anything
		case lex.TokenIdentity:
			// TODO:  this is a bug in lexer
			lv := m.Cur().V
			if bv, err := strconv.ParseBool(lv); err == nil {
				cols[lastColName] = &ValueColumn{Value: value.NewBoolValue(bv)}
			} else {
				lastColName = m.Cur().V
			}
		case lex.TokenUdfExpr:
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			cols[lastColName] = &ValueColumn{Expr: tree.Root}
		default:
			u.Warnf("don't know how to handle ?  %v", m.Cur())
			return nil, fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	panic("unreachable")
}

func (m *Sqlbridge) parseValueList() ([][]*ValueColumn, error) {

	if m.Cur().T != lex.TokenLeftParenthesis {
		return nil, fmt.Errorf("Expecting opening paren ( but got %v", m.Cur())
	}

	var row []*ValueColumn
	values := make([][]*ValueColumn, 0)

	for {

		//u.Debug(m.Cur().String())
		switch m.Cur().T {
		case lex.TokenLeftParenthesis:
			// start of row
			if len(row) > 0 {
				values = append(values, row)
			}
			row = make([]*ValueColumn, 0)
		case lex.TokenRightParenthesis:
			values = append(values, row)
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			if len(row) > 0 {
				values = append(values, row)
			}
			return values, nil
		case lex.TokenValue:
			row = append(row, &ValueColumn{Value: value.NewStringValue(m.Cur().V)})
		case lex.TokenInteger:
			iv, err := strconv.ParseInt(m.Cur().V, 10, 64)
			if err != nil {
				return nil, err
			}
			row = append(row, &ValueColumn{Value: value.NewIntValue(iv)})
		case lex.TokenFloat:
			fv, err := strconv.ParseFloat(m.Cur().V, 64)
			if err != nil {
				return nil, err
			}
			row = append(row, &ValueColumn{Value: value.NewNumberValue(fv)})
		case lex.TokenBool:
			bv, err := strconv.ParseBool(m.Cur().V)
			if err != nil {
				return nil, err
			}
			row = append(row, &ValueColumn{Value: value.NewBoolValue(bv)})
		case lex.TokenIdentity:
			// TODO:  this is a bug in lexer
			lv := m.Cur().V
			if bv, err := strconv.ParseBool(lv); err == nil {
				row = append(row, &ValueColumn{Value: value.NewBoolValue(bv)})
			} else {
				// error?
				u.Warnf("Could not figure out how to use: %v", m.Cur())
			}
		case lex.TokenLeftBracket:
			// an array of values?
			m.Next() // Consume the [
			arrayVal, err := expr.ValueArray(m.SqlTokenPager)
			if err != nil {
				return nil, err
			}
			//n := NewValueNode(arrayVal)
			row = append(row, &ValueColumn{Value: arrayVal})
			u.Infof("what is token?  %v peek:%v", m.Cur(), m.Peek())
			//t.Next()
		case lex.TokenComma:
			// don't need to do anything
		case lex.TokenUdfExpr:
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return nil, err
			}
			//col.Expr = tree.Root
			row = append(row, &ValueColumn{Expr: tree.Root})
		default:
			u.Warnf("don't know how to handle ?  %v", m.Cur())
			return nil, fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	panic("unreachable")
}

func (m *Sqlbridge) parseSources(req *SqlSelect) error {

	//u.Debugf("parseSources cur %v", m.Cur())

	if m.Cur().T != lex.TokenFrom {
		return fmt.Errorf("expected From but got: %v", m.Cur())
	}

	m.Next() // page forward off of From
	//u.Debugf("found from?  %v", m.Cur())

	if m.Cur().T == lex.TokenIdentity {
		if err := m.parseSourceTable(req); err != nil {
			return err
		}
	}

	for {

		src := &SqlSource{}
		//u.Debugf("parseSources %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenRightParenthesis:
			return nil
		case lex.TokenLeftParenthesis:
			// SELECT [columns] FROM [table] AS t1
			//   INNER JOIN (select a,b,c from users WHERE d is not null) u ON u.user_id = t1.user_id
			if err := m.parseSourceSubQuery(src); err != nil {
				return err
			}
			//u.Infof("wat? %v", m.Cur())
			if m.Cur().T == lex.TokenRightParenthesis {
				m.Next()
			}
		case lex.TokenLeft, lex.TokenRight, lex.TokenInner, lex.TokenOuter, lex.TokenJoin:
			// JOIN
			if err := m.parseSourceJoin(src); err != nil {
				return err
			}
		case lex.TokenEOF, lex.TokenEOS, lex.TokenWhere, lex.TokenGroupBy, lex.TokenLimit,
			lex.TokenOffset, lex.TokenWith, lex.TokenAlias, lex.TokenOrderBy:
			return nil
		default:

			u.Warnf("unrecognized token? %v clauseEnd?%v", m.Cur(), m.SqlTokenPager.ClauseEnd())
			return fmt.Errorf("unexpected token got: %v", m.Cur())
		}

		//u.Debugf("cur: %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenAs:
			m.Next() // Skip over As, we don't need it
			src.Alias = m.Cur().V
			m.Next()
			//u.Debugf("found source alias: %v AS %v", src.Name, src.Alias)
			// select u.name, order.date FROM user AS u INNER JOIN ....
		case lex.TokenIdentity:
			//u.Warnf("found identity? %v", m.Cur())
			src.Alias = m.Cur().V
			m.Next()
		}
		//u.Debugf("cur: %v", m.Cur())
		if m.Cur().T == lex.TokenOn {
			src.Op = m.Cur().T
			m.Next()
			//u.Debugf("cur = %v", m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				u.Errorf("could not parse: %v", err)
				return err
			}
			src.JoinExpr = tree.Root
			//u.Debugf("join expression: %v", tree.Root.String())
			//u.Debugf("join:  %#v", src)
		}

		req.From = append(req.From, src)

	}
	return nil
}

func (m *Sqlbridge) parseSourceSubQuery(src *SqlSource) error {

	//u.Debugf("parseSourceSubQuery cur %v", m.Cur())
	m.Next() // page forward off of (
	//u.Debugf("found SELECT?  %v", m.Cur())

	// SELECT * FROM (SELECT 1, 2, 3) AS t1;
	subQuery, err := m.parseSqlSelect()
	if err != nil {
		return err
	}
	src.SubQuery = subQuery
	subQuery.Raw = subQuery.String()

	if m.Cur().T != lex.TokenRightParenthesis {
		return fmt.Errorf("expected right paren but got: %v", m.Cur())
	}
	//u.Debugf("cur %v", m.Cur())
	m.Next() // discard right paren
	//u.Infof("found from subquery: %s", subQuery)
	return nil
}

func (m *Sqlbridge) parseSourceTable(req *SqlSelect) error {

	if m.Cur().T != lex.TokenIdentity {
		return fmt.Errorf("expected tablename but got: %v", m.Cur())
	}

	src := SqlSource{}
	req.From = append(req.From, &src)
	src.Schema, src.Name, _ = expr.LeftRight(m.Next().V)
	if m.Cur().T == lex.TokenAs {
		m.Next() // Skip over "AS", we don't need it
		src.Alias = m.Next().V
	}
	return nil
}

func (m *Sqlbridge) parseSourceJoin(src *SqlSource) error {
	//u.Debugf("parseSourceJoin cur %v", m.Cur())

	switch m.Cur().T {
	case lex.TokenLeft, lex.TokenRight:
		//u.Debugf("left/right join: %v", m.Cur())
		src.LeftOrRight = m.Cur().T
		m.Next()
	}

	// Optional Inner/Outer
	switch m.Cur().T {
	case lex.TokenInner, lex.TokenOuter:
		src.JoinType = m.Cur().T
		m.Next()
	}

	if m.Cur().T == lex.TokenJoin {
		m.Next() // Consume join keyword
	} else {
		return fmt.Errorf("Requires join but got %v", m.Cur())
	}

	switch m.Cur().T {
	case lex.TokenLeftParenthesis:
		// SELECT [columns] FROM [table] AS t1
		//   INNER JOIN (select a,b,c from users WHERE d is not null) u ON u.user_id = t1.user_id
		if err := m.parseSourceSubQuery(src); err != nil {
			return err
		}
	case lex.TokenIdentity:
		// Name of table
		src.Name = m.Cur().V
		m.Next()
	default:
		return fmt.Errorf("unrecognized kw in join %v", m.Cur())
	}

	//u.Debugf("found join %q", src)
	return nil
}

func (m *Sqlbridge) parseInto(req *SqlSelect) error {

	if m.Cur().T != lex.TokenInto {
		return nil
	}
	m.Next() // Consume Into token

	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenTable {
		return fmt.Errorf("expected table but got: %v", m.Cur())
	}
	req.Into = &SqlInto{Table: m.Cur().V}
	m.Next()
	return nil
}

// Parse an expression tree or root Node
func (m *Sqlbridge) parseNode(tree *expr.Tree) error {
	//u.Debugf("cur token parse: token=%v", m.Cur())
	err := tree.BuildTree(m.buildVm)
	if err != nil {
		u.Errorf("error: %v", err)
	}
	return err
}

func (m *Sqlbridge) parseWhereSubSelect(req *SqlSelect) error {

	if m.Cur().T != lex.TokenSelect {
		return nil
	}
	stmt, err := m.parseSqlSelect()
	if err != nil {
		return err
	}
	//u.Infof("found sub-select %+v", stmt)
	req = stmt
	return nil
}

func (m *Sqlbridge) parseWhereSelect(req *SqlSelect) error {

	var err error
	if m.Cur().T != lex.TokenWhere {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("where error? %v \n %v\n%s", r, m.Cur(), m.Lexer().RawInput())
			if m.Cur().T == lex.TokenSelect {
				// TODO this is deeply flawed, need to fix/use tokenpager
				//    with rewind ability
				err = m.parseWhereSubSelect(req)
				return
			}
			err = fmt.Errorf("panic err: %v", r)
		}
	}()

	where, err := m.parseWhere()
	if err != nil {
		return err
	} else if where != nil {
		req.Where = where
	}
	return nil
}

func (m *Sqlbridge) parseWhere() (*SqlWhere, error) {

	var err error
	if m.Cur().T != lex.TokenWhere {
		return nil, nil
	}

	m.Next() // Consume the Where
	//u.Debugf("cur: %v peek=%v", m.Cur(), m.Peek())

	where := SqlWhere{}

	// We are going to Peek forward at the next 3 tokens used
	// to determine which type of where clause
	//t1 := m.Cur().T
	m.Next() // x
	t2 := m.Cur().T
	m.Next()
	t3 := m.Cur().T
	m.Next()
	t4 := m.Cur().T
	m.Backup()
	m.Backup()
	m.Backup()

	// Check for Types of Where
	//                                 t1            T2      T3     T4
	//    SELECT x FROM user   WHERE user_id         IN      (      SELECT user_id from orders where ...)
	//    SELECT * FROM t1     WHERE column1         =       (      SELECT column1 FROM t2);
	//    select a FROM movies WHERE director        IN      (     "Quentin","copola","Bay","another")
	//    select b FROM movies WHERE director        =       "bob";
	//    select b FROM movies WHERE create          BETWEEN "2015" AND "2010";
	//    select b from movies WHERE director        LIKE    "%bob"
	// TODO:
	//    SELECT * FROM t3     WHERE ROW(5*t2.s1,77) =       (      SELECT 50,11*s1 FROM t4)
	switch {
	case (t2 == lex.TokenIN || t2 == lex.TokenEqual) && t3 == lex.TokenLeftParenthesis && t4 == lex.TokenSelect:
		//u.Infof("in parseWhere: %v", m.Cur())
		m.Next() // T1  ?? this might be udf?
		m.Next() // t2  (IN | =)
		m.Next() // t3 = (
		//m.Next() // t4 = SELECT
		where.Op = t2
		where.Source = &SqlSelect{}
		return &where, m.parseWhereSubSelect(where.Source)
	}
	//u.Debugf("doing Where: %v %v", m.Cur(), m.Peek())
	tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
	if err := m.parseNode(tree); err != nil {
		u.Errorf("could not parse: %v", err)
		return nil, err
	}
	where.Expr = tree.Root
	//u.Debugf("where: %v", m.Cur())
	return &where, err
}

func (m *Sqlbridge) parseGroupBy(req *SqlSelect) (err error) {

	if m.Cur().T != lex.TokenGroupBy {
		return nil
	}
	m.Next()

	var col *Column

	for {

		//u.Debugf("Group By? %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			//u.Infof("udf: %v", m.Cur().V)
			col = NewColumnFromToken(m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				return err
			}
			col.Expr = tree.Root

			if m.Cur().T != lex.TokenAs {
				switch n := col.Expr.(type) {
				case *expr.FuncNode:
					col.As = expr.FindIdentityName(0, n, "")
					if col.As == "" {
						col.As = n.Name
					}
				case *expr.BinaryNode:
					//u.Debugf("udf? %T ", n)
					col.As = expr.FindIdentityName(0, n, "")
					if col.As == "" {
						u.Errorf("could not find as name: %#v", n)
					}
				}
			}
			//u.Debugf("next? %v", m.Cur())

		case lex.TokenIdentity:
			//u.Warnf("?? %v", m.Cur())
			col = NewColumnFromToken(m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				return err
			}
			col.Expr = tree.Root
		case lex.TokenValue:
			// Value Literal
			col = NewColumnFromToken(m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				return err
			}
			col.Expr = tree.Root
		}
		//u.Debugf("GroupBy after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAs:
			m.Next()
			//u.Debug(m.Cur())
			switch m.Cur().T {
			case lex.TokenIdentity, lex.TokenValue:
				col.As = m.Cur().V
				col.originalAs = col.As
				//u.Infof("set AS=%v", col.As)
				m.Next()
				continue
			}
			return fmt.Errorf("expected identity but got: %v", m.Cur().String())
		case lex.TokenFrom, lex.TokenOrderBy, lex.TokenInto, lex.TokenLimit, lex.TokenHaving,
			lex.TokenWith, lex.TokenEOS, lex.TokenEOF:

			// This indicates we have come to the End of the columns
			req.GroupBy = append(req.GroupBy, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenIf:
			// If guard
			m.Next()
			//u.Infof("if guard: %v", m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				return err
			}
			col.Guard = tree.Root
			//u.Debugf("after if guard?:   %v  ", m.Cur())
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			req.GroupBy = append(req.GroupBy, col)
			//u.Debugf("comma, added groupby:  %v", len(stmt.GroupBy))
		default:
			u.Errorf("expected col? %v", m.Cur())
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("groupby: %d", len(req.GroupBy))
	return nil
}

func (m *Sqlbridge) parseHaving(req *SqlSelect) (err error) {

	if m.Cur().T != lex.TokenHaving {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("having error? %v \n %v", r, m.Cur())
			if m.Cur().T == lex.TokenSelect {
				// TODO this is deeply flawed, need to fix/use tokenpager
				// with rewind ability
				err = m.parseWhereSelect(req)
				return
			}
			err = fmt.Errorf("panic err: %v", r)
		}
	}()
	m.Next()
	//u.Infof("%v", m.Cur())
	tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
	if err := m.parseNode(tree); err != nil {
		u.Warnf("could not parse: %v", err)
		return err
	}
	req.Having = tree.Root
	//u.Debugf("having: %v", m.Cur())
	return err
}

func (m *Sqlbridge) parseOrderBy(req *SqlSelect) (err error) {

	if m.Cur().T != lex.TokenOrderBy {
		return nil
	}
	m.Next() // Consume Order By

	var col *Column

	for {

		//u.Debugf("Order By? %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			//u.Infof("udf: %v", m.Cur().V)
			col = NewColumnFromToken(m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				u.Warnf("could not parse: %v", err)
				return err
			}
			col.Expr = tree.Root
			switch n := col.Expr.(type) {
			case *expr.FuncNode:
				col.As = expr.FindIdentityName(0, n, "")
				if col.As == "" {
					col.As = n.Name
				}
			case *expr.BinaryNode:
				//u.Debugf("udf? %T ", n)
				col.As = expr.FindIdentityName(0, n, "")
				if col.As == "" {
					u.Errorf("could not find as name: %#v", n)
				}
			}
			//u.Debugf("next? %v", m.Cur())
		case lex.TokenIdentity:
			//u.Warnf("?? %v", m.Cur())
			col = NewColumnFromToken(m.Cur())
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				u.Warnf("could not parse: %v", err)
				return err
			}
			col.Expr = tree.Root
		}
		//u.Debugf("OrderBy after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAsc, lex.TokenDesc:
			col.Order = strings.ToUpper(m.Cur().V)

		case lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			req.OrderBy = append(req.OrderBy, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			req.OrderBy = append(req.OrderBy, col)
			//u.Debugf("comma, added groupby:  %v", len(stmt.OrderBy))
		default:
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseWhereDelete(req *SqlDelete) error {

	if m.Cur().T != lex.TokenWhere {
		return nil
	}

	m.Next()
	tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
	if err := m.parseNode(tree); err != nil {
		u.Warnf("could not parse: %v", err)
		return err
	}
	req.Where = &SqlWhere{Expr: tree.Root}
	return nil
}

func (m *Sqlbridge) parseCommandColumns(req *SqlCommand) (err error) {

	var col *CommandColumn

	for {

		//u.Debugf("command col? %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenIdentity:
			//u.Warnf("?? %v", m.Cur())

			col = &CommandColumn{Name: m.Cur().V}
			tree := expr.NewTreeFuncs(m.SqlTokenPager, m.funcs)
			if err := m.parseNode(tree); err != nil {
				u.Warnf("could not parse: %v", err)
				return err
			}
			col.Expr = tree.Root
			convertIdentityToValue(col.Expr)
			//u.Infof("expr: %T :: %s", col.Expr, col.Expr)
		default:
			return fmt.Errorf("expected idenity but got: %v", m.Cur())
		}
		//u.Debugf("command after colstart?:   %v  ", m.Cur())

		// since we can have multiple columns
		switch m.Cur().T {
		case lex.TokenEOS, lex.TokenEOF:
			req.Columns = append(req.Columns, col)
			return nil
		case lex.TokenComma:
			req.Columns = append(req.Columns, col)
		default:
			u.Errorf("expected col? %v", m.Cur())
			return fmt.Errorf("expected command column but got: %v", m.Cur().String())
		}
		m.Next()
	}
}

func convertIdentityToValue(n expr.Node) {
	switch nt := n.(type) {
	case *expr.BinaryNode:
		switch rhn := nt.Args[1].(type) {
		case *expr.IdentityNode:
			rh2 := expr.NewStringNode(rhn.Text)
			rh2.Quote = rhn.Quote
			nt.Args[1] = rh2
		}
	}
}

func (m *Sqlbridge) parseLimit(req *SqlSelect) error {
	if m.Cur().T != lex.TokenLimit {
		return nil
	}
	m.Next()
	if m.Cur().T != lex.TokenInteger {
		return fmt.Errorf("Limit must be an integer %v %v", m.Cur().T, m.Cur().V)
	}
	limval := m.Next()
	iv, err := strconv.Atoi(limval.V)
	if err != nil {
		return fmt.Errorf("Could not convert limit to integer %v", limval)
	}
	req.Limit = int(iv)
	//u.Infof("limit clause: %v  peek:%v", limval, m.l.PeekX(20))
	switch m.Cur().T {
	case lex.TokenComma:
		// LIMIT 0, 1000
		m.Next() // consume the comma
		if m.Cur().T != lex.TokenInteger {
			return fmt.Errorf("Limit 0, 1000 2nd number must be an integer %v %v", m.Cur().T, m.Cur().V)
		}
		iv, err = strconv.Atoi(m.Next().V)
		if err != nil {
			return fmt.Errorf("Could not convert limit to integer %v", m.Cur().V)
		}
		req.Offset = req.Limit
		req.Limit = iv
	case lex.TokenOffset:
		m.Next() // consume "OFFSET"
		if m.Cur().T != lex.TokenInteger {
			return fmt.Errorf("Offset must be an integer %v %v", m.Cur().T, m.Cur().V)
		}
		iv, err = strconv.Atoi(m.Cur().V)
		m.Next()
		if err != nil {
			return fmt.Errorf("Could not convert offset to integer %v", m.Cur().V)
		}
		req.Offset = iv
	}
	return nil
}

func (m *Sqlbridge) parseAlias(req *SqlSelect) error {
	if m.Cur().T != lex.TokenAlias {
		return nil
	}
	m.Next()
	if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
		return fmt.Errorf("Expected identity but got: %v", m.Cur().T.String())
	}
	req.Alias = strings.ToLower(m.Cur().V)
	m.Next()
	return nil
}
func (m *Sqlbridge) isEnd() bool {
	return m.IsEnd()
}

func ParseWith(pg expr.TokenPager) (u.JsonHelper, error) {
	if pg.Cur().T != lex.TokenWith {
		// This is an optional statement
		return nil, nil
	}
	pg.Next() // consume WITH
	jh := make(u.JsonHelper)
	switch pg.Cur().T {
	case lex.TokenLeftBrace: // {
		if err := ParseJsonObject(pg, jh); err != nil {
			return nil, err
		}
	case lex.TokenIdentity:
		// name=value pairs
		if err := ParseKeyValue(pg, jh); err != nil {
			return nil, err
		}
	default:
		u.Warnf("unexpected token? %v", pg.Cur())
		return nil, fmt.Errorf("Expected json { , or name=value but got: %v", pg.Cur().T.String())
	}
	return jh, nil
}

func (m *Sqlbridge) parseShowFromTable(req *SqlShow) error {

	switch m.Cur().T {
	case lex.TokenFrom, lex.TokenIN:
		m.Next() // Consume {FROM | IN}
	default:
		// FROM OR IN are required for this statement
		return fmt.Errorf("Expected { FROM | IN } for SHOW but got %q", m.Cur().V)
	}

	if m.Cur().T != lex.TokenIdentity {
		return fmt.Errorf("Expected { FROM | IN } IDENTITY for SHOW but got %q", m.Cur().V)
	}
	req.Identity = m.Next().V
	return nil
}

func (m *Sqlbridge) parseShowFromDatabase(req *SqlShow) error {

	switch m.Cur().T {
	case lex.TokenFrom, lex.TokenIN:
		m.Next() // Consume {FROM | IN}
	default:
		// this is optional
		return nil
	}

	if m.Cur().T != lex.TokenIdentity {
		return fmt.Errorf("Expected { FROM | IN } IDENTITY for SHOW but got %q", m.Cur().V)
	}
	req.Db = m.Next().V
	return nil
}

func ParseJsonObject(pg expr.TokenPager, jh u.JsonHelper) error {
	if pg.Cur().T != lex.TokenLeftBrace {
		return fmt.Errorf("Expected json { but got: %v", pg.Cur().T.String())
	}
	pg.Next() // Consume {

	for {
		//u.Debug(pg.Cur())
		switch pg.Cur().T {
		case lex.TokenIdentity:
			if err := parseJsonKeyValue(pg, jh); err != nil {
				return err
			}
		default:
			return fmt.Errorf("Expected json key identity but got: %v", pg.Cur().String())
		}
		switch pg.Cur().T {
		case lex.TokenComma:
			pg.Next()
		case lex.TokenRightBrace:
			pg.Next() // Consume the right }
			return nil
		default:
			return fmt.Errorf("Expected json comma or end of object but got: %v", pg.Cur().String())
		}
	}
	return nil // panic? error?  not reachable
}
func parseJsonKeyValue(pg expr.TokenPager, jh u.JsonHelper) error {
	if pg.Cur().T != lex.TokenIdentity {
		return fmt.Errorf("Expected json key/identity but got: %v", pg.Cur().String())
	}
	key := pg.Cur().V
	pg.Next()
	//u.Debug(key, " ", pg.Cur())
	switch pg.Cur().T {
	case lex.TokenColon:
		pg.Next()
		switch pg.Cur().T {
		case lex.TokenLeftBrace: // {
			obj := make(u.JsonHelper)
			if err := ParseJsonObject(pg, obj); err != nil {
				return err
			}
			jh[key] = obj
		case lex.TokenLeftBracket: // [
			list, err := ParseJsonArray(pg)
			if err != nil {
				return err
			}
			//u.Debugf("list after: %#v", list)
			jh[key] = list
		case lex.TokenValue:
			jh[key] = pg.Cur().V
			pg.Next()
		case lex.TokenBool:
			bv, err := strconv.ParseBool(pg.Cur().V)
			if err != nil {
				return err
			}
			jh[key] = bv
			pg.Next()
		case lex.TokenInteger:
			iv, err := strconv.ParseInt(pg.Cur().V, 10, 64)
			if err != nil {
				return err
			}
			jh[key] = iv
			pg.Next()
		case lex.TokenFloat:
			fv, err := strconv.ParseFloat(pg.Cur().V, 64)
			if err != nil {
				return err
			}
			jh[key] = fv
			pg.Next()
		default:
			u.Warnf("got unexpected token: %s", pg.Cur())
			return fmt.Errorf("Expected json { or [ but got: %v", pg.Cur().T.String())
		}
		//u.Debug(key, " ", pg.Cur())
		return nil
	default:
		return fmt.Errorf("Expected json colon but got: %v", pg.Cur().String())
	}
	return fmt.Errorf("Unreachable json error: %v", pg.Cur().String())
}

func ParseJsonArray(pg expr.TokenPager) ([]interface{}, error) {
	if pg.Cur().T != lex.TokenLeftBracket {
		return nil, fmt.Errorf("Expected json [ but got: %v", pg.Cur().T.String())
	}

	la := make([]interface{}, 0)
	pg.Next() // Consume [
	for {
		//u.Debug(pg.Cur())
		switch pg.Cur().T {
		case lex.TokenValue:
			la = append(la, pg.Cur().V)
			pg.Next()
		case lex.TokenBool:
			bv, err := strconv.ParseBool(pg.Cur().V)
			if err != nil {
				return nil, err
			}
			la = append(la, bv)
			pg.Next()
		case lex.TokenInteger:
			iv, err := strconv.ParseInt(pg.Cur().V, 10, 64)
			if err != nil {
				return nil, err
			}
			la = append(la, iv)
			pg.Next()
		case lex.TokenFloat:
			fv, err := strconv.ParseFloat(pg.Cur().V, 64)
			if err != nil {
				return nil, err
			}
			la = append(la, fv)
			pg.Next()
		case lex.TokenLeftBrace: // {
			obj := make(u.JsonHelper)
			if err := ParseJsonObject(pg, obj); err != nil {
				return nil, err
			}
			la = append(la, obj)
		case lex.TokenLeftBracket: // [
			list, err := ParseJsonArray(pg)
			if err != nil {
				return nil, err
			}
			//u.Debugf("list after: %#v", list)
			la = append(la, list)
		case lex.TokenRightBracket:
			return la, nil
		default:
			return nil, fmt.Errorf("Expected json key identity but got: %v", pg.Cur().String())
		}
		switch pg.Cur().T {
		case lex.TokenComma:
			pg.Next()
		case lex.TokenRightBracket:
			pg.Next() // Consume the right ]
			return la, nil
		default:
			return nil, fmt.Errorf("Expected json comma or end of array ] but got: %v", pg.Cur().String())
		}
	}
	return la, nil
}

func ParseKeyValue(pg expr.TokenPager, jh u.JsonHelper) error {
	if pg.Cur().T != lex.TokenIdentity {
		return fmt.Errorf("Expected key/identity for key=value, array but got: %v", pg.Cur().String())
	}

	for {
		key := pg.Cur().V
		pg.Next()

		switch pg.Cur().T {
		case lex.TokenEOF, lex.TokenEOS:
			return nil
		}
		if pg.Cur().T != lex.TokenEqual {
			pg.Backup() // whoops, we consumed too much
			pg.Backup()
			//u.Debugf("exit keyvalue %v", pg.Cur())
			return nil
		}
		pg.Next() // consume equal

		switch pg.Cur().T {
		case lex.TokenIdentity:
			bv, err := strconv.ParseBool(pg.Cur().V)
			if err == nil {
				jh[key] = bv
			} else {
				jh[key] = pg.Cur().V
			}

		case lex.TokenValue:
			jh[key] = pg.Cur().V
		case lex.TokenBool:
			bv, err := strconv.ParseBool(pg.Cur().V)
			if err != nil {
				return err
			}
			jh[key] = bv
		case lex.TokenInteger:
			iv, err := strconv.ParseInt(pg.Cur().V, 10, 64)
			if err != nil {
				return err
			}
			jh[key] = iv
		case lex.TokenFloat:
			fv, err := strconv.ParseFloat(pg.Cur().V, 64)
			if err != nil {
				return err
			}
			jh[key] = fv
		default:
			u.Warnf("got unexpected token: %s", pg.Cur())
			return fmt.Errorf("Expected value but got: %v  for name=value context", pg.Cur().T.String())
		}
		pg.Next() // consume value
		//u.Debugf("cur: %v", pg.Cur())
		if pg.Cur().T != lex.TokenComma {
			//u.Debugf("finished loop: jh.len=%v  token=%v", len(jh), pg.Cur())
			return nil
		}
		pg.Next() // consume comma
	}
	panic("unreachable")
}

// TokenPager is responsible for determining end of
// current tree (column, etc)
type SqlTokenPager struct {
	*expr.LexTokenPager
	lastKw lex.TokenType
}

func NewSqlTokenPager(l *lex.Lexer) *SqlTokenPager {
	pager := expr.NewLexTokenPager(l)
	return &SqlTokenPager{LexTokenPager: pager}
}

func (m *SqlTokenPager) IsEnd() bool {
	return m.LexTokenPager.IsEnd()
}
func (m *SqlTokenPager) ClauseEnd() bool {
	tok := m.Cur()
	//u.Debugf("IsEnd()? tok:  %v", tok)
	switch tok.T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenHaving, lex.TokenComma,
		lex.TokenIf, lex.TokenAs, lex.TokenLimit, lex.TokenSelect:
		return true
	}
	return false
}
