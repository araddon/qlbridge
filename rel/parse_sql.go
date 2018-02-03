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
	// SqlKeywords the SqlKeywords list
	SqlKeywords = []string{"select", "insert", "update", "delete", "from", "where", "as", "into", "limit",
		"exists", "in", "contains", "include", "not", "and", "having", "or", "null", "group", "order",
		"offset", "include", "all", "any", "some"}
)

// ParseError type
type ParseError struct {
	error
}

// ParseSql Parses SqlStatement and returns a statement or error
// does not parse more than one statement
func ParseSql(sqlQuery string) (SqlStatement, error) {
	return parseSqlResolver(sqlQuery, nil)
}
func parseSqlResolver(sqlQuery string, fr expr.FuncResolver) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	m := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), funcs: fr}
	s, err := m.parse()
	if err != nil {
		return nil, &ParseError{err}
	}
	return s, nil
}

// ParseSqlSelect parse a sql statement as SELECT (or else error)
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

// ParseSqlSelectResolver parse as SELECT using function resolver.
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

// ParseSqlStatements into array of SQL Statements
func ParseSqlStatements(sqlQuery string) ([]SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	m := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l)}
	stmts := make([]SqlStatement, 0)
	for {
		stmt, err := m.parse()
		if err != nil {
			return nil, &ParseError{err}
		}
		stmts = append(stmts, stmt)
		sqlRemaining, hasMore := l.Remainder()
		if !hasMore {
			break
		}
		l = lex.NewSqlLexer(sqlRemaining)
		m = Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l)}
	}
	return stmts, nil
}

// Sqlbridge generic SQL parser evaluates should be sufficient for most
// sql compatible languages
type Sqlbridge struct {
	l       *lex.Lexer
	comment string
	*SqlTokenPager
	firstToken lex.Token
	funcs      expr.FuncResolver
}

// parse the request
func (m *Sqlbridge) parse() (SqlStatement, error) {
	m.comment = readComment(m)
	m.firstToken = m.Cur()
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
		return m.parseShow()
	case lex.TokenExplain, lex.TokenDescribe, lex.TokenDesc:
		return m.parseDescribe()
	case lex.TokenSet, lex.TokenUse:
		return m.parseCommand()
	case lex.TokenRollback, lex.TokenCommit:
		return m.parseTransaction()
	case lex.TokenCreate:
		return m.parseCreate()
	case lex.TokenDrop:
		return m.parseDrop()
	}
	return nil, fmt.Errorf("Unrecognized request type: %v", m.l.PeekWord())
}

func readComment(p expr.TokenPager) string {

	comment := ""

	for {
		// We are going to loop until we find the first Non-Comment Token
		switch p.Cur().T {
		case lex.TokenComment, lex.TokenCommentML:
			comment += p.Cur().V
		case lex.TokenCommentStart, lex.TokenCommentHash, lex.TokenCommentEnd, lex.TokenCommentSingleLine, lex.TokenCommentSlashes:
			// skip, currently ignore these
		default:
			// first non-comment token
			return comment
		}
		p.Next()
	}
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
	if err := parseColumns(m, m.funcs, req); err != nil {
		return nil, err
	}

	// select @@myvar limit 1
	if m.Cur().T == lex.TokenLimit {
		if err := m.parseLimit(req); err != nil {
			return nil, err
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
	if err := m.parseInto(req); err != nil {
		return nil, err
	}

	// FROM
	discardComments(m)
	if err := m.parseSources(req); err != nil {
		return nil, err
	}

	// WHERE
	discardComments(m)
	if err := m.parseWhereSelect(req); err != nil {
		return nil, err
	}

	// GROUP BY
	discardComments(m)
	if err := m.parseGroupBy(req); err != nil {
		return nil, err
	}

	// HAVING
	discardComments(m)
	if err := m.parseHaving(req); err != nil {
		return nil, err
	}

	// ORDER BY
	discardComments(m)
	if err := m.parseOrderBy(req); err != nil {
		return nil, err
	}

	// LIMIT
	discardComments(m)
	if err := m.parseLimit(req); err != nil {
		return nil, err
	}

	// OFFSET
	discardComments(m)
	if err := m.parseOffset(req); err != nil {
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
			return nil, err
		}

		// we are good
		return req, nil
	}

	u.Debugf("Could not complete parsing, return error: %v %v", m.Cur(), m.l.PeekWord())
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
		return nil, err
	}
	req.Columns = cols

	m.Next() // left paren starts lisf of values
	switch m.Cur().T {
	case lex.TokenValues:
		m.Next() // Consume Values keyword
	case lex.TokenSelect:
		sel, err := m.parseSqlSelect()
		if err != nil {
			return nil, err
		}
		if len(sel.From) == 0 {
			return nil, m.ErrMsg("Expected FROM <sources>")
		}
		req.Select = sel
		return req, nil
	default:
		return nil, m.ErrMsg("expected INSERT (columns) VALUES <values>")
	}

	colVals, err := m.parseValueList()
	if err != nil {
		return nil, err
	}
	req.Rows = colVals
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
			return nil, m.ErrMsg("expected values")
		}

		//u.Debugf("found ?  %v", m.Cur())
		colVals, err := m.parseValueList()
		if err != nil {
			u.Error(err)
			return nil, err
		}
		req.Rows = colVals
	default:
		return nil, m.ErrMsg("expected SET name=value, or (col1,col2) after table name")
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
		return nil, m.ErrMsg("expected FROM")
	} else {
		// table name
		m.Next()
		//u.Debugf("found table?  %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenTable:
			req.Table = m.Cur().V
		default:
			return nil, m.ErrMsg("expected table name")
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
		return nil, m.ErrMsg("expected statement name")
	}

	// from
	m.Next()
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenFrom {
		return nil, m.ErrMsg("expected FROM")
	}

	m.Next()
	if m.Cur().T != lex.TokenValue {
		return nil, m.ErrMsg("expected statement value ")
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
			return nil, m.ErrMsg("expected idenity")
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
		return nil, m.ErrMsg("Expected IDENTITY for SHOW CREATE {TABLE | DATABASE | EVENT} IDENTITY")
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

	switch m.Cur().T {
	case lex.TokenEOF, lex.TokenEOS:
		return req, nil
	case lex.TokenLike:
		// SHOW TABLES LIKE '%'
		m.Next() // Consume Like
		ex, err := expr.ParseExpression(fmt.Sprintf("%s LIKE %q", likeLhs, m.Cur().V))
		m.Next()
		if err != nil {
			u.Errorf("Error parsing fake expression: %v", err)
		} else {
			req.Like = ex
		}
	case lex.TokenWhere:
		m.Next() // consume where
		exprNode, err := expr.ParseExprWithFuncs(m.SqlTokenPager, m.funcs)
		if err != nil {
			return nil, err
		}
		req.Where = exprNode
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

// First keyword was CREATE
func (m *Sqlbridge) parseCreate() (*SqlCreate, error) {

	req := NewSqlCreate()
	m.Next() // Consume CREATE token
	req.Raw = m.l.RawInput()

	if m.Cur().T == lex.TokenOr {
		m.Next() // Consume OR
		if m.Next().T != lex.TokenReplace {
			return nil, m.ErrMsg("Expected CREATE OR REPLACE")
		}
		req.OrReplace = true
	}
	// CREATE {DATABASE|SCHEMA|TABLE|VIEW|SOURCE|CONTINUOUSVIEW} <identity>
	switch m.Cur().T {
	case lex.TokenTable, lex.TokenSource, lex.TokenDatabase, lex.TokenSchema:
		req.Tok = m.Next()
	case lex.TokenView, lex.TokenContinuousView:
		req.Tok = m.Next()
		if m.Cur().T != lex.TokenIdentity {
			return nil, m.ErrMsg("Expected CREATE [OR REPLACE] {VIEW|CONTINIOUSVIEW} <identity> AS <select_stmt>")
		}
		req.Identity = m.Next().V

		// Grab remainder which will be SELECT (we have already lexed AS)
		selSQL, _ := m.l.Remainder()

		if m.Next().T != lex.TokenAs {
			return nil, m.ErrMsg("Expected CREATE [OR REPLACE] {VIEW|CONTINIOUSVIEW} <identity> AS <select_stmt>")
		}
		if m.Cur().T != lex.TokenSelect {
			return nil, m.ErrMsg("Expected CREATE [OR REPLACE] {VIEW|CONTINIOUSVIEW} <identity> AS <select_stmt>")
		}

		sel, err := ParseSqlSelect(selSQL)
		if err != nil {
			return nil, err
		}
		req.Select = sel
		return req, nil
	default:
		return nil, m.ErrMsg("Expected view, table, source, schema, database, continuousview for CREATE got")
	}

	// [IF NOT EXISTS]
	if m.Cur().T == lex.TokenIf {
		m.Next() // Consume IF
		if m.Next().T != lex.TokenNegate {
			return nil, m.ErrMsg("Expected CREATE {TABLE|SCHEMA|DATABASE} IF NOT EXISTS <identity>")
		}
		if m.Next().T != lex.TokenExists {
			return nil, m.ErrMsg("Expected CREATE {TABLE|SCHEMA|DATABASE} IF NOT EXISTS <identity>")
		}
		req.IfNotExists = true
	}

	switch m.Cur().T {
	case lex.TokenTable, lex.TokenIdentity:
		req.Identity = m.Next().V
	default:
		return nil, m.ErrMsg("Expected identity after CREATE (TABLE|VIEW|SOURCE|SCHEMA|DATABASE) ")
	}

	switch req.Tok.T {
	case lex.TokenTable:
		discardComments(m)
		if m.Cur().T != lex.TokenLeftParenthesis {
			return nil, m.ErrMsg("Expected (cols) ")
		}
		m.Next() // consume paren

		// list of columns comma separated
		cols, err := m.parseCreateCols()
		if err != nil {
			u.Error(err)
			return nil, err
		}
		req.Cols = cols

		// ENGINE
		discardComments(m)
		if strings.ToLower(m.Cur().V) != "engine" {
			return nil, m.ErrMsg("Expected (cols) ENGINE ... ")
		}
		engine, err := ParseWith(m.SqlTokenPager)
		if err != nil {
			return nil, err
		}
		req.Engine = engine
	case lex.TokenSource:
		// just with
	case lex.TokenSchema:
		// just with for now
	default:
		return nil, fmt.Errorf("not implemented %v", req.Tok.V)
	}

	// WITH
	discardComments(m)
	with, err := ParseWith(m.SqlTokenPager)
	if err != nil {
		return nil, err
	}
	req.With = with

	return req, nil
}

// First keyword was DROP
func (m *Sqlbridge) parseDrop() (*SqlDrop, error) {

	req := NewSqlDrop()
	m.Next() // Consume DROP token
	req.Raw = m.l.RawInput()

	// DROP TEMPORARY TABLE x
	if m.Cur().T == lex.TokenTemp {
		m.Next()
		req.Temp = true
	}

	// DROP (TABLE|VIEW|SOURCE|CONTINUOUSVIEW) <identity>
	switch m.Cur().T {
	case lex.TokenTable, lex.TokenView, lex.TokenSource, lex.TokenContinuousView,
		lex.TokenSchema, lex.TokenDatabase:
		req.Tok = m.Next()
	case lex.TokenIdentity:
		// triggers, indexes
		req.Tok = m.Next()
	default:
		return nil, m.ErrMsg("Expected view, database,schema, table, source, continuousview for DROP got")
	}

	switch m.Cur().T {
	case lex.TokenTable, lex.TokenIdentity:
		req.Identity = m.Next().V
	default:
		return nil, m.ErrMsg("Expected identity after DROP (TABLE|VIEW|SOURCE|SCHEMA|DATABASE)  ")
	}

	switch req.Tok.T {
	case lex.TokenTable:
		// just table
	case lex.TokenSource, lex.TokenSchema:
		// schema
	case lex.TokenContinuousView, lex.TokenView:
		// view
	default:
		// triggers, index, etc
	}

	// WITH
	discardComments(m)
	with, err := ParseWith(m.SqlTokenPager)
	if err != nil {
		return nil, err
	}
	req.With = with
	return req, nil
}

func (m *Sqlbridge) parseTransaction() (*SqlCommand, error) {

	// rollback, commit
	req := &SqlCommand{Columns: make(CommandColumns, 0)}
	req.kw = m.Next().T // rollback, commit

	return req, nil
}

func parseColumns(m expr.TokenPager, fr expr.FuncResolver, stmt ColumnsStatement) error {

	var col *Column

	comment := readComment(m)

	for {

		comment += readComment(m)

		//u.Debug(m.Cur())
		switch m.Cur().T {
		case lex.TokenStar, lex.TokenMultiply:
			col = &Column{Star: true}
			m.Next()
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			col = NewColumnFromToken(m.Cur())
			// function canoncial names are always lowercase
			funcName := strings.ToLower(m.Cur().V)
			exprNode, err := expr.ParseExprWithFuncs(m, fr)
			if err != nil {
				return err
			}
			col.Expr = exprNode
			col.SourceField = expr.FindFirstIdentity(col.Expr)
			if strings.Contains(col.SourceField, ".") {
				if _, right, hasLeft := expr.LeftRight(col.SourceField); hasLeft {
					col.SourceOriginal = col.SourceField
					col.SourceField = right
				}
			}

			if m.Cur().T != lex.TokenAs {
				switch n := col.Expr.(type) {
				case *expr.FuncNode:
					n.Name = funcName
					col.Agg = n.F.Aggregate
					col.As = expr.FindIdentityName(0, n, "")
					if col.As == "" {
						if n.Name == "count" {
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
					col.Agg = n.F.Aggregate
				}
			}
			//u.Debugf("next? %v", m.Cur())

		case lex.TokenIdentity:
			col = NewColumnFromToken(m.Cur())
			exprNode, err := expr.ParseExprWithFuncs(m, fr)
			if err != nil {
				return err
			}
			col.Expr = exprNode
		case lex.TokenValue, lex.TokenInteger:
			// Value Literal
			col = NewColumnValue(m.Cur())
			exprNode, err := expr.ParseExprWithFuncs(m, fr)
			if err != nil {
				return err
			}
			col.Expr = exprNode
		}
		//u.Debugf("after colstart?:   %v  ", m.Cur())
		comment += readComment(m)

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
			return m.ErrMsg("expected identity")
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			col.Comment = comment
			stmt.AddColumn(*col)
			return nil
		case lex.TokenIf:
			// If guard
			m.Next()
			exprNode, err := expr.ParseExprWithFuncs(m, fr)
			if err != nil {
				return err
			}
			col.Guard = exprNode
			// Hm, we need to backup here?  Parse Node went to deep?
			continue
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			if col == nil {
				return m.ErrMsg("Expected Column Expression")
			}
			col.Comment = comment
			stmt.AddColumn(*col)
			comment = ""
		default:
			return m.ErrMsg("Expected Column Expression")
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseFieldList() (Columns, error) {

	if m.Cur().T != lex.TokenLeftParenthesis {
		return nil, m.ErrMsg("Expecting opening paren")
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
			return nil, m.ErrMsg("expected column but")
		}
		m.Next()
	}
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
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return nil, err
			}
			cols[lastColName] = &ValueColumn{Expr: exprNode}
		default:
			u.Warnf("don't know how to handle ?  %v", m.Cur())
			return nil, m.ErrMsg("expected column")
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseValueList() ([][]*ValueColumn, error) {

	if m.Cur().T != lex.TokenLeftParenthesis {
		return nil, m.ErrMsg("Expecting opening paren ( ")
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
			arrayVal, err := expr.ValueArray(0, m.SqlTokenPager)
			if err != nil {
				return nil, err
			}
			row = append(row, &ValueColumn{Value: arrayVal})
			u.Infof("what is token?  %v peek:%v", m.Cur(), m.Peek())
		case lex.TokenComma:
			// don't need to do anything
		case lex.TokenUdfExpr:
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return nil, err
			}
			row = append(row, &ValueColumn{Expr: exprNode})
		default:
			u.Warnf("don't know how to handle ?  %v", m.Cur())
			return nil, m.ErrMsg("expected column")
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseSources(req *SqlSelect) error {

	discardComments(m)

	if m.Cur().T != lex.TokenFrom {
		return m.ErrMsg("expected From")
	}

	m.Next() // consume From

	if m.Cur().T == lex.TokenIdentity {
		if err := m.parseSourceTable(req); err != nil {
			return err
		}
	}

	for {

		discardComments(m)

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
			return m.ErrMsg("unexpected token")
		}

		discardComments(m)

		switch m.Cur().T {
		case lex.TokenAs:
			m.Next() // Skip over As, we don't need it
			src.Alias = m.Cur().V
			m.Next()
			// select u.name, order.date FROM user AS u INNER JOIN ....
		case lex.TokenIdentity:
			src.Alias = m.Cur().V
			m.Next()
		}
		if m.Cur().T == lex.TokenOn {
			src.Op = m.Cur().T
			m.Next()
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			src.JoinExpr = exprNode
		}
		req.From = append(req.From, src)
	}
}

func (m *Sqlbridge) parseSourceSubQuery(src *SqlSource) error {

	m.Next() // page forward off of (

	// SELECT * FROM (SELECT 1, 2, 3) AS t1;
	subQuery, err := m.parseSqlSelect()
	if err != nil {
		return err
	}
	src.SubQuery = subQuery
	subQuery.Raw = subQuery.String()

	if m.Cur().T != lex.TokenRightParenthesis {
		return m.ErrMsg("expected right paren ) ")
	}
	m.Next() // discard right paren
	return nil
}

func (m *Sqlbridge) parseSourceTable(req *SqlSelect) error {

	if m.Cur().T != lex.TokenIdentity {
		return m.ErrMsg("expected tablename")
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

	switch m.Cur().T {
	case lex.TokenLeft, lex.TokenRight:
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
		return m.ErrMsg("Requires join")
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
		return m.ErrMsg("unrecognized kw in join")
	}
	return nil
}

func (m *Sqlbridge) parseInto(req *SqlSelect) error {

	if m.Cur().T != lex.TokenInto {
		return nil
	}
	m.Next() // Consume Into token
	if m.Cur().T != lex.TokenTable {
		return m.ErrMsg("expected table")
	}
	if strings.ToLower(m.Cur().V) == "FROM" {
		return m.ErrMsg("expected table")
	}
	req.Into = &SqlInto{Table: m.Cur().V}
	m.Next()
	return nil
}

func (m *Sqlbridge) parseWhereSubSelect(req *SqlSelect) error {

	if m.Cur().T != lex.TokenSelect {
		return nil
	}
	stmt, err := m.parseSqlSelect()
	if err != nil {
		return err
	}
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
	exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
	if err != nil {
		return nil, err
	}
	where.Expr = exprNode
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
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Expr = exprNode

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
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Expr = exprNode
		case lex.TokenValue:
			// Value Literal
			col = NewColumnFromToken(m.Cur())
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Expr = exprNode
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
			return m.ErrMsg("expected identity")
		case lex.TokenFrom, lex.TokenOrderBy, lex.TokenInto, lex.TokenLimit, lex.TokenHaving,
			lex.TokenWith, lex.TokenEOS, lex.TokenEOF:

			// This indicates we have come to the End of the columns
			req.GroupBy = append(req.GroupBy, col)
			return nil
		case lex.TokenIf:
			// If guard
			m.Next()
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Guard = exprNode
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			req.GroupBy = append(req.GroupBy, col)
		default:
			return m.ErrMsg("expected column of group by ")
		}
		m.Next()
	}
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
	exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
	if err != nil {
		return err
	}
	req.Having = exprNode
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
			col = NewColumnFromToken(m.Cur())
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Expr = exprNode
			switch n := col.Expr.(type) {
			case *expr.FuncNode:
				col.As = expr.FindIdentityName(0, n, "")
				if col.As == "" {
					col.As = n.Name
				}
			case *expr.BinaryNode:
				col.As = expr.FindIdentityName(0, n, "")
				if col.As == "" {
					u.Errorf("could not find as name: %#v", n)
				}
			}
		case lex.TokenIdentity:
			col = NewColumnFromToken(m.Cur())
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Expr = exprNode
		}
		//u.Debugf("OrderBy after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAsc, lex.TokenDesc:
			col.Order = strings.ToUpper(m.Cur().V)

		case lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			req.OrderBy = append(req.OrderBy, col)
			return nil
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			req.OrderBy = append(req.OrderBy, col)
		default:
			return m.ErrMsg("expected order by column")
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseWhereDelete(req *SqlDelete) error {
	if m.Cur().T != lex.TokenWhere {
		return nil
	}
	m.Next()
	exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
	if err != nil {
		return err
	}
	req.Where = &SqlWhere{Expr: exprNode}
	return nil
}

func (m *Sqlbridge) parseCommandColumns(req *SqlCommand) (err error) {

	var col *CommandColumn

	for {

		//u.Debugf("command col? %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenIdentity:

			col = &CommandColumn{Name: m.Cur().V}
			exprNode, err := expr.ParseExprWithFuncs(m, m.funcs)
			if err != nil {
				return err
			}
			col.Expr = exprNode
			convertIdentityToValue(col.Expr)

		default:
			return m.ErrMsg("expected idenity")
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
			return m.ErrMsg("expected command column")
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseCreateCols() ([]*DdlColumn, error) {

	cols := make([]*DdlColumn, 0)
	var col *DdlColumn
	/*
		CREATE TABLE articles (
		  ID int(11) NOT NULL AUTO_INCREMENT,
		  Email char(150) NOT NULL DEFAULT '',
		  PRIMARY KEY (ID),
		  CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email)
		)
	*/
	for {

		discardComments(m)
		switch m.Cur().T {
		case lex.TokenIdentity:
			col = &DdlColumn{Name: strings.ToLower(m.Next().V), Kw: lex.TokenIdentity}
			if err := m.parseDdlColumn(col); err != nil {
				return nil, err
			}
		case lex.TokenConstraint:
			col = &DdlColumn{Kw: m.Next().T}
			if err := m.parseDdlConstraint(col); err != nil {
				return nil, err
			}
		case lex.TokenPrimary:
			col = &DdlColumn{Kw: m.Next().T}
			if strings.ToLower(m.Next().V) != "key" {
				return nil, m.ErrMsg("expected 'PRIMARY KEY'")
			}
			if m.Next().T != lex.TokenLeftParenthesis {
				return nil, m.ErrMsg("expected 'PRIMARY KEY (field)'")
			}

		PrimaryKeyLoop:
			for {

				switch m.Cur().T {
				case lex.TokenRightParenthesis:
					m.Next() // consume )
					break PrimaryKeyLoop
				case lex.TokenIdentity:
					col = &DdlColumn{Name: strings.ToLower(m.Next().V), Kw: lex.TokenIdentity}
				case lex.TokenConstraint:
					col = &DdlColumn{Kw: m.Next().T}
				case lex.TokenPrimary:
					col = &DdlColumn{Kw: m.Next().T}
					if strings.ToLower(m.Cur().V) != "key" {
						return nil, m.ErrMsg("expected 'PRIMARY KEY'")
					}
					m.Next()

				default:
					return nil, m.ErrMsg("expected identity")
				}
			}

		default:
			return nil, m.ErrMsg("expected identity")
		}

		// since we can have multiple columns
		switch m.Cur().T {
		case lex.TokenRightParenthesis:
			m.Next()
			cols = append(cols, col)
			return cols, nil
		case lex.TokenComma:
			cols = append(cols, col)
		default:
			return nil, m.ErrMsg("expected create column statement")
		}
		m.Next()
	}
}

func (m *Sqlbridge) parseDdlConstraint(col *DdlColumn) error {

	/*
		http://dev.mysql.com/doc/refman/5.7/en/create-table.html

		create_definition:
		    col_name column_definition
		  | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
		      [index_option] ...
		  | {INDEX|KEY} [index_name] [index_type] (index_col_name,...)
		      [index_option] ...
		  | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY]
		      [index_name] [index_type] (index_col_name,...)
		      [index_option] ...
		  | {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (index_col_name,...)
		      [index_option] ...
		  | [CONSTRAINT [symbol]] FOREIGN KEY
		      [index_name] (index_col_name,...) reference_definition
		  | CHECK (expr)


		index_type:
			USING {BTREE | HASH}
		reference_definition:
		    REFERENCES tbl_name (index_col_name,...)
		      [MATCH FULL | MATCH PARTIAL | MATCH SIMPLE]
		      [ON DELETE reference_option]
		      [ON UPDATE reference_option]

		CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email) COMMENT "hello constraint"
	*/

	if m.Cur().T != lex.TokenIdentity {
		return m.ErrMsg("expected 'CONSTRAINT <identity>'")
	}
	col.Name = m.Next().V

	switch m.Cur().T {
	case lex.TokenTypeDef, lex.TokenTypeBool, lex.TokenTypeTime,
		lex.TokenTypeText, lex.TokenTypeJson:

		col.DataType = m.Next().V
	case lex.TokenTypeFloat, lex.TokenTypeInteger, lex.TokenTypeString,
		lex.TokenTypeVarChar, lex.TokenTypeChar, lex.TokenTypeBigInt:
		col.DataType = m.Next().V
		if m.Cur().T == lex.TokenLeftParenthesis {
			m.Next()
			if m.Cur().T != lex.TokenInteger {
				return m.ErrMsg("expected 'type(integer)'")
			}
			iv, err := strconv.ParseInt(m.Next().V, 10, 64)
			if err != nil {
				return m.ErrMsg("Expected integer")
			}
			col.DataTypeSize = int(iv)
			if m.Next().T != lex.TokenRightParenthesis {
				m.Backup()
				return m.ErrMsg("expected 'type(integer)'")
			}
		}
	default:
		col.Null = true
	}

	// [UNIQUE [KEY] | [PRIMARY] KEY]
	switch m.Cur().T {
	case lex.TokenUnique:
		col.Key = m.Next().T
	case lex.TokenPrimary:
		col.Key = m.Next().T
	case lex.TokenForeign:
		col.Key = m.Next().T
	}
	if m.Cur().T == lex.TokenKey {
		m.Next()
	}

	// [index_type]
	// index_type:
	//    USING {BTREE | HASH}
	if strings.ToLower(m.Cur().V) == "using" {
		m.Next()
		col.IndexType = m.Next().V
	}

	if m.Cur().T == lex.TokenLeftParenthesis {
		m.Next()
	indexCol:
		for {
			switch m.Cur().T {
			case lex.TokenRightParenthesis:
				m.Next() // consume )
				break indexCol
			case lex.TokenIdentity:
				col.IndexCols = append(col.IndexCols, strings.ToLower(m.Next().V))
			default:
				return m.ErrMsg("Expected identity")
			}
		}
	}

	if strings.ToLower(m.Cur().V) == "references" {
		m.Next()
		col.RefTable = m.Next().V
		if m.Cur().T == lex.TokenLeftParenthesis {
			m.Next()
		refCol:
			for {
				switch m.Cur().T {
				case lex.TokenRightParenthesis:
					m.Next() // consume )
					break refCol
				case lex.TokenIdentity:
					col.RefCols = append(col.RefCols, strings.ToLower(m.Next().V))
				default:
					return m.ErrMsg("Expected identity")
				}
			}
		}
	}

	// [COMMENT 'string']
	if strings.ToLower(m.Cur().V) == "comment" {
		m.Next()
		col.Comment = m.Next().V
	}

	// since we can have multiple columns
	return nil
}

func (m *Sqlbridge) parseDdlColumn(col *DdlColumn) error {

	/*
		http://dev.mysql.com/doc/refman/5.7/en/create-table.html

		create_definition:
		    col_name column_definition

		column_definition:
		    data_type [NOT NULL | NULL] [DEFAULT default_value]
		      [AUTO_INCREMENT] [UNIQUE [KEY] | [PRIMARY] KEY]
		      [COMMENT 'string']
		      [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
		      [STORAGE {DISK|MEMORY|DEFAULT}]
		      [reference_definition]
		  | data_type [GENERATED ALWAYS] AS (expression)
		      [VIRTUAL | STORED] [UNIQUE [KEY]] [COMMENT comment]
		      [NOT NULL | NULL] [[PRIMARY] KEY]

		  ID int(11) NOT NULL AUTO_INCREMENT,
		  Email char(150) NOT NULL DEFAULT '',
	*/

	//u.Debugf("create col after colstart?:   %v  ", m.Cur())

	switch m.Cur().T {
	case lex.TokenTypeDef, lex.TokenTypeBool, lex.TokenTypeTime,
		lex.TokenTypeText, lex.TokenTypeJson:

		col.DataType = m.Next().V
	case lex.TokenTypeFloat, lex.TokenTypeInteger, lex.TokenTypeString,
		lex.TokenTypeVarChar, lex.TokenTypeChar, lex.TokenTypeBigInt:
		col.DataType = m.Next().V
		if m.Cur().T == lex.TokenLeftParenthesis {
			m.Next()
			if m.Cur().T != lex.TokenInteger {
				return m.ErrMsg("expected 'type(integer)'")
			}
			iv, err := strconv.ParseInt(m.Next().V, 10, 64)
			if err != nil {
				return m.ErrMsg("Expected integer")
			}
			col.DataTypeSize = int(iv)
			if m.Next().T != lex.TokenRightParenthesis {
				m.Backup()
				return m.ErrMsg("expected 'type(integer)'")
			}
		}
	default:
		col.Null = true
	}

	// [NOT NULL | NULL]
	switch m.Cur().T {
	case lex.TokenNegate:
		m.Next()
		if m.Cur().T == lex.TokenNull {
			col.Null = false
		}
		m.Next()
	case lex.TokenNull:
		m.Next()
		col.Null = true
	default:
		col.Null = true
	}

	// [DEFAULT default_value]
	switch m.Cur().T {
	case lex.TokenDefault:
		m.Next() // Consume DEFAULT token
		col.Default = expr.NewStringNode(m.Next().V)
	}

	// [AUTO_INCREMENT]
	switch strings.ToLower(m.Cur().V) {
	case "auto_increment":
		m.Next()
		col.AutoIncrement = true
	}

	// [UNIQUE [KEY] | [PRIMARY] KEY]
	switch m.Cur().T {
	case lex.TokenUnique:
		col.Key = m.Next().T
	case lex.TokenPrimary:
		col.Key = m.Next().T
	}
	if m.Cur().T == lex.TokenKey {
		m.Next()
	}

	// [COMMENT 'string']
	if strings.ToLower(m.Cur().V) == "comment" {
		m.Next()
		col.Comment = m.Next().V
	}

	return nil
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
		return m.ErrMsg("Limit must be an integer")
	}
	limval := m.Next()
	iv, err := strconv.Atoi(limval.V)
	if err != nil {
		return m.ErrMsg("Could not convert limit to integer")
	}
	req.Limit = int(iv)

	switch m.Cur().T {
	case lex.TokenComma:
		// LIMIT 0, 1000
		m.Next() // consume the comma
		if m.Cur().T != lex.TokenInteger {
			return m.ErrMsg("Limit 0, 1000 2nd number must be an integer")
		}
		iv, err = strconv.Atoi(m.Next().V)
		if err != nil {
			return m.ErrMsg("Could not convert limit to integer")
		}
		req.Offset = req.Limit
		req.Limit = iv
	case lex.TokenOffset:
		m.Next() // consume "OFFSET"
		if m.Cur().T != lex.TokenInteger {
			return m.ErrMsg("Offset must be an integer")
		}
		iv, err = strconv.Atoi(m.Cur().V)
		m.Next()
		if err != nil {
			return m.ErrMsg("Could not convert offset to integer")
		}
		req.Offset = iv
	}
	return nil
}
func (m *Sqlbridge) parseOffset(req *SqlSelect) error {
	if m.Cur().T != lex.TokenOffset {
		return nil
	}
	m.Next() // Consume "OFFSET"
	if m.Cur().T != lex.TokenInteger && m.Cur().T != lex.TokenValue {
		return m.ErrMsg("Expected Integer/Value for OFFSET")
	}
	iv, err := strconv.Atoi(m.Cur().V)
	m.Next()
	if err != nil {
		return m.ErrMsg("Could not convert offset to integer")
	}
	req.Offset = iv
	return nil
}
func (m *Sqlbridge) parseAlias(req *SqlSelect) error {
	if m.Cur().T != lex.TokenAlias {
		return nil
	}
	m.Next()
	if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
		return m.ErrMsg("Expected identity for Alias")
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
		return nil, pg.ErrMsg("Expected json { , or name=value")
	}
	return jh, nil
}

func (m *Sqlbridge) parseShowFromTable(req *SqlShow) error {

	switch m.Cur().T {
	case lex.TokenFrom, lex.TokenIN:
		m.Next() // Consume {FROM | IN}
	default:
		// FROM OR IN are required for this statement
		return m.ErrMsg("Expected { FROM | IN } for SHOW")
	}

	if m.Cur().T != lex.TokenIdentity {
		return m.ErrMsg("Expected { FROM | IN } IDENTITY for SHOW")
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
		return fmt.Errorf("Expected { FROM | IN } IDENTITY for SHOW")
	}
	req.Db = m.Next().V
	return nil
}

func ParseJsonObject(pg expr.TokenPager, jh u.JsonHelper) error {
	if pg.Cur().T != lex.TokenLeftBrace {
		return pg.ErrMsg("Expected json {")
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
			return pg.ErrMsg("Expected json key identity")
		}
		switch pg.Cur().T {
		case lex.TokenComma:
			pg.Next()
		case lex.TokenRightBrace:
			pg.Next() // Consume the right }
			return nil
		default:
			return pg.ErrMsg("Expected json comma or end of object")
		}
	}
}
func parseJsonKeyValue(pg expr.TokenPager, jh u.JsonHelper) error {
	if pg.Cur().T != lex.TokenIdentity {
		return pg.ErrMsg("Expected json key/identity")
	}
	key := pg.Cur().V
	pg.Next()
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
			return pg.ErrMsg("Expected json { or [")
		}
		return nil
	default:
		return pg.ErrMsg("Expected json colon")
	}
}

func ParseJsonArray(pg expr.TokenPager) ([]interface{}, error) {
	if pg.Cur().T != lex.TokenLeftBracket {
		return nil, pg.ErrMsg("Expected json [")
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
			return nil, pg.ErrMsg("Expected json key identity")
		}
		switch pg.Cur().T {
		case lex.TokenComma:
			pg.Next()
		case lex.TokenRightBracket:
			pg.Next() // Consume the right ]
			return la, nil
		default:
			return nil, pg.ErrMsg("Expected json comma or end of array ]")
		}
	}
}

func ParseKeyValue(pg expr.TokenPager, jh u.JsonHelper) error {
	if pg.Cur().T != lex.TokenIdentity {
		return pg.ErrMsg("Expected key/identity for key=value, array")
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
			return pg.ErrMsg("Expected value")
		}
		pg.Next() // consume value
		if pg.Cur().T != lex.TokenComma {
			return nil
		}
		pg.Next() // consume comma
	}
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
	switch m.Cur().T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenError:
		return true
	}
	return m.LexTokenPager.IsEnd()
}
func (m *SqlTokenPager) ClauseEnd() bool {
	tok := m.Cur()
	switch tok.T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenHaving, lex.TokenComma,
		lex.TokenIf, lex.TokenAs, lex.TokenLimit, lex.TokenSelect:
		return true
	}
	return false
}
