package expr

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// Ensure SqlSelect and cousins etc are NodeTypes as well as SqlStatements
	_ SqlStatement    = (*SqlSelect)(nil)
	_ SqlStatement    = (*SqlInsert)(nil)
	_ SqlStatement    = (*SqlUpsert)(nil)
	_ SqlStatement    = (*SqlUpdate)(nil)
	_ SqlStatement    = (*SqlDelete)(nil)
	_ SqlStatement    = (*SqlShow)(nil)
	_ SqlStatement    = (*SqlDescribe)(nil)
	_ SqlStatement    = (*SqlCommand)(nil)
	_ SqlSubStatement = (*SqlSource)(nil)
	_ Node            = (*SqlWhere)(nil)
	_ Node            = (*SqlInto)(nil)

	// A select * columns
	starCols Columns
)

func init() {
	starCols = make(Columns, 1)
	starCols[0] = NewColumnFromToken(lex.Token{T: lex.TokenStar, V: "*"})
}

// The sqlStatement interface, to define the sql-types
//  Select, Insert, Delete etc
type SqlStatement interface {
	Node
	Accept(visitor Visitor) (interface{}, error)
	Keyword() lex.TokenType
}

// The sqlStatement interface, to define the subselect/join-types
//   Join, SubSelect, From
type SqlSubStatement interface {
	Node
	Accept(visitor SubVisitor) (interface{}, error)
	Keyword() lex.TokenType
}

type (
	// Prepared/Aliased SQL
	PreparedStatement struct {
		Alias     string
		Statement SqlStatement
	}
	// SQL Select statement
	SqlSelect struct {
		Db      string       // If provided a use "dbname"
		Raw     string       // full original raw statement
		Star    bool         // for select * from ...
		Columns Columns      // An array (ordered) list of columns
		From    []*SqlSource // From, Join
		Into    *SqlInto     // Into "table"
		Where   *SqlWhere    // Expr Node, or *SqlSelect
		Having  Node         // Filter results
		GroupBy Columns
		OrderBy Columns
		Limit   int
		Offset  int
		Alias   string       // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With    u.JsonHelper // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
		proj    *Projection  // Projected fields
	}
	// Source is a table name, sub-query, or join as used in
	// SELECT .. FROM SQLSOURCE
	//  - SELECT .. FROM table_name
	//  - SELECT .. from (select a,b,c from tableb)
	//  - SELECT .. FROM tablex INNER JOIN ...
	SqlSource struct {
		final       bool               // has this been finalized?
		alias       string             // either the short table name or full
		Raw         string             // Raw Partial
		Name        string             // From Name (optional, empty if join, subselect)
		Alias       string             // From name aliased
		Op          lex.TokenType      // In, =, ON
		LeftOrRight lex.TokenType      // Left, Right
		JoinType    lex.TokenType      // INNER, OUTER
		Source      *SqlSelect         // optional, Join or SubSelect statement
		JoinExpr    Node               // Join expression       x.y = q.y
		cols        map[string]*Column // Un-aliased columns
		colIndex    map[string]int     // Key(alias) to index in []driver.Value positions

		// If we do have to rewrite statement these are used to store the
		// info on the rew-writtern query
		// TODO:  move this into private  *SqlSelect field instead
		Into    string
		Star    bool      // all ?
		Columns Columns   // cols
		Where   *SqlWhere // Expr Node, or *SqlSelect
	}
	// WHERE is select stmt, or set of expressions
	// - WHERE x in (select name from q)
	// - WHERE x = y
	// - WHERE x = y AND z = q
	// - WHERE tolower(x) IN (select name from q)
	SqlWhere struct {
		Op     lex.TokenType // In, =, ON     for Select Clauses operators
		Source *SqlSelect
		Expr   Node
	}
	SqlInsert struct {
		kw      lex.TokenType // Insert, Replace
		Table   string
		Columns Columns // Column Names
		Rows    [][]*ValueColumn
		Select  *SqlSelect
	}
	SqlUpsert struct {
		Columns Columns
		Rows    [][]*ValueColumn
		Values  map[string]*ValueColumn
		Where   Node
		Table   string
	}
	SqlUpdate struct {
		Values map[string]*ValueColumn
		Where  Node
		Table  string
	}
	SqlDelete struct {
		Table string
		Where Node
		Limit int
	}
	SqlShow struct {
		Raw      string
		Identity string
		From     string
		Full     bool
	}
	SqlDescribe struct {
		Identity string
		Tok      lex.Token // Explain, Describe, Desc
		Stmt     SqlStatement
	}
	SqlInto struct {
		Table string
	}
	SqlCommand struct {
		kw       lex.TokenType // SET
		Columns  CommandColumns
		Identity string
		Value    Node
	}
	// List of Columns in SELECT [columns]
	Columns []*Column
	// Column represents the Column as expressed in a [SELECT]
	// expression
	Column struct {
		sourceQuoteByte byte
		asQuoteByte     byte
		originalAs      string
		left            string
		right           string
		ParentIndex     int    // Field position in parent query cols
		Index           int    // Field Position Order in original query
		SourceField     string // field name of underlying field
		As              string // As field, auto-populate the Field Name if exists
		Comment         string // optional in-line comments
		Order           string // (ASC | DESC)
		Star            bool   // If   just *
		Expr            Node   // Expression, optional, often Identity.Node
		Guard           Node   // If
	}
	// List of Value columns in INSERT into TABLE (colnames) VALUES (valuecolumns)
	ValueColumn struct {
		Value value.Value
		Expr  Node
	}
	ResultColumns []*ResultColumn
	// Result Column
	ResultColumn struct {
		//Expr   Node            // If expression, is here
		Name   string          // Original path/name for query field
		ColPos int             // Ordinal position in sql statement
		Col    *Column         // the original sql column
		Star   bool            // Was this a select * ??
		As     string          // aliased
		Type   value.ValueType // Data Type
	}
	// Projection is just the ResultColumns for a result-set
	Projection struct {
		Distinct bool
		Columns  ResultColumns
	}
	// SQL commands such as:
	//     set autocommit
	//     SET @@local.sort_buffer_size=10000;
	//     USE myschema;
	CommandColumns []*CommandColumn
	CommandColumn  struct {
		Expr Node   // column expression
		Name string // Original path/name for command field
	}
)

func NewProjection() *Projection {
	return &Projection{Columns: make(ResultColumns, 0)}
}
func NewResultColumn(as string, ordinal int, col *Column, valtype value.ValueType) *ResultColumn {
	return &ResultColumn{Name: as, As: as, ColPos: ordinal, Col: col, Type: valtype}
}
func NewSqlSelect() *SqlSelect {
	req := &SqlSelect{}
	req.Columns = make(Columns, 0)
	return req
}
func NewSqlInsert() *SqlInsert {
	req := &SqlInsert{}
	req.Columns = make(Columns, 0)
	return req
}
func NewSqlUpdate() *SqlUpdate {
	req := &SqlUpdate{}
	return req
}
func NewSqlUpsert() *SqlUpsert {
	req := &SqlUpsert{}
	return req
}
func NewSqlDelete() *SqlDelete {
	return &SqlDelete{}
}
func NewPreparedStatement() *PreparedStatement {
	return &PreparedStatement{}
}
func NewSqlInto(table string) *SqlInto {
	return &SqlInto{Table: table}
}
func NewSqlSource(table string) *SqlSource {
	return &SqlSource{Name: table}
}
func NewSqlWhere(where Node) *SqlWhere {
	return &SqlWhere{Expr: where}
}
func NewColumnFromToken(tok lex.Token) *Column {
	return &Column{
		As:              tok.V,
		sourceQuoteByte: tok.Quote,
		asQuoteByte:     tok.Quote,
		SourceField:     tok.V,
	}
}

func (m *Projection) AddColumnShort(name string, vt value.ValueType) {
	m.Columns = append(m.Columns, NewResultColumn(name, len(m.Columns), nil, vt))
}

func (m *Columns) FingerPrint(r rune) string {
	colCt := len(*m)
	if colCt == 1 {
		return (*m)[0].FingerPrint(r)
	} else if colCt == 0 {
		return ""
	}

	s := make([]string, len(*m))
	for i, col := range *m {
		s[i] = col.FingerPrint(r)
	}

	return strings.Join(s, ", ")
}
func (m *Columns) writeBuf(buf *bytes.Buffer) {
	colCt := len(*m)
	if colCt == 1 {
		(*m)[0].writeBuf(buf)
		return
	} else if colCt == 0 {
		return
	}
	for i, col := range *m {
		if i != 0 {
			buf.WriteString(", ")
		}
		col.writeBuf(buf)
	}
}
func (m *Columns) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}
func (m *Columns) FieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		names[i] = col.Key()
	}
	return names
}
func (m *Columns) UnAliasedFieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		_, right, _ := col.LeftRight()
		names[i] = right
	}
	return names
}
func (m *Columns) ByName(name string) (*Column, bool) {
	for _, col := range *m {
		//u.Debugf("col.SourceField='%s' key()='%s' As='%s' ", col.SourceField, col.Key(), col.As)
		if col.SourceField == name {
			return col, true
		}
	}
	return nil, false
}
func (m *Columns) ByAs(as string) (*Column, bool) {
	for _, col := range *m {
		if col.As == as {
			return col, true
		}
	}
	return nil, false
}

func (m *Column) Key() string { return m.As }
func (m *Column) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}
func (m *Column) writeBuf(buf *bytes.Buffer) {
	if m.Star {
		buf.WriteByte('*')
		return
	}
	exprStr := ""
	if m.Expr != nil {
		exprStr = m.Expr.String()
		buf.WriteString(exprStr)
		//u.Debugf("has expr: %T %#v  str=%s=%s", m.Expr, m.Expr, m.Expr.String(), exprStr)
	}
	if m.asQuoteByte != 0 && m.originalAs != "" {
		as := string(m.asQuoteByte) + m.originalAs + string(m.asQuoteByte)
		//u.Warnf("%s", as)
		buf.WriteString(fmt.Sprintf(" AS %v", as))
	} else if m.originalAs != "" && exprStr != m.originalAs {
		//u.Warnf("%s", m.originalAs)
		buf.WriteString(fmt.Sprintf(" AS %v", m.originalAs))
	} else if m.Expr == nil {
		//u.Warnf("wat? %#v", m)
		buf.WriteString(m.As)
	}
	if m.Guard != nil {
		buf.WriteString(fmt.Sprintf(" IF %s ", m.Guard.String()))
	}
	if m.Order != "" {
		buf.WriteString(fmt.Sprintf(" %s", m.Order))
	}
}
func (m *Column) FingerPrint(r rune) string {
	if m.Star {
		return "*"
	}
	buf := bytes.Buffer{}
	exprStr := ""
	if m.Expr != nil {
		exprStr = m.Expr.FingerPrint(r)
		buf.WriteString(exprStr)
		//u.Debugf("has expr: %T %#v  str=%s=%s", m.Expr, m.Expr, m.Expr.FingerPrint(r), exprStr)
	}
	if m.asQuoteByte != 0 && m.originalAs != "" {
		as := string(m.asQuoteByte) + m.originalAs + string(m.asQuoteByte)
		//u.Warnf("%s", as)
		buf.WriteString(fmt.Sprintf(" AS %v", as))
	} else if m.originalAs != "" && exprStr != m.originalAs {
		//u.Warnf("%s", m.originalAs)
		buf.WriteString(fmt.Sprintf(" AS %v", m.originalAs))
	} else if m.Expr == nil {
		//u.Warnf("wat? %#v", m)
		buf.WriteString(m.As)
	}
	if m.Guard != nil {
		buf.WriteString(fmt.Sprintf(" IF %s ", m.Guard.FingerPrint(r)))
	}
	if m.Order != "" {
		buf.WriteString(fmt.Sprintf(" %s", m.Order))
	}
	return buf.String()
}

// Is this a select count(*) column
func (m *Column) CountStar() bool {
	if m.Expr == nil {
		return false
	}
	if m.Expr.NodeType() != FuncNodeType {
		return false
	}
	if fn, ok := m.Expr.(*FuncNode); ok {
		u.Infof("countStar? %T  %#v", m.Expr, m.Expr)
		u.Debugf("args? %s", fn.Args[0].String())
		return strings.ToLower(fn.Name) == "count" && fn.Args[0].String() == `*`
	}
	return false
}

// Create a new copy of this column for rewrite purposes re-alias
//
func (m *Column) CopyRewrite(alias string) *Column {
	left, right, _ := m.LeftRight()
	newCol := &Column{
		sourceQuoteByte: m.sourceQuoteByte,
		asQuoteByte:     m.asQuoteByte,
		SourceField:     m.SourceField,
		As:              m.right,
		originalAs:      right,
	}
	//Expr:            m.Expr,
	//u.Warnf("in rewrite:  Alias:'%s'  '%s'.'%s'  sourcefield:'%v' ok?%v", alias, left, right, m.SourceField, ok)
	if left == alias {
		newCol.SourceField = right
		newCol.right = right
	}
	newCol.Expr = &IdentityNode{Text: right}
	//u.Infof("%s", newCol.String())
	return newCol
}
func (m *Column) Copy() *Column {
	return &Column{
		sourceQuoteByte: m.sourceQuoteByte,
		asQuoteByte:     m.asQuoteByte,
		originalAs:      m.originalAs,
		SourceField:     m.SourceField,
		As:              m.right,
		Comment:         m.Comment,
		Order:           m.Order,
		Star:            m.Star,
		Expr:            m.Expr,
		Guard:           m.Guard,
	}
}

// Return left, right values if is of form   `table.column` and
// also return true/false for if it even has left/right
func (m *Column) LeftRight() (string, string, bool) {
	if m.right == "" {
		vals := strings.SplitN(m.As, ".", 2)
		if len(vals) == 1 {
			m.right = m.As
		} else {
			m.left = vals[0]
			m.right = vals[1]
		}
	}
	return m.left, m.right, m.left != ""
}

func (m *PreparedStatement) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitPreparedStmt(m)
}
func (m *PreparedStatement) Keyword() lex.TokenType { return lex.TokenPrepare }
func (m *PreparedStatement) Check() error           { return m.Check() }
func (m *PreparedStatement) Type() reflect.Value    { return nilRv }
func (m *PreparedStatement) NodeType() NodeType     { return SqlPreparedType }
func (m *PreparedStatement) String() string {
	return fmt.Sprintf("PREPARE %s FROM %s", m.Alias, m.Statement.String())
}
func (m *PreparedStatement) FingerPrint(r rune) string {
	return fmt.Sprintf("PREPARE %s FROM %s", m.Alias, m.Statement.FingerPrint(r))
}

func (m *SqlSelect) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitSelect(m) }
func (m *SqlSelect) Keyword() lex.TokenType                      { return lex.TokenSelect }
func (m *SqlSelect) Check() error                                { return nil }
func (m *SqlSelect) NodeType() NodeType                          { return SqlSelectNodeType }
func (m *SqlSelect) Type() reflect.Value                         { return nilRv }
func (m *SqlSelect) String() string {
	buf := bytes.Buffer{}
	buf.WriteString("SELECT ")
	m.Columns.writeBuf(&buf)
	if m.Into != nil {
		buf.WriteString(fmt.Sprintf(" INTO %v", m.Into))
	}
	if m.From != nil {
		buf.WriteString(" FROM")
		for _, from := range m.From {
			buf.WriteByte(' ')
			from.writeBuf(&buf)
		}
	}
	if m.Where != nil {
		buf.WriteString(" WHERE ")
		m.Where.writeBuf(&buf)
	}
	if m.GroupBy != nil {
		buf.WriteString(" GROUP BY ")
		m.GroupBy.writeBuf(&buf)
	}
	if m.Having != nil {
		buf.WriteString(fmt.Sprintf(" HAVING %s", m.Having.String()))
	}
	if m.OrderBy != nil {
		buf.WriteString(fmt.Sprintf(" ORDER BY %s", m.OrderBy.String()))
	}
	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Offset > 0 {
		buf.WriteString(fmt.Sprintf(" OFFSET %d", m.Offset))
	}
	return buf.String()
}
func (m *SqlSelect) FingerPrint(r rune) string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("SELECT %s", m.Columns.FingerPrint(r)))
	if m.Into != nil {
		buf.WriteString(fmt.Sprintf(" INTO %v", m.Into))
	}
	if m.From != nil {
		buf.WriteString(" FROM")
		for _, from := range m.From {
			buf.WriteByte(' ')
			buf.WriteString(from.FingerPrint(r))
		}
	}
	if m.Where != nil {
		buf.WriteString(fmt.Sprintf(" WHERE %s", m.Where.FingerPrint(r)))
	}
	if m.GroupBy != nil {
		buf.WriteString(fmt.Sprintf(" GROUP BY %s", m.GroupBy.FingerPrint(r)))
	}
	if m.Having != nil {
		buf.WriteString(fmt.Sprintf(" HAVING %s", m.Having.FingerPrint(r)))
	}
	if m.OrderBy != nil {
		buf.WriteString(fmt.Sprintf(" ORDER BY %s", m.OrderBy.FingerPrint(r)))
	}
	if m.Limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Offset > 0 {
		// Don't think we write this out for fingerprint
		//buf.WriteString(fmt.Sprintf(" OFFSET %d", m.Offset))
	}
	return buf.String()
}
func (m *SqlSelect) FingerPrintID() int64 {
	h := fnv.New64()
	h.Write([]byte(m.FingerPrint(rune('?'))))
	return int64(h.Sum64())
}

func (m *SqlSelect) Projection(p *Projection) *Projection {
	if p != nil {
		m.proj = p
	}
	if m.proj != nil {
		return m.proj
	}
	// p := NewProjection()
	// for i, col := range m.Columns {
	// 	//p.AddColumnShort(name, vt)
	// }
	return nil
}

// Finalize this Query plan by preparing sub-sources
//  ie we need to rewrite some things into sub-statements
//  - we need to share the join expression across sources
func (m *SqlSelect) Finalize() error {
	if len(m.From) == 0 {
		return nil
	}

	// TODO:   This is invalid, as you can have more than one join on a table
	exprs := make(map[string]Node)

	cols := m.UnAliasedColumns()

	for _, from := range m.From {
		from.Finalize()
		from.cols = cols
		//left, right, ok := from.LeftRight()
		if from.JoinExpr != nil {
			left, right := from.findFromAliases()
			//u.Debugf("from1:%v  from2:%v   joinexpr:  %v", left, right, from.JoinExpr.String())
			exprs[left] = from.JoinExpr
			exprs[right] = from.JoinExpr
		}
		//u.Debugf("from.Alias:%v from.Name:%v  from:%#v", from.Alias, from.Name, from)
		//exprs[strings.ToLower(from.Alias)] = from.JoinExpr
	}
	// for name, expr := range exprs {
	// 	u.Debugf("EXPR:   name: %v  expr:%v", name, expr.String())
	// }
	for _, from := range m.From {
		if from.JoinExpr == nil {
			//u.Debugf("from join nil?%v  %v", from.JoinExpr == nil, from)
			if expr, ok := exprs[from.alias]; ok {
				//u.Warnf("NICE found: %#v", expr)
				from.JoinExpr = expr
			}
		}
	}

	return nil
}

func (m *SqlSelect) UnAliasedColumns() map[string]*Column {
	cols := make(map[string]*Column)
	//u.Infof("doing ALIAS: %v", len(m.Columns))
	for _, col := range m.Columns {
		_, right, _ := col.LeftRight()
		//u.Debugf("aliasing: l:%v r:%v ok?%v  %q", left, right, ok, col.String())
		cols[right] = col
	}
	return cols
}

func (m *SqlSelect) AddColumn(colArg Column) error {
	col := &colArg
	//curCol := m.ColumnsAsMap[col.As]
	// if curCol != nil {
	// 	// We have duplicate column names? is this an error?
	// }
	col.Index = len(m.Columns)
	m.Columns = append(m.Columns, col)

	if col.As == "" {
		u.Errorf("no as on col, is required?  %#s", col)
	}
	//m.ColumnsAsMap[col.As] = col
	//u.Infof("added col: %p %#v", col, col)
	return nil
}

// Is this a select count(*) FROM ...   query?
func (m *SqlSelect) CountStar() bool {
	if len(m.Columns) != 1 {
		return false
	}
	col := m.Columns[0]
	if col.Expr == nil {
		return false
	}
	if f, ok := col.Expr.(*FuncNode); ok {
		if strings.ToLower(f.Name) != "count" {
			return false
		}
		if len(f.Args) == 1 && f.Args[0].String() == "*" {
			return true
		}
	}
	return false
}

// Is this a internal variable query?
//     @@max_packet_size   ??
func (m *SqlSelect) SysVariable() string {

	if len(m.Columns) != 1 {
		return ""
	}
	col := m.Columns[0]
	if col.Expr == nil {
		return ""
	}
	switch n := col.Expr.(type) {
	case *IdentityNode:
		if strings.HasPrefix(n.Text, "@@") {
			return n.Text
		}
		if len(m.From) == 0 {
			// SELECT current_user
			return n.Text
		}
	case *FuncNode:
		if len(m.From) == 0 {
			// SELECT current_user()
			return n.String()
		}
	}
	return ""
}

func (m *SqlSource) Accept(visitor SubVisitor) (interface{}, error) { return visitor.VisitSubselect(m) }
func (m *SqlSource) Keyword() lex.TokenType                         { return m.Op }
func (m *SqlSource) Check() error                                   { return nil }
func (m *SqlSource) Type() reflect.Value                            { return nilRv }
func (m *SqlSource) NodeType() NodeType                             { return SqlSourceNodeType }
func (m *SqlSource) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}
func (m *SqlSource) writeBuf(buf *bytes.Buffer) {

	if int(m.Op) == 0 && int(m.LeftOrRight) == 0 && int(m.JoinType) == 0 {
		if m.Alias != "" {
			buf.WriteString(fmt.Sprintf("%s AS %v", m.Name, m.Alias))
			return
		}
		buf.WriteString(m.Name)
		return
	}
	//u.Warnf("op:%d leftright:%d jointype:%d", m.Op, m.LeftRight, m.JoinType)
	//u.Warnf("op:%s leftright:%s jointype:%s", m.Op, m.LeftRight, m.JoinType)
	//u.Infof("%#v", m)
	//   Jointype                Op
	//  INNER JOIN orders AS o 	ON
	if int(m.JoinType) != 0 {
		buf.WriteString(strings.ToTitle(m.JoinType.String()))
		buf.WriteByte(' ')
	}
	buf.WriteString("JOIN ")

	if m.Alias != "" {
		buf.WriteString(fmt.Sprintf("%s AS %v", m.Name, m.Alias))
	} else {
		buf.WriteString(m.Name)
	}
	buf.WriteByte(' ')
	buf.WriteString(strings.ToTitle(m.Op.String()))

	//u.Warnf("JoinExpr? %#v", m.JoinExpr)
	if m.JoinExpr != nil {
		buf.WriteByte(' ')
		buf.WriteString(m.JoinExpr.String())
	}
}
func (m *SqlSource) FingerPrint(r rune) string {

	if int(m.Op) == 0 && int(m.LeftOrRight) == 0 && int(m.JoinType) == 0 {
		if m.Alias != "" {
			return fmt.Sprintf("%s AS %v", m.Name, m.Alias)
		}
		return m.Name
	}
	buf := bytes.Buffer{}
	//u.Warnf("op:%d leftright:%d jointype:%d", m.Op, m.LeftRight, m.JoinType)
	//u.Warnf("op:%s leftright:%s jointype:%s", m.Op, m.LeftRight, m.JoinType)
	//u.Infof("%#v", m)
	//   Jointype                Op
	//  INNER JOIN orders AS o 	ON
	if int(m.JoinType) != 0 {
		buf.WriteString(strings.ToTitle(m.JoinType.String()))
		buf.WriteByte(' ')
	}
	buf.WriteString("JOIN ")

	if m.Alias != "" {
		buf.WriteString(fmt.Sprintf("%s AS %v", m.Name, m.Alias))
	} else {
		buf.WriteString(m.Name)
	}
	buf.WriteByte(' ')
	buf.WriteString(strings.ToTitle(m.Op.String()))

	//u.Warnf("JoinExpr? %#v", m.JoinExpr)
	if m.JoinExpr != nil {
		buf.WriteByte(' ')
		buf.WriteString(m.JoinExpr.FingerPrint(r))
		//buf.WriteByte(' ')
	}
	//u.Warnf("source? %#v", m.Source)
	// if m.Source != nil {
	// 	buf.WriteString(m.Source.String())
	// }
	return buf.String()
}

// Rewrite this Source to act as a stand-alone query to backend
//  @parentStmt = the parent statement that this a partial source to
func (m *SqlSource) Rewrite(parentStmt *SqlSelect) *SqlSelect {
	// Rewrite this SqlSource for the given parent, ie
	//   1)  find the column names we need to project, including those used in join/where
	//   2)  rewrite the where for this partial query
	//   3)  any columns in join expression that are not equal between
	//          sides should be aliased towards the left-hand join portion
	//   4)  if we need different sort for our join algo?

	if parentStmt.Star {
		m.Star = true
	} else {
		m.Columns = make(Columns, 0)
		for idx, col := range parentStmt.Columns {
			left, _, ok := col.LeftRight()
			//u.Infof("col: P:%p ok?%v %#v", col, ok, col)
			if !ok {
				// Was not left/right qualified, so use as is?  or is this an error?
				u.Warnf("unknown col alias?: %#v", col)
				newCol := col.Copy()
				newCol.ParentIndex = idx
				newCol.Index = len(m.Columns)
				m.Columns = append(m.Columns, newCol)

			} else if ok && left == m.Alias {
				//u.Debugf("CopyRewrite: %v  P:%p %#v", m.Alias, col, col)
				newCol := col.CopyRewrite(m.Alias)
				// Now Rewrite the Join Expression
				n := rewriteNode(m, col.Expr)
				if n != nil {
					newCol.Expr = n
				}
				newCol.ParentIndex = idx
				newCol.Index = len(m.Columns)
				m.Columns = append(m.Columns, newCol)
				//u.Debugf("appending rewritten col to subquery: %#v", newCol)

			} else {
				// not used in this source
				//u.Debugf("sub-query does not use this parent col?  FROM %v  col:%v", m.Name, col.As)
			}
		}
	}
	// TODO:
	//  - rewrite the Where clause
	//  - rewrite the Sort
	sql2 := &SqlSelect{Columns: m.Columns, Star: m.Star}
	sql2.From = append(sql2.From, &SqlSource{Name: m.Name})
	//u.Debugf("colsFromNode? joinExpr:%#v  %#v", m.JoinExpr, sql2.Columns)
	sql2.Columns = columnsFromNode(m, m.JoinExpr, sql2.Columns)
	//u.Debugf("cols len: %v", len(sql2.Columns))
	if parentStmt.Where != nil {
		node := rewriteWhere(parentStmt, m, parentStmt.Where.Expr)
		if node != nil {
			//u.Warnf("node string():  %v", node.String())
			sql2.Where = &SqlWhere{Expr: node}
		}
		//u.Warnf("new where node:   %#v", node)
	}
	m.Source = sql2
	//u.Infof("going to unaliase: #cols=%v %#v", len(sql2.Columns), sql2.Columns)
	m.cols = sql2.UnAliasedColumns()
	//u.Infof("after aliasing: %#v", m.cols)
	return sql2
}

func (m *SqlSource) findFromAliases() (string, string) {
	from1, from2 := m.alias, ""
	if m.JoinExpr != nil {
		switch nt := m.JoinExpr.(type) {
		case *BinaryNode:
			if in, ok := nt.Args[0].(*IdentityNode); ok {
				if left, _, ok := in.LeftRight(); ok {
					from1 = left
				}
			}
			if in, ok := nt.Args[1].(*IdentityNode); ok {
				if left, _, ok := in.LeftRight(); ok {
					from2 = left
				}
			}
		default:
			u.Warnf("%T node types are not suppored yet for join rewrite", m.JoinExpr)
		}
	}
	return from1, from2
}

func rewriteWhere(stmt *SqlSelect, from *SqlSource, node Node) Node {
	switch nt := node.(type) {
	case *IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			//u.Debugf("rewriteWhere  from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := IdentityNode{Text: right}
				//u.Warnf("nice, found it! in = %v", in)
				return &in
			} else {
				//u.Warnf("what to do? source:%v    %v", from.alias, nt.String())
			}
		} else {
			//u.Warnf("dropping where: %#v", nt)
		}
	case *NumberNode, *NullNode, *StringNode:
		return nt
	case *BinaryNode:
		//u.Infof("binaryNode  T:%v", nt.Operator.T.String())
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			n1 := rewriteWhere(stmt, from, nt.Args[0])
			n2 := rewriteWhere(stmt, from, nt.Args[1])

			if n1 != nil && n2 != nil {
				return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
			} else if n1 != nil {
				return n1
			} else if n2 != nil {
				return n2
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenGT, lex.TokenGE, lex.TokenLE, lex.TokenNE:
			n1 := rewriteWhere(stmt, from, nt.Args[0])
			n2 := rewriteWhere(stmt, from, nt.Args[1])
			//u.Debugf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			if n1 != nil && n2 != nil {
				return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
				// } else if n1 != nil {
				// 	return n1
				// } else if n2 != nil {
				// 	return n2
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		default:
			u.Warnf("un-implemented op: %#v", nt)
		}
	default:
		u.Warnf("%T node types are not suppored yet for where rewrite", node)
	}
	return nil
}

// We need to find all columns used in the given Node (where/join expression)
//  to ensure we have those columns in projection for sub-queries
func columnsFromNode(from *SqlSource, node Node, cols Columns) Columns {
	switch nt := node.(type) {
	case *IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			//u.Debugf("from.Name:%v AS %v   Joinnode l:%v  r:%v    %#v", from.Name, from.alias, left, right, nt)
			//u.Warnf("check cols against join expr arg: %#v", nt)
			if left == from.alias {
				found := false
				for _, col := range cols {
					colLeft, colRight, _ := col.LeftRight()
					//u.Debugf("left='%s'  colLeft='%s' right='%s'  %#v", left, colLeft, colRight,  col)
					//u.Debugf("col:  From %s AS '%s'   '%s'.'%s'  JoinExpr: '%v'.'%v' col:%#v", from.Name, from.alias, colLeft, colRight, left, right, col)
					if left == colLeft || colRight == right {
						found = true
						//u.Infof("columnsFromNode from.Name:%v l:%v  r:%v", from.alias, left, right)
					} else {
						//u.Warnf("not? from.Name:%v l:%v  r:%v   col: P:%p %#v", from.alias, left, right, col, col)
					}
				}
				if !found {
					//u.Debugf("columnsFromNode from.Name:%v l:%v  r:%v", from.alias, left, right)
					newCol := &Column{As: right, SourceField: right, Expr: &IdentityNode{Text: right}}
					newCol.Index = len(cols)
					cols = append(cols, newCol)
					//u.Warnf("sure we want to add?, found it! %s len(cols) = %v", right, len(cols))
				}
			}
		}
	case *BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			cols = columnsFromNode(from, nt.Args[0], cols)
			cols = columnsFromNode(from, nt.Args[1], cols)
		case lex.TokenEqual, lex.TokenEqualEqual:
			cols = columnsFromNode(from, nt.Args[0], cols)
			cols = columnsFromNode(from, nt.Args[1], cols)
		default:
			u.Warnf("un-implemented op: %v", nt.Operator)
		}
	default:
		u.LogTracef(u.INFO, "whoops")
		u.Warnf("%T node types are not suppored yet for join rewrite %s", node, from.String())
	}
	return cols
}

func rewriteNode(from *SqlSource, node Node) Node {
	switch nt := node.(type) {
	case *IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			//u.Debugf("rewriteNode from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := IdentityNode{Text: right}
				//u.Warnf("nice, found it! in = %v", in)
				return &in
			}
		}
	case *BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			n1 := rewriteNode(from, nt.Args[0])
			n2 := rewriteNode(from, nt.Args[1])
			return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
		case lex.TokenEqual, lex.TokenEqualEqual:
			n := rewriteNode(from, nt.Args[0])
			if n != nil {
				return n
			}
			n = rewriteNode(from, nt.Args[1])
			if n != nil {
				return n
			}
			u.Warnf("Could not find node: %#v", node)
		default:
			u.Warnf("un-implemented op: %v", nt.Operator)
		}
	default:
		u.Warnf("%T node types are not suppored yet for join rewrite", node)
	}
	return nil
}

// Get a list of Un-Aliased Columns, ie columns with column
//  names that have NOT yet been aliased
func (m *SqlSource) UnAliasedColumns() map[string]*Column {
	return m.cols
	// ?????
	cols := make(map[string]*Column)
	//u.Infof("doing ALIAS: %v", len(m.Columns))
	for _, col := range m.Columns {
		left, right, ok := col.LeftRight()
		//u.Debugf("aliasing: l:%v r:%v ok?%v", left, right, ok)
		if ok {
			cols[right] = col
		} else {
			cols[left] = col
		}
	}
	return cols
}

// Get a list of Column names to position
func (m *SqlSource) ColumnPositions() map[string]int {
	if len(m.colIndex) > 0 {
		return m.colIndex
	}
	cols := make(map[string]int)
	for idx, col := range m.Columns {
		left, right, ok := col.LeftRight()
		//u.Debugf("aliasing: l:%v r:%v ok?%v", left, right, ok)
		if ok {
			cols[right] = idx
		} else {
			cols[left] = idx
		}
	}
	m.colIndex = cols
	return m.colIndex
}

// We need to be able to rewrite statements to convert a stmt such as:
//
//		FROM users AS u
//			INNER JOIN orders AS o
//			ON u.user_id = o.user_id
//
//  So that we can evaluate the Join Key on left/right
//     in this case, it is simple, just
//
//    =>   user_id
//
//  or this one:
//
//		FROM users AS u
//			INNER JOIN orders AS o
//			ON LOWER(u.email) = LOWER(o.email)
//
//    =>  LOWER(user_id)
//
func (m *SqlSource) JoinValueExpr() (Node, error) {

	//u.Debugf("alias:%v get JoinExpr: T:%T v:%#v", m.alias, m.JoinExpr, m.JoinExpr)
	//u.Debugf("source: T:%T  v:%#v", m, m)
	bn, ok := m.JoinExpr.(*BinaryNode)
	if !ok {
		return nil, fmt.Errorf("Could not evaluate node %v", m.JoinExpr.String())
	}
	if bn.IsSimple() {
		//u.Debugf("is simple binary node: %v", bn.Operator.T.String())
		for _, arg := range bn.Args {
			switch n := arg.(type) {
			case *IdentityNode:
				left, right, ok := n.LeftRight()
				if ok {
					if left == m.alias && right != "" {
						// this is correct node
						//u.Warnf("NICE, found: %v     right=%v", n.String(), right)
						return &IdentityNode{Text: right}, nil
					} else if left == m.alias && right == "" {
						//u.Warnf("NICE2, found: %v     right=%v", n.String(), right)
					}
				}
			}
		}
	}

	return m.JoinExpr, nil
	return nil, fmt.Errorf("Whoops:  %v", m.JoinExpr.String())
}
func (m *SqlSource) Finalize() error {
	if m.final {
		return nil
	}
	m.alias = strings.ToLower(m.Alias)
	if m.alias == "" {
		m.alias = strings.ToLower(m.Name)
	}
	//u.Warnf("finalize sqlsource: %v", len(m.Columns))
	m.final = true
	return nil
}

func (m *SqlWhere) Keyword() lex.TokenType { return m.Op }
func (m *SqlWhere) Check() error           { return nil }
func (m *SqlWhere) Type() reflect.Value    { return nilRv }
func (m *SqlWhere) NodeType() NodeType     { return SqlWhereNodeType }
func (m *SqlWhere) writeBuf(buf *bytes.Buffer) {
	if int(m.Op) == 0 && m.Source == nil && m.Expr != nil {
		buf.WriteString(m.Expr.String())
		return
	}
	// Op = subselect or in etc
	if int(m.Op) != 0 && m.Source != nil {
		buf.WriteString(fmt.Sprintf("%s (%s)", m.Op.String(), m.Source.String()))
		return
	}
	u.Warnf("unexpected SqlWhere string? is this? %#v", m)
}
func (m *SqlWhere) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(&buf)
	return buf.String()
}
func (m *SqlWhere) FingerPrint(r rune) string {
	if int(m.Op) == 0 && m.Source == nil && m.Expr != nil {
		return m.Expr.FingerPrint(r)
	}
	// Op = subselect or in etc
	if int(m.Op) != 0 && m.Source != nil {
		return fmt.Sprintf("%s (%s)", m.Op.String(), m.Source.FingerPrint(r))
	}
	u.Warnf("what is this? %#v", m)
	return ""
}

func (m *SqlInto) Keyword() lex.TokenType    { return lex.TokenInto }
func (m *SqlInto) Check() error              { return nil }
func (m *SqlInto) Type() reflect.Value       { return nilRv }
func (m *SqlInto) NodeType() NodeType        { return SqlIntoNodeType }
func (m *SqlInto) String() string            { return fmt.Sprintf("%s", m.Table) }
func (m *SqlInto) FingerPrint(r rune) string { return m.String() }

/*
func (m *Join) Accept(visitor SubVisitor) (interface{}, error) { return visitor.VisitSubselect(m) }
func (m *Join) Keyword() lex.TokenType                         { return lex.TokenJoin }
func (m *Join) Check() error                                   { return nil }
func (m *Join) Type() reflect.Value                            { return nilRv }
func (m *Join) NodeType() NodeType                             { return SqlJoinNodeType }
func (m *Join) StringAST() string                              { return m.String() }
func (m *Join) String() string                                 { return fmt.Sprintf("%s", m.Table) }
*/
func (m *SqlInsert) Keyword() lex.TokenType                      { return m.kw }
func (m *SqlInsert) Check() error                                { return nil }
func (m *SqlInsert) Type() reflect.Value                         { return nilRv }
func (m *SqlInsert) NodeType() NodeType                          { return SqlInsertNodeType }
func (m *SqlInsert) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitInsert(m) }
func (m *SqlInsert) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("INSERT INTO %s (", m.Table))

	for i, col := range m.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
		//u.Infof("write:  %q   %#v", col.String(), col)
	}
	buf.WriteString(") VALUES")
	for i, row := range m.Rows {
		if i > 0 {
			buf.WriteString("\n\t,")
		}
		buf.WriteString(" (")
		for vi, val := range row {
			if vi > 0 {
				buf.WriteString(" ,")
			}
			if val.Expr != nil {
				buf.WriteString(val.Expr.String())
			} else { // Value is not nil
				switch vt := val.Value.(type) {
				case value.StringValue:
					buf.WriteString(fmt.Sprintf("%q", vt.Val()))
				case value.SliceValue:
					by, err := vt.MarshalJSON()
					if err == nil {
						buf.Write(by)
					} else {
						buf.Write([]byte("null"))
					}
				case nil:
					// ?? what to do?
					u.Warnf("what is going on in nil val? %#v", val)
				default:
					buf.WriteString(vt.ToString())
				}
			}
		}
		buf.WriteByte(')')
	}
	return buf.String()
}
func (m *SqlInsert) FingerPrint(r rune) string { return m.String() }
func (m *SqlInsert) ColumnNames() []string {
	cols := make([]string, 0)
	for _, col := range m.Columns {
		cols = append(cols, col.Key())
	}
	return cols
}

func (m *SqlUpsert) Keyword() lex.TokenType                      { return lex.TokenUpsert }
func (m *SqlUpsert) Check() error                                { return nil }
func (m *SqlUpsert) Type() reflect.Value                         { return nilRv }
func (m *SqlUpsert) NodeType() NodeType                          { return SqlUpsertNodeType }
func (m *SqlUpsert) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpsert) FingerPrint(r rune) string                   { return m.String() }
func (m *SqlUpsert) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitUpsert(m) }
func (m *SqlUpsert) SqlSelect() *SqlSelect                       { return sqlSelectFromWhere(m.Table, m.Where) }

func (m *SqlUpdate) Keyword() lex.TokenType                      { return lex.TokenUpdate }
func (m *SqlUpdate) Check() error                                { return nil }
func (m *SqlUpdate) Type() reflect.Value                         { return nilRv }
func (m *SqlUpdate) NodeType() NodeType                          { return SqlUpdateNodeType }
func (m *SqlUpdate) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitUpdate(m) }
func (m *SqlUpdate) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", m.Table))
	firstCol := true
	for key, val := range m.Values {
		if !firstCol {
			buf.WriteByte(',')
		}
		firstCol = false
		buf.WriteByte(' ')
		switch vt := val.Value.(type) {
		case value.StringValue:
			buf.WriteString(fmt.Sprintf("%s = %q", key, vt.ToString()))
		default:
			buf.WriteString(fmt.Sprintf("%s = %v", key, vt.Value()))
		}
	}
	if m.Where != nil {
		buf.WriteString(fmt.Sprintf(" WHERE %s", m.Where.String()))
	}
	return buf.String()
}
func (m *SqlUpdate) FingerPrint(r rune) string { return m.String() }
func (m *SqlUpdate) SqlSelect() *SqlSelect     { return sqlSelectFromWhere(m.Table, m.Where) }

func sqlSelectFromWhere(from string, where Node) *SqlSelect {
	req := NewSqlSelect()
	req.From = []*SqlSource{NewSqlSource(from)}
	switch wt := where.(type) {
	case *SqlWhere:
		req.Where = NewSqlWhere(wt.Expr)
	default:
		req.Where = NewSqlWhere(where)
	}

	req.Star = true
	req.Columns = starCols
	return req
}

func (m *SqlDelete) Keyword() lex.TokenType                      { return lex.TokenDelete }
func (m *SqlDelete) Check() error                                { return nil }
func (m *SqlDelete) Type() reflect.Value                         { return nilRv }
func (m *SqlDelete) NodeType() NodeType                          { return SqlDeleteNodeType }
func (m *SqlDelete) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDelete) FingerPrint(r rune) string                   { return m.String() }
func (m *SqlDelete) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitDelete(m) }
func (m *SqlDelete) SqlSelect() *SqlSelect                       { return sqlSelectFromWhere(m.Table, m.Where) }

func (m *SqlDescribe) Keyword() lex.TokenType                      { return lex.TokenDescribe }
func (m *SqlDescribe) Check() error                                { return nil }
func (m *SqlDescribe) Type() reflect.Value                         { return nilRv }
func (m *SqlDescribe) NodeType() NodeType                          { return SqlDescribeNodeType }
func (m *SqlDescribe) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDescribe) FingerPrint(r rune) string                   { return m.String() }
func (m *SqlDescribe) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitDescribe(m) }

func (m *SqlShow) Keyword() lex.TokenType                      { return lex.TokenShow }
func (m *SqlShow) Check() error                                { return nil }
func (m *SqlShow) Type() reflect.Value                         { return nilRv }
func (m *SqlShow) NodeType() NodeType                          { return SqlShowNodeType }
func (m *SqlShow) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlShow) FingerPrint(r rune) string                   { return m.String() }
func (m *SqlShow) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitShow(m) }

func (m *CommandColumn) FingerPrint(r rune) string { return m.String() }
func (m *CommandColumn) String() string {
	if len(m.Name) > 0 {
		return m.Name
	}
	if m.Expr != nil {
		return m.Expr.String()
	}
	return ""
}

func (m *CommandColumns) FingerPrint(r rune) string { return m.String() }
func (m *CommandColumns) String() string {
	colCt := len(*m)
	if colCt == 1 {
		return (*m)[0].String()
	} else if colCt == 0 {
		return ""
	}
	s := make([]string, len(*m))
	for i, col := range *m {
		s[i] = col.String()
	}
	return strings.Join(s, ", ")
}

func (m *SqlCommand) Keyword() lex.TokenType                      { return m.kw }
func (m *SqlCommand) Check() error                                { return nil }
func (m *SqlCommand) Type() reflect.Value                         { return nilRv }
func (m *SqlCommand) NodeType() NodeType                          { return SqlCommandNodeType }
func (m *SqlCommand) FingerPrint(r rune) string                   { return m.String() }
func (m *SqlCommand) String() string                              { return fmt.Sprintf("%s %s", m.Keyword(), m.Columns.String()) }
func (m *SqlCommand) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitCommand(m) }
