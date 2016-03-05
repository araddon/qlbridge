package rel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	u "github.com/araddon/gou"
	"github.com/gogo/protobuf/proto"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// Ensure SqlSelect and cousins etc are SqlStatements
	_ SqlStatement = (*SqlSelect)(nil)
	_ SqlStatement = (*SqlInsert)(nil)
	_ SqlStatement = (*SqlUpsert)(nil)
	_ SqlStatement = (*SqlUpdate)(nil)
	_ SqlStatement = (*SqlDelete)(nil)
	_ SqlStatement = (*SqlShow)(nil)
	_ SqlStatement = (*SqlDescribe)(nil)
	_ SqlStatement = (*SqlCommand)(nil)
	_ SqlStatement = (*SqlInto)(nil)

	// sub-query statements
	_ SqlSourceStatement = (*SqlSource)(nil)

	// A select * columns
	starCols Columns
)

func init() {
	starCols = make(Columns, 1)
	starCols[0] = NewColumnFromToken(lex.Token{T: lex.TokenStar, V: "*"})
}

type (
	// The sqlStatement interface, to define the sql statement
	//  Select, Insert, Update, Delete, Command, Show, Describe etc
	SqlStatement interface {
		// string representation of Node, AST parseable back to itself
		String() string

		// string representation of Node, AST but with values replaced by @rune (? generally)
		//  used to allow statements to be deterministically cached/prepared even without
		//  usage of keyword prepared
		FingerPrint(r rune) string

		// SQL keyword (select, insert, etc)
		Keyword() lex.TokenType
	}

	// The sqlStatement interface, to define the subselect/join-types
	//   Join, SubSelect, From
	SqlSourceStatement interface {
		// string representation of Node, AST parseable back to itself
		String() string

		// string representation of Node, AST but with values replaced by @rune (? generally)
		//  used to allow statements to be deterministically cached/prepared even without
		//  usage of keyword prepared
		FingerPrint(r rune) string

		Keyword() lex.TokenType
	}
)

type (
	// Prepared/Aliased SQL statement
	PreparedStatement struct {
		Alias     string
		Statement SqlStatement
	}
	// SQL Select statement
	SqlSelect struct {
		Db        string       // If provided a use "dbname"
		Raw       string       // full original raw statement
		Star      bool         // for select * from ...
		Distinct  bool         // Distinct flag?
		Columns   Columns      // An array (ordered) list of columns
		From      []*SqlSource // From, Join
		Into      *SqlInto     // Into "table"
		Where     *SqlWhere    // Expr Node, or *SqlSelect
		Having    expr.Node    // Filter results
		GroupBy   Columns
		OrderBy   Columns
		Limit     int
		Offset    int
		Alias     string       // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With      u.JsonHelper // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
		proj      *Projection  // Projected fields
		isAgg     bool         // is this an aggregate query?  has group-by, or aggregate selector expressions (count, cardinality etc)
		finalized bool         // have we already finalized, ie formalized left/right aliases
		schemaqry bool         // is this a schema qry?  ie select @@max_packet etc

		// Memoized sql, we assume this is an immuteable struct so if this is populated use it
		pb            *SqlStatementPb
		fingerprintid int64
	}
	// Source is a table name, sub-query, or join as used in
	// SELECT <columns> FROM <SQLSOURCE>
	//  - SELECT .. FROM table_name
	//  - SELECT .. from (select a,b,c from tableb)
	//  - SELECT .. FROM tablex INNER JOIN ...
	SqlSource struct {
		final       bool               // has this been finalized?
		alias       string             // either the short table name or full
		cols        map[string]*Column // Un-aliased columns, ie "x.y" -> "y"
		colIndex    map[string]int     // Key(alias) to index in []driver.Value positions
		joinNodes   []expr.Node        // x.y = q.y AND x.z = q.z  --- []Node{Identity{x},Identity{z}}
		Source      *SqlSelect         // Sql Select Source query, written by Rewrite
		Raw         string             // Raw Partial Query
		Name        string             // From Name (optional, empty if join, subselect)
		Alias       string             // From name aliased
		Schema      string             //  FROM `schema`.`table`
		Op          lex.TokenType      // In, =, ON
		LeftOrRight lex.TokenType      // Left, Right
		JoinType    lex.TokenType      // INNER, OUTER
		JoinExpr    expr.Node          // Join expression       x.y = q.y
		SubQuery    *SqlSelect         // optional, Join/SubSelect statement

		// Plan Hints, move to a dedicated planner
		Seekable bool
		// Memoized sql, we assume this is an immuteable struct so if this is populated use it
		pb *SqlSourcePb
	}
	// WHERE is select stmt, or set of expressions
	// - WHERE x in (select name from q)
	// - WHERE x = y
	// - WHERE x = y AND z = q
	// - WHERE tolower(x) IN (select name from q)
	SqlWhere struct {
		// Either Op + Source exists
		Op     lex.TokenType // (In|=|ON)  for Select Clauses operators
		Source *SqlSelect    // IN (SELECT a,b,c from z)

		// OR expr but not both
		Expr expr.Node // x = y AND q > 5
	}
	// SQL Insert Statement
	SqlInsert struct {
		kw      lex.TokenType    // Insert, Replace
		Table   string           // table name
		Columns Columns          // Column Names
		Rows    [][]*ValueColumn // Values to insert
		Select  *SqlSelect       //
	}
	// SQL Upsert Statement
	SqlUpsert struct {
		Columns Columns
		Rows    [][]*ValueColumn
		Values  map[string]*ValueColumn
		Where   *SqlWhere
		Table   string
	}
	// SQL Update Statement
	SqlUpdate struct {
		Values map[string]*ValueColumn
		Where  *SqlWhere
		Table  string
	}
	// SQL Delete Statement
	SqlDelete struct {
		Table string
		Where *SqlWhere
		Limit int
	}
	// SQL SHOW Statement
	SqlShow struct {
		Raw        string // full raw statement
		Db         string // Database/Schema name
		Full       bool   // SHOW FULL TABLE FROM
		Scope      string // {FULL, GLOBAL, SESSION}
		ShowType   string // object type, [tables, columns, etc]
		From       string // `table`   or `schema`.`table`
		Identity   string // `table`   or `schema`.`table`
		Create     bool
		CreateWhat string
		Where      expr.Node
		Like       expr.Node
	}
	// SQL Describe statement
	SqlDescribe struct {
		Raw      string    // full original raw statement
		Identity string    // Describe
		Tok      lex.Token // Explain, Describe, Desc
		Stmt     SqlStatement
	}
	// SQL INTO statement   (select a,b,c from y INTO z)
	SqlInto struct {
		Table string
	}
	// Sql Command is admin command such as "SET"
	SqlCommand struct {
		kw       lex.TokenType  // SET
		Columns  CommandColumns // can have multiple columns in command
		Identity string         //
		Value    expr.Node      //
	}
	// List of Columns in SELECT [columns]
	Columns []*Column
	// Column represents the Column as expressed in a [SELECT]
	// expression
	Column struct {
		sourceQuoteByte byte      // quote mark?   [ or ` etc
		asQuoteByte     byte      // quote mark   [ or `
		originalAs      string    // original as string
		left            string    // users.col_name   = "users"
		right           string    // users.first_name = "first_name"
		ParentIndex     int       // slice idx position in parent query cols
		Index           int       // slice idx position in original query cols
		SourceIndex     int       // slice idx position in source []driver.Value
		SourceField     string    // field name of underlying field
		As              string    // As field, auto-populate the Field Name if exists
		Comment         string    // optional in-line comments
		Order           string    // (ASC | DESC)
		Star            bool      // *
		Agg             bool      // aggregate function column?   count(*), avg(x) etc
		Expr            expr.Node // Expression, optional, often Identity.Node
		Guard           expr.Node // column If guard, non-standard sql column guard
	}
	// List of Value columns in INSERT into TABLE (colnames) VALUES (valuecolumns)
	ValueColumn struct {
		Value value.Value
		Expr  expr.Node
	}
	// List of ResultColumns used to describe projection response columns
	ResultColumns []*ResultColumn
	// Result Column used in projection
	ResultColumn struct {
		Final  bool            // Is this part of final projection (ie, response)
		Name   string          // Original path/name for query field
		ColPos int             // Ordinal position in sql (or partial sql) statement
		Col    *Column         // the original sql column
		Star   bool            // Was this a select * ??
		As     string          // aliased
		Type   value.ValueType // Data Type
	}
	// Projection describes the results to expect from sql statement
	// ie the ResultColumns for a result-set
	Projection struct {
		Distinct bool
		Final    bool // Is this a Final Projection? or intermiediate?
		colNames map[string]struct{}
		Columns  ResultColumns
		// Memoized pb, we assume this is an immuteable struct so if this is populated use it
		pb *ProjectionPb
	}
	// SQL commands such as:
	//     set autocommit
	//     SET @@local.sort_buffer_size=10000;
	//     USE myschema;
	CommandColumns []*CommandColumn
	// Command column is single column such as "autocommit"
	CommandColumn struct {
		Expr expr.Node // column expression
		Name string    // Original path/name for command field
	}
)

func NewProjection() *Projection {
	return &Projection{Columns: make(ResultColumns, 0), colNames: make(map[string]struct{})}
}
func NewResultColumn(as string, ordinal int, col *Column, valtype value.ValueType) *ResultColumn {
	rc := ResultColumn{Name: as, As: as, ColPos: ordinal, Col: col, Type: valtype}
	if col != nil {
		rc.Name = col.SourceField
	}
	return &rc
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
func NewSqlWhere(where expr.Node) *SqlWhere {
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
func NewColumnValue(tok lex.Token) *Column {
	return &Column{
		sourceQuoteByte: tok.Quote,
		asQuoteByte:     tok.Quote,
	}
}
func NewColumn(col string) *Column {
	return &Column{
		As:          col,
		SourceField: col,
		Expr:        &expr.IdentityNode{Text: col},
	}
}

func (m *ResultColumn) Equal(s *ResultColumn) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Final != s.Final {
		return false
	}
	if m.Name != s.Name {
		return false
	}
	if m.ColPos != s.ColPos {
		return false
	}
	if m.Star != s.Star {
		return false
	}
	if m.As != s.As {
		return false
	}
	if m.Type != s.Type {
		return false
	}
	if m.Col != nil && !m.Col.Equal(m.Col) {
		return false
	}
	return true
}
func resultColumnFromPb(pb *ResultColumnPb) *ResultColumn {
	s := ResultColumn{}
	s.Final = pb.GetFinal()
	s.Name = pb.GetName()
	s.ColPos = int(pb.GetColPos())
	s.Col = columnFromPb(pb.Column)
	s.Star = pb.GetStar()
	s.As = pb.GetAs()
	s.Type = value.ValueType(pb.GetValueType())
	return &s
}
func resultColumnToPb(m *ResultColumn) *ResultColumnPb {
	s := ResultColumnPb{}
	if m.Col != nil {
		s.Column = m.Col.ToPB()
	}
	if m.Final {
		s.Final = &m.Final
	}
	if m.Star {
		s.Star = &m.Star
	}
	s.Name = m.Name
	s.ColPos = int32(m.ColPos)
	s.As = m.As
	s.ValueType = int32(m.Type)
	return &s
}

func (m *Projection) AddColumnShort(colName string, vt value.ValueType) {
	//colName = strings.ToLower(colName)
	// if _, exists := m.colNames[colName]; exists {
	// 	return
	// }
	//u.Infof("adding column %s to %v", colName, m.colNames)
	//m.colNames[colName] = struct{}{}
	m.Columns = append(m.Columns, NewResultColumn(colName, len(m.Columns), nil, vt))
}
func (m *Projection) AddColumn(col *Column, vt value.ValueType) {
	//colName := strings.ToLower(col.As)
	// if _, exists := m.colNames[colName]; exists {
	// 	return
	// }
	//m.colNames[colName] = struct{}{}
	m.Columns = append(m.Columns, NewResultColumn(col.As, len(m.Columns), col, vt))
}
func (m *Projection) Equal(s *Projection) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Distinct != s.Distinct {
		return false
	}
	if len(m.colNames) != len(s.colNames) {
		return false
	}
	for name, _ := range m.colNames {
		_, hasSameName := s.colNames[name]
		if !hasSameName {
			return false
		}
	}
	if len(m.Columns) != len(s.Columns) {
		return false
	}
	for i, c := range m.Columns {
		if !c.Equal(s.Columns[i]) {
			return false
		}
	}
	return true
}
func (m *Projection) FromPB(pb *ProjectionPb) *Projection {
	return ProjectionFromPb(pb)
}
func (m *Projection) ToPB() *ProjectionPb {
	if m.pb == nil {
		m.pb = projectionToPb(m)
	}
	return m.pb
}
func ProjectionFromPb(pb *ProjectionPb) *Projection {
	s := Projection{}
	s.Distinct = pb.GetDistinct()
	s.colNames = make(map[string]struct{}, len(pb.ColNames))
	for _, name := range pb.ColNames {
		s.colNames[name] = struct{}{}
	}
	s.Columns = make(ResultColumns, len(pb.Columns))
	for i, pbc := range pb.Columns {
		s.Columns[i] = resultColumnFromPb(pbc)
	}
	return &s
}
func projectionToPb(m *Projection) *ProjectionPb {
	s := ProjectionPb{}
	s.Distinct = m.Distinct
	if len(m.colNames) > 0 {
		s.ColNames = make([]string, 0, len(m.colNames))
		for name, _ := range m.colNames {
			s.ColNames = append(s.ColNames, name)
		}
	}
	if len(m.Columns) > 0 {
		s.Columns = make([]*ResultColumnPb, len(m.Columns))
		for i, c := range m.Columns {
			s.Columns[i] = resultColumnToPb(c)
		}
	}
	return &s
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
func (m *Columns) AliasedFieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		names[i] = col.As
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
	if fn, ok := m.Expr.(*expr.FuncNode); ok {
		u.Infof("countStar? %T  %#v", m.Expr, m.Expr)
		u.Debugf("args? %s", fn.Args[0].String())
		return strings.ToLower(fn.Name) == "count" && fn.Args[0].String() == `*`
	}
	return false
}
func (m *Column) InFinalProjection() bool {
	return m.ParentIndex >= 0
}

func (m *Column) Equal(c *Column) bool {
	if m == nil && c == nil {
		return true
	}
	if m == nil && c != nil {
		return false
	}
	if m != nil && c == nil {
		return false
	}
	if m.sourceQuoteByte != c.sourceQuoteByte {
		return false
	}
	if m.asQuoteByte != c.asQuoteByte {
		return false
	}
	if m.originalAs != c.originalAs {
		return false
	}
	if m.left != c.left {
		return false
	}
	if m.right != c.right {
		return false
	}
	if m.ParentIndex != c.ParentIndex {
		return false
	}
	if m.Index != c.Index {
		return false
	}
	if m.SourceIndex != c.SourceIndex {
		return false
	}
	if m.SourceField != c.SourceField {
		return false
	}
	if m.As != c.As {
		return false
	}
	if m.Comment != c.Comment {
		return false
	}
	if m.Order != c.Order {
		return false
	}
	if m.Star != c.Star {
		return false
	}
	if m.Expr != nil {
		if !m.Expr.Equal(c.Expr) {
			return false
		}
	}
	if m.Guard != nil {
		if !m.Guard.Equal(c.Guard) {
			return false
		}
	}
	return true
}

// Create a new copy of this column for rewrite purposes re-alias
//
func (m *Column) CopyRewrite(alias string) *Column {
	left, right, _ := m.LeftRight()
	newCol := m.Copy()
	//u.Warnf("in rewrite:  Alias:'%s'  '%s'.'%s'  sourcefield:'%v'", alias, left, right, m.SourceField)
	if left == alias {
		newCol.SourceField = right
		newCol.right = right
	}
	// if strings.HasPrefix(newCol.As, left) {
	// 	newCol.As = newCol.As[len(left):]
	// } else {
	// 	//u.Infof("no prefix? as=%q  left=%q", newCol.As, left)
	// }
	if newCol.Expr != nil && newCol.Expr.String() == m.SourceField {
		//u.Warnf("replace identity")
		newCol.Expr = &expr.IdentityNode{Text: right}
	}

	//u.Infof("%s", newCol.String())
	return newCol
}
func (m *Column) Copy() *Column {
	return &Column{
		sourceQuoteByte: m.sourceQuoteByte,
		asQuoteByte:     m.asQuoteByte,
		originalAs:      m.originalAs,
		ParentIndex:     m.ParentIndex,
		Index:           m.Index,
		SourceField:     m.SourceField,
		As:              m.right,
		Comment:         m.Comment,
		Order:           m.Order,
		Star:            m.Star,
		Expr:            m.Expr,
		Guard:           m.Guard,
	}
}
func (m *Column) ToPB() *ColumnPb {
	n := ColumnPb{}
	n.SourceQuote = []byte{m.sourceQuoteByte}
	n.AsQuoteByte = []byte{m.asQuoteByte}
	if len(m.originalAs) > 0 {
		n.OriginalAs = &m.originalAs
	}
	if len(m.left) > 0 {
		n.Left = &m.left
	}
	if len(m.right) > 0 {
		n.Right = &m.right
	}
	n.ParentIndex = int32(m.ParentIndex)
	n.Index = int32(m.Index)
	n.SourceIndex = int32(m.SourceIndex)
	if len(m.SourceField) > 0 {
		n.SourceField = &m.SourceField
	}
	n.As = m.As
	if len(m.Comment) > 0 {
		n.Comment = &m.Comment
	}
	if len(m.Order) > 0 {
		n.Order = &m.Order
	}
	if m.Star {
		n.Star = &m.Star
	}
	if m.Expr != nil {
		n.Expr = m.Expr.ToPB()
	}
	if m.Guard != nil {
		n.Guard = m.Guard.ToPB()
	}
	return &n
}
func columnFromPb(c *ColumnPb) *Column {
	return &Column{
		sourceQuoteByte: optionalByte(c.GetSourceQuote()),
		asQuoteByte:     optionalByte(c.GetAsQuoteByte()),
		originalAs:      c.GetOriginalAs(),
		left:            c.GetLeft(),
		right:           c.GetRight(),
		ParentIndex:     int(c.GetParentIndex()),
		Index:           int(c.GetIndex()),
		SourceIndex:     int(c.GetSourceIndex()),
		SourceField:     c.GetSourceField(),
		As:              c.GetAs(),
		Order:           c.GetOrder(),
		Star:            c.GetStar(),
		Expr:            expr.NodeFromNodePb(c.GetExpr()),
		Guard:           expr.NodeFromNodePb(c.GetGuard()),
	}
}

// Return left, right values if is of form   `table.column` and
// also return true/false for if it even has left/right
func (m *Column) LeftRight() (string, string, bool) {
	if m.right == "" {
		m.left, m.right, _ = expr.LeftRight(m.As)
	}
	return m.left, m.right, m.left != ""
}

func (m *PreparedStatement) Keyword() lex.TokenType { return lex.TokenPrepare }
func (m *PreparedStatement) String() string {
	return fmt.Sprintf("PREPARE %s FROM %s", m.Alias, m.Statement.String())
}
func (m *PreparedStatement) FingerPrint(r rune) string {
	return fmt.Sprintf("PREPARE %s FROM %s", m.Alias, m.Statement.FingerPrint(r))
}

func (m *SqlSelect) Keyword() lex.TokenType { return lex.TokenSelect }
func (m *SqlSelect) SystemQry() bool        { return len(m.From) == 0 && m.schemaqry }
func (m *SqlSelect) SetSystemQry()          { m.schemaqry = true }
func (m *SqlSelect) FromPB(spb *SqlSelectPb) *SqlSelect {
	return SqlSelectFromPb(spb)
}
func (m *SqlSelect) ToPbStatement() *SqlStatementPb {
	if m.pb == nil {
		m.pb = &SqlStatementPb{Select: SqlSelectToPb(m)}
	}
	return m.pb
}
func (m *SqlSelect) ToPB() *SqlSelectPb {
	return m.ToPbStatement().Select
}
func SqlSelectToPb(m *SqlSelect) *SqlSelectPb {
	return sqlSelectToPbDepth(m, 0)
}
func sqlSelectToPbDepth(m *SqlSelect, depth int) *SqlSelectPb {
	//u.Debugf("SqlSelectToPb %d? %p", depth, m)
	s := SqlSelectPb{}
	s.Db = m.Db
	s.Raw = m.Raw
	s.Star = m.Star
	s.Distinct = m.Distinct
	s.Limit = int32(m.Limit)
	s.Offset = int32(m.Offset)
	s.IsAgg = m.isAgg
	s.Finalized = m.finalized
	s.Schemaqry = m.schemaqry
	if len(m.Alias) > 0 {
		s.Alias = &m.Alias
	}
	if m.Where != nil {
		s.Where = SqlWhereToPb(m.Where)
	}
	if m.proj != nil {
		s.Projection = projectionToPb(m.proj)
	}
	if m.Having != nil {
		s.Having = m.Having.ToPB()
	}
	if len(m.Columns) > 0 {
		s.Columns = ColumnsToPb(m.Columns)
	}
	if len(m.GroupBy) > 0 {
		s.GroupBy = ColumnsToPb(m.GroupBy)
	}
	if len(m.OrderBy) > 0 {
		s.OrderBy = ColumnsToPb(m.OrderBy)
	}
	if len(m.From) > 0 && depth == 0 {
		s.From = make([]*SqlSourcePb, len(m.From))
		for i, from := range m.From {
			s.From[i] = from.ToPB()
		}
	}
	if len(m.With) > 0 {
		by, err := json.Marshal(m.With)
		if err != nil {
			u.Errorf("unhandled error json with? %v", err)
		} else {
			s.With = by
		}
	}
	if m.Into != nil {
		s.Into = &m.Into.Table
	}
	return &s
}
func (m *SqlSelect) Equal(ss SqlStatement) bool {
	s, ok := ss.(*SqlSelect)
	if !ok {
		return false
	}
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Db != s.Db {
		return false
	}
	if m.Raw != s.Raw {
		return false
	}
	if m.Star != s.Star {
		return false
	}
	if m.Distinct != s.Distinct {
		return false
	}
	if m.Limit != s.Limit {
		return false
	}
	if m.Offset != s.Offset {
		return false
	}
	if m.Alias != s.Alias {
		return false
	}
	if m.isAgg != s.isAgg {
		return false
	}
	if m.finalized != s.finalized {
		return false
	}
	if m.schemaqry != s.schemaqry {
		return false
	}
	if !m.Into.Equal(s.Into) {
		return false
	}
	if m.Where != nil && !m.Where.Equal(s.Where) {
		return false
	}
	if m.Having != nil && !m.Having.Equal(s.Having) {
		return false
	}

	if len(m.Columns) != len(s.Columns) {
		return false
	}
	for i, c := range m.Columns {
		if !c.Equal(s.Columns[i]) {
			return false
		}
	}
	if len(m.From) != len(s.From) {
		return false
	}
	for i, c := range m.From {
		if !c.Equal(s.From[i]) {
			return false
		}
	}
	if len(m.GroupBy) != len(s.GroupBy) {
		return false
	}
	for i, c := range m.GroupBy {
		if !c.Equal(s.GroupBy[i]) {
			return false
		}
	}
	if len(m.OrderBy) != len(s.OrderBy) {
		return false
	}
	for i, c := range m.OrderBy {
		if !c.Equal(s.OrderBy[i]) {
			return false
		}
	}
	if !m.proj.Equal(s.proj) {
		return false
	}
	return true
}
func SqlSelectFromPb(pb *SqlSelectPb) *SqlSelect {
	ss := SqlSelect{
		Db:        pb.GetDb(),
		Raw:       pb.GetRaw(),
		Star:      pb.GetStar(),
		Distinct:  pb.GetDistinct(),
		Alias:     pb.GetAlias(),
		Limit:     int(pb.GetLimit()),
		Offset:    int(pb.GetOffset()),
		isAgg:     pb.GetIsAgg(),
		finalized: pb.GetFinalized(),
		schemaqry: pb.GetSchemaqry(),
	}
	if pb.Into != nil {
		ss.Into = &SqlInto{pb.GetInto()}
	}
	if pb.Where != nil {
		ss.Where = SqlWhereFromPb(pb.GetWhere())
	}
	if pb.Having != nil {
		ss.Having = expr.NodeFromNodePb(pb.GetHaving())
	}
	if pb.Projection != nil {
		ss.proj = ProjectionFromPb(pb.GetProjection())
	}
	if len(pb.Columns) > 0 {
		ss.Columns = ColumnsFromPb(pb.GetColumns())
	}
	if len(pb.GroupBy) > 0 {
		ss.GroupBy = ColumnsFromPb(pb.GetGroupBy())
	}
	if len(pb.OrderBy) > 0 {
		ss.OrderBy = ColumnsFromPb(pb.GetOrderBy())
	}
	if len(pb.From) > 0 {
		ss.From = make([]*SqlSource, len(pb.From))
		for i, fpb := range pb.From {
			ss.From[i] = SqlSourceFromPb(fpb)
		}
	}
	if len(pb.With) > 0 {
		ss.With = make(u.JsonHelper)
		json.Unmarshal(pb.With, &ss.With)
	}
	return &ss
}
func (m *SqlSelect) IsAggQuery() bool {
	if m.isAgg || len(m.GroupBy) > 0 {
		return true
	}
	return false
}
func (m *SqlSelect) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(0, &buf)
	return buf.String()
}
func (m *SqlSelect) writeBuf(depth int, buf *bytes.Buffer) {

	buf.WriteString("SELECT ")
	if m.Distinct {
		buf.WriteString("DISTINCT ")
	}
	m.Columns.writeBuf(buf)
	if m.Into != nil {
		buf.WriteString(fmt.Sprintf(" INTO %v", m.Into))
	}
	if m.From != nil {
		buf.WriteString(" FROM")
		for i, from := range m.From {
			if i == 0 {
				buf.WriteByte(' ')
			} else {
				if from.SubQuery != nil {
					buf.WriteByte('\n')
					buf.WriteString(strings.Repeat("\t", depth+1))
				} else {
					buf.WriteByte('\n')
					buf.WriteString(strings.Repeat("\t", depth+1))
				}
			}
			from.writeBuf(depth+1, buf)
		}
	}
	if m.Where != nil {
		buf.WriteString(" WHERE ")
		m.Where.writeBuf(buf)
	}
	if m.GroupBy != nil {
		buf.WriteString(" GROUP BY ")
		m.GroupBy.writeBuf(buf)
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
	if m.fingerprintid == 0 {
		h := fnv.New64()
		h.Write([]byte(m.FingerPrint(rune('?'))))
		m.fingerprintid = int64(h.Sum64())
	}
	return m.fingerprintid
}

// Finalize this Query plan by preparing sub-sources
//  ie we need to rewrite some things into sub-statements
//  - we need to share the join expression across sources
func (m *SqlSelect) Finalize() error {
	if m.finalized {
		return nil
	}
	m.finalized = true
	if len(m.From) == 0 {
		return nil
	}

	// TODO:   This is invalid, as you can have more than one join on a table
	//exprs := make(map[string]Node)

	//cols := m.UnAliasedColumns()

	for _, from := range m.From {
		from.Finalize()
		//from.cols = cols
		//left, right, ok := from.LeftRight()
		// if from.JoinExpr != nil {
		// 	left, right := from.findFromAliases()
		// 	//u.Debugf("from1:%v  from2:%v   joinexpr:  %v", left, right, from.JoinExpr.String())
		// 	exprs[left] = from.JoinExpr
		// 	exprs[right] = from.JoinExpr
		// }
		//u.Debugf("from.Alias:%v from.Name:%v  from:%#v", from.Alias, from.Name, from)
		//exprs[strings.ToLower(from.Alias)] = from.JoinExpr
	}
	// for name, expr := range exprs {
	// 	u.Debugf("EXPR:   name: %v  expr:%v", name, expr.String())
	// }
	// for _, from := range m.From {
	// 	if from.JoinExpr == nil {
	// 		//u.Debugf("from join nil?%v  %v", from.JoinExpr == nil, from)
	// 		if expr, ok := exprs[from.alias]; ok {
	// 			//u.Warnf("NICE found: %#v", expr)
	// 			from.JoinExpr = expr
	// 		}
	// 	}
	// }

	return nil
}

func (m *SqlSelect) UnAliasedColumns() map[string]*Column {
	cols := make(map[string]*Column, len(m.Columns))
	for _, col := range m.Columns {
		_, right, _ := col.LeftRight()
		cols[right] = col
	}
	return cols
}
func (m *SqlSelect) AliasedColumns() map[string]*Column {
	cols := make(map[string]*Column, len(m.Columns))
	for _, col := range m.Columns {
		//u.Debugf("aliasing: key():%-15q  As:%-15q   %-15q", col.Key(), col.As, col.String())
		cols[col.Key()] = col
	}
	return cols
}
func (m *SqlSelect) ColIndexes() map[string]int {
	cols := make(map[string]int, len(m.Columns))
	for i, col := range m.Columns {
		//u.Debugf("aliasing: key():%-15q  As:%-15q   %-15q", col.Key(), col.As, col.String())
		cols[col.Key()] = i
	}
	return cols
}

func (m *SqlSelect) AddColumn(colArg Column) error {
	col := &colArg
	col.Index = len(m.Columns)
	m.Columns = append(m.Columns, col)

	if col.As == "" && col.Expr == nil {
		u.Errorf("no as or expression?  %#s", col)
	}
	if col.Agg && !m.isAgg {
		m.isAgg = true
	}
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
	if f, ok := col.Expr.(*expr.FuncNode); ok {
		if strings.ToLower(f.Name) != "count" {
			return false
		}
		if len(f.Args) == 1 && f.Args[0].String() == "*" {
			return true
		}
	}
	return false
}

func (m *SqlSelect) Rewrite() {
	for _, f := range m.From {
		f.Rewrite(m)
	}
}

// Is this a internal variable query?
//     @@max_packet_size   ??
//     @@session.auto_inrcement, @@character_set_client, ...
func (m *SqlSelect) IsSysQuery() bool {

	if len(m.From) > 0 {
		return false
	}
	if len(m.Columns) < 1 {
		return false
	}
	col := m.Columns[0]
	if col.Expr == nil {
		return false
	}
	switch n := col.Expr.(type) {
	case *expr.IdentityNode:
		if strings.HasPrefix(n.Text, "@@") {
			return true
		}
		// SELECT current_user
		return true //n.Text
	case *expr.FuncNode:
		// SELECT current_user()
		return true // n.String()
	}
	u.Warnf("wat? %v", col)
	return false
}

func (m *SqlSource) Keyword() lex.TokenType { return m.Op }
func (m *SqlSource) SourceName() string {
	if m.SubQuery != nil {
		if len(m.SubQuery.From) == 1 {
			return m.SubQuery.From[0].Name
		}
		u.Warnf("could not find source name bc SubQuery had %d sources", len(m.SubQuery.From))
		return ""
	}
	_, right, hasLeft := expr.LeftRight(m.Name)
	if hasLeft {
		return right
	}
	return right
}
func (m *SqlSource) String() string {
	buf := bytes.Buffer{}
	m.writeBuf(0, &buf)
	return buf.String()
}
func (m *SqlSource) writeBuf(depth int, buf *bytes.Buffer) {

	if int(m.Op) == 0 && int(m.LeftOrRight) == 0 && int(m.JoinType) == 0 {
		if m.Alias != "" {
			buf.WriteString(fmt.Sprintf("%s AS %v", m.Name, m.Alias))
			return
		}
		if m.Schema == "" {
			buf.WriteString(expr.IdentityMaybeQuote('`', m.Name))
		} else {
			buf.WriteByte('`')
			buf.WriteString(m.Schema)
			buf.WriteString("`.`")
			buf.WriteString(m.Name)
			buf.WriteByte('`')
		}
		return
	}
	//u.Warnf("op:%d leftright:%d jointype:%d", m.Op, m.LeftRight, m.JoinType)
	//   Jointype                Op
	//  INNER JOIN orders AS o 	ON
	if int(m.JoinType) != 0 {
		buf.WriteString(strings.ToTitle(m.JoinType.String())) // inner/outer
		buf.WriteByte(' ')
	}
	buf.WriteString("JOIN ")

	if m.SubQuery != nil {
		buf.WriteString("(\n" + strings.Repeat("\t", depth+1))
		m.SubQuery.writeBuf(depth+1, buf)
		buf.WriteString("\n" + strings.Repeat("\t", depth) + ")")
	} else {
		if m.Schema == "" {
			buf.WriteString(expr.IdentityMaybeQuote('`', m.Name))
		} else {
			buf.WriteByte('`')
			buf.WriteString(m.Schema)
			buf.WriteString("`.`")
			buf.WriteString(m.Name)
			buf.WriteByte('`')
		}

	}
	if m.Alias != "" {
		buf.WriteString(" AS ")
		buf.WriteString(m.Alias)
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

func (m *SqlSource) BuildColIndex(colNames []string) error {
	if len(m.colIndex) == 0 {
		m.colIndex = make(map[string]int, len(colNames))
	}
	if len(colNames) == 0 {
		u.LogTracef(u.WARN, "No columns?")
	}
	for _, col := range m.Source.Columns {
		found := false
		for colIdx, colName := range colNames {
			//u.Debugf("col.Key():%v  sourceField:%v  colName:%v", col.Key(), col.SourceField, colName)
			if colName == col.Key() || col.SourceField == colName { //&&
				//u.Debugf("build col:  idx=%d  key=%-15q as=%-15q col=%-15s sourcidx:%d", len(m.colIndex), col.Key(), col.As, col.String(), colIdx)
				m.colIndex[col.Key()] = colIdx
				col.SourceIndex = colIdx
				found = true
				break
			}
		}
		if !found {
			// This is most likely NOT a bug, as `select email, 3 from users`
			// the 3 column is valid literal but no key/source
			//u.Debugf("could not find col: %v  %v", col.Key(), colNames)
		}
	}
	return nil
}

// Rewrite this Source to act as a stand-alone query to backend
//  @parentStmt = the parent statement that this a partial source to
func (m *SqlSource) Rewrite(parentStmt *SqlSelect) *SqlSelect {

	//u.Debugf("Rewrite %s", m.String())
	if m.Source != nil {
		return m.Source
	}
	// Rewrite this SqlSource for the given parent, ie
	//   1)  find the column names we need to request from source including those used in join/where
	//   2)  rewrite the where for this partial query
	//   3)  any columns in join expression that are not equal between
	//          sides should be aliased towards the left-hand join portion
	//   4)  if we need different sort for our join algo?

	newCols := make(Columns, 0)
	if !parentStmt.Star {
		for idx, col := range parentStmt.Columns {
			left, _, hasLeft := col.LeftRight()
			//u.Infof("col: P:%p hasLeft?%v %q", col, hasLeft, col)
			if !hasLeft {
				// Was not left/right qualified, so use as is?  or is this an error?
				//  what is official sql grammar on this?
				//u.Warnf("unknown col alias?: %#v", col)
				newCol := col.Copy()
				newCol.ParentIndex = idx
				newCol.Index = len(newCols)
				newCols = append(newCols, newCol)

			} else if hasLeft && left == m.Alias {
				//u.Debugf("CopyRewrite: %v  P:%p %#v", m.Alias, col, col)
				newCol := col.CopyRewrite(m.Alias)
				//newCol := col.Copy()
				// Now Rewrite the Join Expression
				// n := rewriteNode(m, col.Expr)
				// if n != nil {
				// 	newCol.Expr = n
				// }
				//u.Infof("newCol?  %+v", newCol)
				newCol.ParentIndex = idx
				newCol.SourceIndex = len(newCols)
				newCol.Index = len(newCols)
				newCols = append(newCols, newCol)
				//u.Debugf("source rewrite: %s idx:%d sidx:%d pidx:%d", newCol.As, newCol.Index, newCol.SourceIndex, newCol.ParentIndex)

			} else {
				// not used in this source
				//u.Debugf("sub-query does not use this parent col?  FROM %v  col:%v", m.Name, col.As)
			}
		}
	}

	// TODO:
	//  - rewrite the Sort
	//  - rewrite the group-by
	sql2 := &SqlSelect{Columns: newCols, Star: parentStmt.Star}
	m.joinNodes = make([]expr.Node, 0)
	if m.SubQuery != nil {
		if len(m.SubQuery.From) != 1 {
			u.Errorf("Not supported, nested subQuery %v", m.SubQuery.String())
		} else {
			sql2.From = append(sql2.From, &SqlSource{Name: m.SubQuery.From[0].Name})
		}
	} else {
		sql2.From = append(sql2.From, &SqlSource{Name: m.Name})
	}

	for _, from := range parentStmt.From {
		// We need to check each participant in the Join for possible
		// columns which need to be re-written
		sql2.Columns = columnsFromJoin(m, from.JoinExpr, sql2.Columns)

		// We also need to create an expression used for evaluating
		// the values of Join "Keys"
		if from.JoinExpr != nil {
			//preNodeCt := len(m.joinNodes)
			//u.Debugf("from: %q     joinP: %p  join: %q", from.String(), from.JoinExpr, from.JoinExpr.String())
			joinNodesForFrom(parentStmt, m, from.JoinExpr, 0)
			//u.Debugf("P %p pre:%v  post:%v  for:%q", m, preNodeCt, len(m.joinNodes), m.String())

		} else {
			//u.Debugf("nil join? %v", from.String())
		}
	}
	// for _, jn := range m.joinNodes {
	// 	u.Debugf("jh %s", jn.String())
	// }
	//u.Debugf("cols len: %v", len(sql2.Columns))
	if parentStmt.Where != nil {
		node, cols := rewriteWhere(parentStmt, m, parentStmt.Where.Expr, make(Columns, 0))
		if node != nil {
			//u.Warnf("node string():  %v", node.String())
			sql2.Where = &SqlWhere{Expr: node}
		}
		if len(cols) > 0 {
			//u.Warnf("new where cols:   %#v", cols)
			parentIdx := len(parentStmt.Columns)
			for _, col := range cols {
				col.Index = len(sql2.Columns)
				col.ParentIndex = parentIdx
				//u.Warnf("added col: %s   pidx:%d", col.As, parentIdx)
				parentIdx++
				sql2.Columns = append(sql2.Columns, col)
				//u.Warnf("added col: %v", col.String())
			}
		}
	}
	m.Source = sql2
	//u.Infof("going to unaliase: #cols=%v %#v", len(sql2.Columns), sql2.Columns)
	m.cols = sql2.UnAliasedColumns()
	//u.Infof("after aliasing: %#v \n\tsql2=%s", m.cols, sql2.String())
	return sql2
}

func (m *SqlSource) findFromAliases() (string, string) {
	from1, from2 := m.alias, ""
	if m.JoinExpr != nil {
		switch nt := m.JoinExpr.(type) {
		case *expr.BinaryNode:
			if in, ok := nt.Args[0].(*expr.IdentityNode); ok {
				if left, _, ok := in.LeftRight(); ok {
					from1 = left
				}
			}
			if in, ok := nt.Args[1].(*expr.IdentityNode); ok {
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

func rewriteWhere(stmt *SqlSelect, from *SqlSource, node expr.Node, cols Columns) (expr.Node, Columns) {

	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, hasLeft := nt.LeftRight(); hasLeft {
			//u.Debugf("rewriteWhere  from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := expr.IdentityNode{Text: right}
				cols = append(cols, NewColumn(right))
				//u.Warnf("nice, found it! in = %v  cols:%d", in, len(cols))
				return &in, cols
			} else {
				//u.Warnf("what to do? source:%v    %v", from.alias, nt.String())
			}
		} else {
			//u.Warnf("dropping where: %#v", nt)
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode:
		return nt, cols
	case *expr.BinaryNode:
		//u.Infof("binaryNode  T:%v", nt.Operator.T.String())
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			var n1, n2 expr.Node
			n1, cols = rewriteWhere(stmt, from, nt.Args[0], cols)
			n2, cols = rewriteWhere(stmt, from, nt.Args[1], cols)

			if n1 != nil && n2 != nil {
				return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}, cols
			} else if n1 != nil {
				return n1, cols
			} else if n2 != nil {
				return n2, cols
			} else {
				//u.Warnf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			}
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenGT, lex.TokenGE, lex.TokenLE, lex.TokenNE:
			var n1, n2 expr.Node
			n1, cols = rewriteWhere(stmt, from, nt.Args[0], cols)
			n2, cols = rewriteWhere(stmt, from, nt.Args[1], cols)
			//u.Debugf("n1=%#v  n2=%#v    %#v", n1, n2, nt)
			if n1 != nil && n2 != nil {
				return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}, cols
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
	return nil, cols
}

func joinNodesForFrom(stmt *SqlSelect, from *SqlSource, node expr.Node, depth int) expr.Node {

	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, hasLeft := nt.LeftRight(); hasLeft {
			//u.Debugf("joinNodesForFrom  from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				identNode := expr.IdentityNode{Text: right}
				//u.Debugf("%d nice, found it! identnode=%q fromnode:%q", depth, identNode.String(), nt.String())
				if depth == 1 {
					from.joinNodes = append(from.joinNodes, &identNode)
					return nil
				}
				return &identNode
			} else {
				// This is for other side of join, ignore
				//u.Warnf("what to do? source:%v    %v", from.alias, nt.String())
			}
		} else {
			u.Warnf("dropping join expr node: %q", nt.String())
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode, *expr.ValueNode:
		//u.Warnf("skipping? %v", nt.String())
		return nt
	case *expr.FuncNode:
		//u.Warnf("%v  try join from func node: %v", depth, nt.String())
		args := make([]expr.Node, len(nt.Args))
		for i, arg := range nt.Args {
			args[i] = rewriteNode(from, arg)
			if args[i] == nil {
				// What???
				//u.Infof("error, from:%q   arg:%q", from.String(), arg.String())
				return nil
			}
		}
		fn := expr.NewFuncNode(nt.Name, nt.F)
		fn.Args = args
		if depth == 1 {
			//u.Infof("adding func: %s", fn.String())
			from.joinNodes = append(from.joinNodes, fn)
			return nil
		}
		return fn
	case *expr.BinaryNode:
		//u.Infof("%v binaryNode  %v", depth, nt.String())
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			n1 := joinNodesForFrom(stmt, from, nt.Args[0], depth+1)
			n2 := joinNodesForFrom(stmt, from, nt.Args[1], depth+1)

			if n1 != nil && n2 != nil {
				//u.Debugf("%d neither nil:  n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				//return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
			} else if n1 != nil {
				//u.Debugf("%d n1 not nil: n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				return n1
			} else if n2 != nil {
				//u.Debugf("%d n2 not nil n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				return n2
			} else {
				//u.Warnf("%d n1=%#v  n2=%#v    %#v", depth, n1, n2, nt)
			}
		case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenGT, lex.TokenGE, lex.TokenLE, lex.TokenNE:
			n1 := joinNodesForFrom(stmt, from, nt.Args[0], depth+1)
			n2 := joinNodesForFrom(stmt, from, nt.Args[1], depth+1)

			if n1 != nil && n2 != nil {
				//u.Debugf("%d neither nil:  n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				//return &BinaryNode{Operator: nt.Operator, Args: [2]Node{n1, n2}}
			} else if n1 != nil {
				//u.Debugf("%d n1 not nil: n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				// 	return n1
				if depth == 1 {
					//u.Infof("adding node: %s", n1.String())
					from.joinNodes = append(from.joinNodes, n1)
					return nil
				}
			} else if n2 != nil {
				//u.Debugf("%d  n2 not nil n1=%v  n2=%v    %q", depth, n1, n2, nt.String())
				if depth == 1 {
					//u.Infof("adding node: %s", n1.String())
					from.joinNodes = append(from.joinNodes, n2)
					return nil
				}
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
func columnsFromJoin(from *SqlSource, node expr.Node, cols Columns) Columns {
	if node == nil {
		return cols
	}
	//u.Debugf("columnsFromJoin()  T:%T  node=%q", node, node.String())
	switch nt := node.(type) {
	case *expr.IdentityNode:
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
						//u.Infof("columnsFromJoin from.Name:%v l:%v  r:%v", from.alias, left, right)
					} else {
						//u.Warnf("not? from.Name:%v l:%v  r:%v   col: P:%p %#v", from.alias, left, right, col, col)
					}
				}
				if !found {
					//u.Debugf("columnsFromJoin from.Name:%v l:%v  r:%v", from.alias, left, right)
					newCol := &Column{As: right, SourceField: right, Expr: &expr.IdentityNode{Text: right}}
					newCol.Index = len(cols)
					newCol.ParentIndex = -1 // if -1, we don't need in parent index
					cols = append(cols, newCol)
					//u.Warnf("added col %s idx:%d pidx:%v", right, newCol.Index, newCol.Index)
				}
			}
		}
	case *expr.FuncNode:
		//u.Warnf("columnsFromJoin func node: %s", nt.String())
		for _, arg := range nt.Args {
			cols = columnsFromJoin(from, arg, cols)
		}
	case *expr.BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			cols = columnsFromJoin(from, nt.Args[0], cols)
			cols = columnsFromJoin(from, nt.Args[1], cols)
		case lex.TokenEqual, lex.TokenEqualEqual:
			cols = columnsFromJoin(from, nt.Args[0], cols)
			cols = columnsFromJoin(from, nt.Args[1], cols)
		default:
			u.Warnf("un-implemented op: %v", nt.Operator)
		}
	default:
		u.LogTracef(u.INFO, "whoops")
		u.Warnf("%T node types are not suppored yet for join rewrite %s", node, from.String())
	}
	return cols
}

// Remove any aliases
func rewriteNode(from *SqlSource, node expr.Node) expr.Node {
	switch nt := node.(type) {
	case *expr.IdentityNode:
		if left, right, ok := nt.LeftRight(); ok {
			//u.Debugf("rewriteNode from.Name:%v l:%v  r:%v", from.alias, left, right)
			if left == from.alias {
				in := expr.IdentityNode{Text: right}
				//u.Warnf("nice, found it! in = %v", in)
				return &in
			}
		}
	case *expr.NumberNode, *expr.NullNode, *expr.StringNode, *expr.ValueNode:
		//u.Warnf("skipping? %v", nt.String())
		return nt
	case *expr.BinaryNode:
		switch nt.Operator.T {
		case lex.TokenAnd, lex.TokenLogicAnd, lex.TokenLogicOr:
			n1 := rewriteNode(from, nt.Args[0])
			n2 := rewriteNode(from, nt.Args[1])
			return &expr.BinaryNode{Operator: nt.Operator, Args: []expr.Node{n1, n2}}
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
	case *expr.FuncNode:
		fn := expr.NewFuncNode(nt.Name, nt.F)
		fn.Args = make([]expr.Node, len(nt.Args))
		for i, arg := range nt.Args {
			fn.Args[i] = rewriteNode(from, arg)
			if fn.Args[i] == nil {
				// What???
				u.Warnf("error, nil node: %s", arg.String())
				return nil
			}
		}
		return fn
	default:
		u.Warnf("%T node types are not suppored yet for column rewrite", node)
	}
	return nil
}

// Get a list of Un-Aliased Columns, ie columns with column
//  names that have NOT yet been aliased
func (m *SqlSource) UnAliasedColumns() map[string]*Column {
	//u.Warnf("un-aliased %d", len(m.Source.Columns))
	if len(m.cols) > 0 || m.Source != nil && len(m.Source.Columns) == 0 {
		return m.cols
	}

	cols := make(map[string]*Column, len(m.Source.Columns))
	for _, col := range m.Source.Columns {
		_, right, hasLeft := col.LeftRight()
		//u.Debugf("aliasing: l:%q r:%q hasLeft?%v", left, right, hasLeft)
		if hasLeft {
			cols[right] = col
		} else {
			cols[right] = col
		}
	}
	return cols
}

// Get a list of Column names to position
func (m *SqlSource) ColumnPositions() map[string]int {
	if len(m.colIndex) > 0 {
		return m.colIndex
	}
	if m.Source == nil {
		return nil
	}
	cols := make(map[string]int)
	for idx, col := range m.Source.Columns {
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
func (m *SqlSource) JoinNodes() []expr.Node {
	return m.joinNodes
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
func (m *SqlSource) FromPB(n *SqlSourcePb) *SqlSource {
	return SqlSourceFromPb(n)
}
func (m *SqlSource) ToPB() *SqlSourcePb {
	if m.pb == nil {
		m.pb = sqlSourceToPb(m)
	}
	return m.pb
}
func (m *SqlSource) Equal(s *SqlSource) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}

	if m.final != s.final {
		return false
	}
	if m.alias != s.alias {
		return false
	}
	if m.Raw != s.Raw {
		return false
	}
	if m.Name != s.Name {
		return false
	}
	if m.Alias != s.Alias {
		return false
	}
	if m.Op != s.Op {
		return false
	}
	if m.LeftOrRight != s.LeftOrRight {
		return false
	}
	if m.JoinType != s.JoinType {
		return false
	}
	if m.Seekable != s.Seekable {
		return false
	}
	if m.JoinExpr != nil && !m.JoinExpr.Equal(s.JoinExpr) {
		return false
	}
	if len(m.cols) != len(s.cols) {
		return false
	}
	for k, c := range m.cols {
		sc, ok := s.cols[k]
		if !ok {
			return false
		}
		if !c.Equal(sc) {
			return false
		}
	}
	if len(m.colIndex) != len(s.colIndex) {
		return false
	}
	for k, midx := range m.colIndex {
		sidx, ok := s.colIndex[k]
		if !ok {
			return false
		}
		if midx != sidx {
			return false
		}
	}
	if len(m.joinNodes) != len(s.joinNodes) {
		return false
	}
	for i, jn := range m.joinNodes {
		if !jn.Equal(s.joinNodes[i]) {
			return false
		}
	}
	if !m.SubQuery.Equal(s.SubQuery) {
		return false
	}
	return true
}
func sqlSourceToPb(m *SqlSource) *SqlSourcePb {
	s := SqlSourcePb{}
	cols := make([]*ColumnPb, 0, len(m.cols))
	for k, col := range m.cols {
		col.As = k
		cols = append(cols, col.ToPB())
	}
	s.Columns = cols
	s.Final = m.final
	s.Seekable = m.Seekable
	s.Raw = m.Raw
	s.Name = m.Name
	s.Alias = m.Alias
	s.Op = int32(m.Op)
	s.LeftOrRight = int32(m.LeftOrRight)
	s.JoinType = int32(m.JoinType)
	if len(m.alias) > 0 {
		s.AliasInner = &m.alias
	}
	kvs := make([]KvInt, 0, len(m.colIndex))
	for k, v := range m.colIndex {
		kvs = append(kvs, KvInt{K: k, V: int32(v)})
	}
	s.ColIndex = kvs
	if len(m.joinNodes) > 0 {
		s.JoinNodes = expr.NodesPbFromNodes(m.joinNodes)
	}
	// We get into recursive hell if we don't bail
	// but need to go stich in source?
	if m.Source != nil {
		//u.Warnf("about to descend? %p", m.Source)
		s.Source = sqlSelectToPbDepth(m.Source, 1)
	}
	if m.SubQuery != nil {
		s.SubQuery = SqlSelectToPb(m.SubQuery)
	}
	if m.JoinExpr != nil {
		s.JoinExpr = m.JoinExpr.ToPB()
	}

	return &s
}
func SqlSourceFromPb(pb *SqlSourcePb) *SqlSource {
	s := SqlSource{
		final:       pb.GetFinal(),
		alias:       pb.GetAliasInner(),
		colIndex:    MapIntFromPb(pb.GetColIndex()),
		joinNodes:   expr.NodesFromNodesPbPtr(pb.GetJoinNodes()),
		Raw:         pb.GetRaw(),
		Name:        pb.GetName(),
		Alias:       pb.GetAlias(),
		Op:          lex.TokenType(pb.GetOp()),
		LeftOrRight: lex.TokenType(pb.GetLeftOrRight()),
		JoinType:    lex.TokenType(pb.GetJoinType()),
		JoinExpr:    expr.NodeFromNodePb(pb.GetJoinExpr()),
		Seekable:    pb.GetSeekable(),
	}
	if pb.Source != nil {
		s.Source = SqlSelectFromPb(pb.Source)
	} else {
		u.Warnf("no source for SqlSource? %+v", pb)
	}
	if pb.SubQuery != nil {
		s.SubQuery = SqlSelectFromPb(pb.SubQuery)
	}
	if len(pb.Columns) > 0 {
		s.cols = make(map[string]*Column, len(pb.Columns))
		for _, pbc := range pb.Columns {
			col := columnFromPb(pbc)
			s.cols[col.As] = col
		}
	}
	return &s
}

func (m *SqlWhere) Keyword() lex.TokenType { return m.Op }
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
func (m *SqlWhere) Equal(s *SqlWhere) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Op != s.Op {
		return false
	}
	if !m.Source.Equal(s.Source) {
		return false
	}
	if !m.Expr.Equal(s.Expr) {
		return false
	}
	return true
}
func SqlWhereToPb(m *SqlWhere) *SqlWherePb {
	s := SqlWherePb{}
	s.Op = int32(m.Op)
	if m.Source != nil {
		s.Source = SqlSelectToPb(m.Source)
	}
	if m.Expr != nil {
		s.Expr = m.Expr.ToPB()
	}
	return &s
}
func SqlWhereFromPb(pb *SqlWherePb) *SqlWhere {
	w := SqlWhere{
		Op: lex.TokenType(pb.GetOp()),
	}
	if pb.Source != nil {
		w.Source = SqlSelectFromPb(pb.Source)
	}
	if pb.Expr != nil {
		w.Expr = expr.NodeFromNodePb(pb.GetExpr())
	}
	return &w
}

func (m *SqlInto) Keyword() lex.TokenType    { return lex.TokenInto }
func (m *SqlInto) String() string            { return fmt.Sprintf("%s", m.Table) }
func (m *SqlInto) FingerPrint(r rune) string { return m.String() }
func (m *SqlInto) Equal(s *SqlInto) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Table != s.Table {
		return false
	}
	return true
}

func (m *SqlInsert) Keyword() lex.TokenType { return m.kw }
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

func (m *SqlUpsert) Keyword() lex.TokenType    { return lex.TokenUpsert }
func (m *SqlUpsert) String() string            { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpsert) FingerPrint(r rune) string { return m.String() }
func (m *SqlUpsert) SqlSelect() *SqlSelect     { return sqlSelectFromWhere(m.Table, m.Where) }

func (m *SqlUpdate) Keyword() lex.TokenType { return lex.TokenUpdate }
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

func sqlSelectFromWhere(from string, where *SqlWhere) *SqlSelect {
	req := NewSqlSelect()
	req.From = []*SqlSource{NewSqlSource(from)}
	switch {
	case where.Expr != nil:
		req.Where = NewSqlWhere(where.Expr)
	default:
		req.Where = where
	}

	req.Star = true
	req.Columns = starCols
	return req
}

func (m *SqlDelete) Keyword() lex.TokenType    { return lex.TokenDelete }
func (m *SqlDelete) String() string            { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDelete) FingerPrint(r rune) string { return m.String() }
func (m *SqlDelete) SqlSelect() *SqlSelect     { return sqlSelectFromWhere(m.Table, m.Where) }

func (m *SqlDescribe) Keyword() lex.TokenType    { return lex.TokenDescribe }
func (m *SqlDescribe) String() string            { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDescribe) FingerPrint(r rune) string { return m.String() }

func (m *SqlShow) Keyword() lex.TokenType    { return lex.TokenShow }
func (m *SqlShow) String() string            { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlShow) FingerPrint(r rune) string { return m.String() }

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

func (m *SqlCommand) Keyword() lex.TokenType    { return m.kw }
func (m *SqlCommand) FingerPrint(r rune) string { return m.String() }
func (m *SqlCommand) String() string            { return fmt.Sprintf("%s %s", m.Keyword(), m.Columns.String()) }

// Node serialization helpers
func tokenFromInt(iv int32) lex.Token {
	t, ok := lex.TokenNameMap[lex.TokenType(iv)]
	if ok {
		return lex.Token{T: t.T, V: t.Description}
	}
	return lex.Token{}
}

// Create a sql statement from pb
func SqlFromPb(pb []byte) (SqlStatement, error) {
	s := &SqlStatementPb{}
	if err := proto.Unmarshal(pb, s); err != nil {
		return nil, err
	}
	return statementFromPb(s), nil
}
func statementFromPb(s *SqlStatementPb) SqlStatement {
	switch {
	case s.Select != nil:
		var ss *SqlSelect
		return ss.FromPB(s.Select)
	case s.Source != nil:
		var ss *SqlSource
		return ss.FromPB(s.Source)
	}
	return nil
}
func MapIntFromPb(kv []KvInt) map[string]int {
	m := make(map[string]int, len(kv))
	for _, kv := range kv {
		m[kv.K] = int(kv.V)
	}
	return m
}

func ColumnsFromPb(c []*ColumnPb) Columns {
	cols := make(Columns, len(c))
	for i, col := range c {
		cols[i] = columnFromPb(col)
	}
	return cols
}
func ColumnsToPb(c Columns) []*ColumnPb {
	cols := make([]*ColumnPb, len(c))
	for i, col := range c {
		cols[i] = col.ToPB()
	}
	return cols
}

func optionalByte(b []byte) byte {
	var out byte
	if len(b) > 0 {
		return b[0]
	}
	return out
}
