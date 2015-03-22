package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// Ensure SqlSelect and cousins etc are NodeTypes
	_ Node = (*SqlSelect)(nil)
	_ Node = (*SqlInsert)(nil)
	_ Node = (*SqlUpsert)(nil)
	_ Node = (*SqlUpdate)(nil)
	_ Node = (*SqlInsert)(nil)
	_ Node = (*SqlSource)(nil)
	_ Node = (*SqlDescribe)(nil)
)

// The sqlStatement interface, to define the sub-types
//  Select, Insert, Delete etc
type SqlStatement interface {
	Accept(visitor Visitor) (interface{}, error)
	Keyword() lex.TokenType
}

type PreparedStatement struct {
	Pos
	Alias     string
	Statement SqlStatement
}

type SqlSelect struct {
	Pos
	Db      string // If provided a use "dbname"
	Raw     string // full original raw statement
	Star    bool
	Columns Columns
	From    []*SqlSource
	Where   *SqlWhere // Expr Node, or *SqlSelect
	Having  Node
	GroupBy Columns
	OrderBy Columns
	Limit   int
	//Join      *SqlSelect
	//FromAlias string //  select name from x AS y
	//SubQuery *SqlSelect // ie WHERE x in (select *)
}

// Source is a table name, sub-query, or join
type SqlSource struct {
	Pos
	Name      string
	Alias     string
	Op        lex.TokenType // In, =, ON
	LeftRight lex.TokenType // Left, Right
	JoinType  lex.TokenType // INNER, OUTER
	Source    *SqlSelect
	JoinExpr  Node
}

// Source is select stmt, or expression
type SqlWhere struct {
	Pos
	Op     lex.TokenType // In, =, ON
	Source *SqlSelect
	Expr   Node
}

type SqlInsert struct {
	Pos
	Columns Columns
	Rows    [][]value.Value
	Into    string
}
type SqlUpsert struct {
	Pos
	Columns Columns
	Rows    [][]value.Value
	Into    string
}
type SqlUpdate struct {
	Pos
	kw      lex.TokenType // Update, Upsert
	Columns Columns
	Where   Node
	From    string
}
type SqlDelete struct {
	Pos
	Table string
	Where Node
	Limit int
}
type SqlShow struct {
	Pos
	Identity string
	From     string
}
type SqlDescribe struct {
	Pos
	Identity string
	Tok      lex.Token // Explain, Describe, Desc
	Stmt     SqlStatement
}
type Join struct {
	Pos
	Identity string
}

type ResultColumns []*ResultColumn

type ResultColumn struct {
	//Expr   Node            // If expression, is here
	Name   string          // Original path/name for query field
	ColPos int             // Ordinal position in sql statement
	Col    *Column         // the original sql column
	Star   bool            // Was this a select * ??
	As     string          // aliased
	Type   value.ValueType // Data Type
}

type Projection struct {
	Distinct bool
	Columns  ResultColumns
}

func NewProjection() *Projection {
	return &Projection{Columns: make(ResultColumns, 0)}
}
func NewResultColumn(as string, ordinal int, col *Column, valtype value.ValueType) *ResultColumn {
	return &ResultColumn{Name: as, As: as, ColPos: ordinal, Col: col, Type: valtype}
}

func (m *Projection) AddColumnShort(name string, vt value.ValueType) {
	m.Columns = append(m.Columns, NewResultColumn(name, len(m.Columns), nil, vt))
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
	req := &SqlUpdate{kw: lex.TokenUpdate}
	req.Columns = make(Columns, 0)
	return req
}
func NewSqlDelete() *SqlDelete {
	return &SqlDelete{}
}
func NewPreparedStatement() *PreparedStatement {
	return &PreparedStatement{}
}

// Array of Columns
type Columns []*Column

func (m *Columns) AddColumn(col *Column) { *m = append(*m, col) }
func (m *Columns) String() string {
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
func (m *Columns) FieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		names[i] = col.Key()
	}
	return names
}

// Column represents the Column as expressed in a [SELECT]
// expression
type Column struct {
	sourceQuoteByte byte
	asQuoteByte     byte
	SourceField     string // field name of underlying field
	As              string // As field, auto-populate the Field Name if exists
	Comment         string // optional in-line comments
	Order           string // (ASC | DESC)
	Star            bool   // If   just *
	Tree            *Tree  // Expression, optional
	Guard           *Tree  // If
}

func NewColumn(tok lex.Token) *Column {
	return &Column{
		As:              tok.V,
		sourceQuoteByte: tok.Quote,
		asQuoteByte:     tok.Quote,
		SourceField:     tok.V,
	}
}
func (m *Column) Key() string { return m.As }
func (m *Column) String() string {
	if m.asQuoteByte == 0 {
		return m.As
	}
	return string(m.asQuoteByte) + m.As + string(m.asQuoteByte)
}

// Is this a select count(*) column
func (m *Column) CountStar() bool {
	if m.Tree == nil || m.Tree.Root == nil {
		return false
	}
	if m.Tree.Root.NodeType() != FuncNodeType {
		return false
	}
	if fn, ok := m.Tree.Root.(*FuncNode); ok {
		return strings.ToLower(fn.Name) == "count" && fn.Args[0].String() == "*"
	}
	return false
}

func (m *PreparedStatement) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitPreparedStmt(m)
}
func (m *PreparedStatement) Keyword() lex.TokenType { return lex.TokenPrepare }
func (m *PreparedStatement) Check() error           { return nil }
func (m *PreparedStatement) Type() reflect.Value    { return nilRv }
func (m *PreparedStatement) NodeType() NodeType     { return SqlPreparedType }
func (m *PreparedStatement) StringAST() string      { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *PreparedStatement) String() string         { return fmt.Sprintf("%s ", m.Keyword()) }

func (m *SqlSelect) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitSelect(m) }
func (m *SqlSelect) Keyword() lex.TokenType                      { return lex.TokenSelect }
func (m *SqlSelect) Check() error                                { return nil }
func (m *SqlSelect) NodeType() NodeType                          { return SqlSelectNodeType }
func (m *SqlSelect) Type() reflect.Value                         { return nilRv }
func (m *SqlSelect) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlSelect) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", m.Columns, m.From))
	if m.Where != nil {
		buf.WriteString(fmt.Sprintf(" WHERE %s ", m.Where.String()))
	}
	return buf.String()
}

// Is this a select count(*) FROM ...   query?
func (m *SqlSelect) CountStar() bool {
	if len(m.Columns) != 1 {
		return false
	}
	col := m.Columns[0]
	if col.Tree == nil || col.Tree.Root == nil {
		return false
	}
	if f, ok := col.Tree.Root.(*FuncNode); ok {
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
	if col.Tree == nil || col.Tree.Root == nil {
		return ""
	}
	if in, ok := col.Tree.Root.(*IdentityNode); ok {
		if strings.HasPrefix(in.Text, "@@") {
			return in.Text
		}
	}
	return ""
}

func (m *SqlSource) Keyword() lex.TokenType { return m.Op }
func (m *SqlSource) Check() error           { return nil }
func (m *SqlSource) Type() reflect.Value    { return nilRv }
func (m *SqlSource) NodeType() NodeType     { return SqlSourceNodeType }
func (m *SqlSource) StringAST() string      { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlSource) String() string         { return fmt.Sprintf("%#v ", m) }

func (m *SqlWhere) Keyword() lex.TokenType { return m.Op }
func (m *SqlWhere) Check() error           { return nil }
func (m *SqlWhere) Type() reflect.Value    { return nilRv }
func (m *SqlWhere) NodeType() NodeType     { return SqlWhereNodeType }
func (m *SqlWhere) StringAST() string {
	if int(m.Op) == 0 && m.Source == nil && m.Expr != nil {
		return m.Expr.StringAST()
	}
	if int(m.Op) != 0 && m.Source != nil {
		fmt.Sprintf("%s (%s)", m.Op.String(), m.Source.StringAST())
	}
	return fmt.Sprintf("%s ", m.Keyword())
}
func (m *SqlWhere) String() string { return fmt.Sprintf("%#v ", m) }

func (m *SqlInsert) Keyword() lex.TokenType                      { return lex.TokenInsert }
func (m *SqlInsert) Check() error                                { return nil }
func (m *SqlInsert) Type() reflect.Value                         { return nilRv }
func (m *SqlInsert) NodeType() NodeType                          { return SqlInsertNodeType }
func (m *SqlInsert) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlInsert) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlInsert) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitInsert(m) }

func (m *SqlUpsert) Keyword() lex.TokenType                      { return lex.TokenUpsert }
func (m *SqlUpsert) Check() error                                { return nil }
func (m *SqlUpsert) Type() reflect.Value                         { return nilRv }
func (m *SqlUpsert) NodeType() NodeType                          { return SqlUpsertNodeType }
func (m *SqlUpsert) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpsert) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpsert) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitUpsert(m) }

func (m *SqlUpdate) Keyword() lex.TokenType                      { return m.kw }
func (m *SqlUpdate) Check() error                                { return nil }
func (m *SqlUpdate) Type() reflect.Value                         { return nilRv }
func (m *SqlUpdate) NodeType() NodeType                          { return SqlUpdateNodeType }
func (m *SqlUpdate) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpdate) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpdate) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitUpdate(m) }

func (m *SqlDelete) Keyword() lex.TokenType                      { return lex.TokenDelete }
func (m *SqlDelete) Check() error                                { return nil }
func (m *SqlDelete) Type() reflect.Value                         { return nilRv }
func (m *SqlDelete) NodeType() NodeType                          { return SqlDeleteNodeType }
func (m *SqlDelete) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDelete) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDelete) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitDelete(m) }

func (m *SqlDescribe) Keyword() lex.TokenType                      { return lex.TokenDescribe }
func (m *SqlDescribe) Check() error                                { return nil }
func (m *SqlDescribe) Type() reflect.Value                         { return nilRv }
func (m *SqlDescribe) NodeType() NodeType                          { return SqlDescribeNodeType }
func (m *SqlDescribe) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDescribe) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDescribe) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitDescribe(m) }

func (m *SqlShow) Keyword() lex.TokenType                      { return lex.TokenShow }
func (m *SqlShow) Check() error                                { return nil }
func (m *SqlShow) Type() reflect.Value                         { return nilRv }
func (m *SqlShow) NodeType() NodeType                          { return SqlShowNodeType }
func (m *SqlShow) StringAST() string                           { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlShow) String() string                              { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlShow) Accept(visitor Visitor) (interface{}, error) { return visitor.VisitShow(m) }
