package qlbridge

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlbridge/lex"
)

var _ = u.EMPTY

type RequestType uint8

const (
	REQUEST_TYPE_ERROR RequestType = iota // this request could not be evaluated, error
	REQUEST_TYPE_CMD                      // internal commands
	REQUEST_TYPE_SQL                      // SQL commands of normal (insert, update, et)
)

// QL Request
type QlRequest interface {
	QLType() RequestType
}

// Sql is a traditional sql command (insert, update, select)
type SqlRequest struct {
	QlRequest
	Columns   Columns
	FromTable string
	Where     map[string]string
	//From    string
}

// Sql Request
func NewSqlRequest() *SqlRequest {
	req := &SqlRequest{}
	req.Columns = make(Columns, 0)
	req.Where = make(map[string]string)
	return req
}

func (m *SqlRequest) String() string {
	return fmt.Sprintf("SELECT ", "hello")
}

func (m *SqlRequest) QLType() RequestType {
	return REQUEST_TYPE_SQL
}

func (m *SqlRequest) AddWhere(t *ql.Token) {
	m.Where[t.V] = t.V
}

// func (m *SqlRequest) getTableName() string {
// 	return m.FromTable
// }

// Array of Columns
type Columns []*Column

func (m *Columns) AddColumn(col string) {
	*m = append(*m, &Column{As: col})
	u.Infof("add col: %s ct=%d", col, len(*m))
}
func (m *Columns) String() string {
	s := make([]string, len(*m))
	for i, col := range *m {
		s[i] = col.String()
	}
	return strings.Join(s, ", ")
}

// Column
type Column struct {
	As     string
	Source ExprOrValue
}

func (m *Column) String() string {
	return m.As
}

// Expressions or Values
type ExprOrValue struct {
	Val  string
	Args []ExprOrValue
}
