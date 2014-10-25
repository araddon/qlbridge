package qlparse

import (
	u "github.com/araddon/gou"
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
	Columns   *Columns
	FromTable string
	Where     map[string]string
	//From    string
}

// Sql Request
func NewSqlRequest() *SqlRequest {
	req := &SqlRequest{}
	req.Columns = NewColumns()
	req.Where = make(map[string]string)
	return req
}

func (m *SqlRequest) QLType() RequestType {
	return REQUEST_TYPE_SQL
}

func (m *SqlRequest) AddWhere(t *Token) {
	m.Where[t.V] = t.V
}

// func (m *SqlRequest) getTableName() string {
// 	return m.FromTable
// }

// Array of Columns
type Columns struct {
	Cols []*Column
}

func NewColumns() *Columns {
	return &Columns{Cols: make([]*Column, 0)}
}

func (m *Columns) AddColumn(col string) {

	m.Cols = append(m.Cols, &Column{As: col})
	u.Infof("add col: %s ct=%d", col, len(m.Cols))
}

// Column
type Column struct {
	As     string
	Source ExprOrValue
}

// Expressions or Values
type ExprOrValue struct {
	Val  string
	Args []ExprOrValue
}
