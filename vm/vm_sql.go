package vm

import (
	"fmt"
	"reflect"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/ast"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

var (
	SqlEvalError = fmt.Errorf("Could not evaluate sql statement")
)

// SqlVm vm is a vm for parsing, evaluating a
//
type SqlVm struct {
	Statement ast.SqlStatement
	Keyword   lex.TokenType
	sel       *ast.SqlSelect
	ins       *ast.SqlInsert
	del       *ast.SqlDelete
}

// SqlVm parsers a sql query into columns, where guards, etc
//
func NewSqlVm(sqlText string) (*SqlVm, error) {

	stmt, err := ast.ParseSqlVm(sqlText)
	if err != nil {
		return nil, err
	}
	m := &SqlVm{
		Statement: stmt,
	}
	switch v := stmt.(type) {
	case *ast.SqlSelect:
		m.Keyword = lex.TokenSelect
		m.sel = v
	case *ast.SqlInsert:
		m.Keyword = lex.TokenInsert
		m.ins = v
	case *ast.SqlDelete:
		m.Keyword = lex.TokenDelete
		m.del = v
	}
	return m, nil
}

// Execute applies a parse expression to the specified context's
//
//     writeContext in the case of sql query is similar to a recordset for selects,
//       or for delete, insert, update it is like the storage layer
//
func (m *SqlVm) Execute(writeContext ContextWriter, readContext ContextReader) (err error) {

	switch m.Keyword {
	case lex.TokenSelect:
		return m.ExecuteSelect(writeContext, readContext)
	case lex.TokenInsert:
		if rowWriter, ok := writeContext.(RowWriter); ok {
			return m.ExecuteInsert(rowWriter)
		} else {
			return fmt.Errorf("Must implement RowWriter: %T", writeContext)
		}
	case lex.TokenDelete:
		return m.ExecuteDelete(writeContext, readContext)
	default:
		u.Warnf("not implemented: %v", m.Keyword)
		return fmt.Errorf("not implemented %v", m.Keyword)
	}
	return nil
}

// Execute applies a dml sql select expression to the specified context's
//
//     writeContext in the case of sql query is similar to a recordset for selects,
//       or for delete, insert, update it is like the storage layer
//
func (m *SqlVm) ExecuteSelect(writeContext ContextWriter, readContext ContextReader) (err error) {
	//defer errRecover(&err)
	s := &State{
		ExprVm:        m,
		ContextReader: readContext,
	}
	s.rv = reflect.ValueOf(s)

	// Check and see if we are where Guarded
	if m.sel.Where != nil {
		//u.Debugf("Has a Where:  %v", m.Request.Where.Root.StringAST())
		whereValue, ok := s.Walk(m.sel.Where.Root)
		if !ok {
			return SqlEvalError
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal == value.BoolValueFalse {
				u.Debugf("Filtering out")
				return nil
			}
		}
		//u.Debugf("Matched where: %v", whereValue)
	}
	for _, col := range m.sel.Columns {
		if col.Guard != nil {
			// TODO:  evaluate if guard
		}
		if col.Star {
			for k, v := range readContext.Row() {
				writeContext.Put(&ast.Column{As: k}, nil, v)
			}
		} else {
			//u.Debugf("tree.Root: as?%v %#v", col.As, col.Tree.Root)
			v, ok := s.Walk(col.Tree.Root)
			if ok {
				writeContext.Put(col, readContext, v)
			}
		}

	}

	//writeContext.Put()
	return
}

func (m *SqlVm) ExecuteInsert(writeContext RowWriter) (err error) {

	for _, row := range m.ins.Rows {

		for i, col := range m.ins.Columns {

			//u.Debugf("tree.Root: i, as, val:  %v %v %v", i, col.As, row[i])
			if col.Tree != nil && col.Tree.Root != nil {
				u.Warnf("Not implemented")
			}
			//v, ok := s.Walk(col.Tree.Root)
			writeContext.Put(col, nil, row[i])
		}
		writeContext.Commit(nil, writeContext)

	}
	return
}

func (m *SqlVm) ExecuteDelete(writeContext ContextWriter, readContext ContextReader) (err error) {
	//defer errRecover(&err)
	scanner, ok := readContext.(RowScanner)
	if !ok {
		return fmt.Errorf("Must implement RowScanner: %T", writeContext)
	}
	s := &State{
		ExprVm:        m,
		ContextReader: readContext,
	}
	s.rv = reflect.ValueOf(s)

	// Check and see if we are where Guarded
	if m.del.Where != nil {
		u.Debugf("Has a Where:  %v", m.del.Where.Root.StringAST())

		for row := scanner.Next(); ; row = scanner.Next() {
			if row == nil {
				break
			}
			whereValue, ok := s.Walk(m.del.Where.Root)
			u.Infof("where: %v %v", ok, whereValue)
			if !ok {
				continue
			}
			switch whereVal := whereValue.(type) {
			case value.BoolValue:
				if whereVal == value.BoolValueTrue {
					if err := writeContext.Delete(row); err != nil {
						u.Errorf("error %v", err)
					}
				}
			}
		}

	}
	// //u.Debugf("tree.Root: as?%v %#v", col.As, col.Tree.Root)
	// v, ok := s.Walk(col.Tree.Root)
	// if ok {
	// 	writeContext.Put(col, readContext, v)
	// }

	return
}
