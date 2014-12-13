package vm

import (
	"reflect"

	u "github.com/araddon/gou"
	//ql "github.com/araddon/qlbridge/lex"
)

// SqlVm vm is a vm for parsing, evaluating a
//
type SqlVm struct {
	Request *SqlRequest
}

// SqlVm parsers a sql query into columns, where guards, etc
//
func NewSqlVm(sqlText string) (*SqlVm, error) {

	sqlRequest, err := ParseSql(sqlText)
	if err != nil {
		return nil, err
	}
	m := &SqlVm{
		Request: sqlRequest,
	}
	return m, nil
}

// Execute applies a parse expression to the specified context's
//
//     writeContext in the case of sql query is similar to a recordset for selects,
//       or for delete, insert, update it is like the storage layer
//
func (m *SqlVm) Execute(writeContext ContextWriter, readContext ContextReader) (err error) {
	//defer errRecover(&err)
	s := &State{
		ExprVm: m,
		read:   readContext,
	}
	s.rv = reflect.ValueOf(s)

	// Check and see if we are where Guarded
	if m.Request.Where != nil {
		//u.Debugf("Has a Where:  %v", m.Request.Where)
		whereValue := s.Walk(m.Request.Where.Root)
		switch whereVal := whereValue.(type) {
		case BoolValue:
			if whereVal == BoolValueFalse {
				u.Debugf("Filtering out")
				return nil
			}
		}
		u.Debugf("Matched where: %v", whereValue)
	}
	for _, col := range m.Request.Columns {
		if col.Guard != nil {
			// TODO:  evaluate if guard
		}

		u.Debugf("tree.Root: as?%v %#v", col.As, col.Tree.Root)
		v := s.Walk(col.Tree.Root)
		writeContext.Put(col.As, v)
	}

	//writeContext.Put()
	return
}
