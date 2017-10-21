package datasource

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// IntrospectCount is default number of rows to evaluate for introspection
	// based schema discovery.
	IntrospectCount = 20
)

// IntrospectSchema discover schema from contents of row introspection.
func IntrospectSchema(s *schema.Schema, name string, iter schema.Iterator) error {
	tbl, err := s.Table(name)
	if err != nil {
		u.Errorf("Could not find table %q", name)
		return err
	}
	return IntrospectTable(tbl, iter)
}

// IntrospectTable accepts a table and schema Iterator and will
// read a representative sample of rows, introspecting the results
// to create a schema.  Generally used for CSV, Json files to
// create strongly typed schemas.
func IntrospectTable(tbl *schema.Table, iter schema.Iterator) error {

	needsCols := len(tbl.Columns()) == 0
	nameIndex := make(map[int]string, len(tbl.Columns()))
	for i, colName := range tbl.Columns() {
		nameIndex[i] = colName
	}
	//u.Infof("s:%s INTROSPECT SCHEMA name %q", s, name)
	ct := 0
	for {
		msg := iter.Next()
		//u.Debugf("msg %#v", msg)
		if msg == nil || ct > IntrospectCount {
			break
		}
		switch mt := msg.Body().(type) {
		case []driver.Value:
			for i, v := range mt {

				k := nameIndex[i]
				_, exists := tbl.FieldMap[k]

				//u.Debugf("i:%v k:%s  v: %T %v", i, k, v, v)
				switch val := v.(type) {
				case int, int64, int16, int32, uint16, uint64, uint32:
					tbl.AddFieldType(k, value.IntType)
				case time.Time, *time.Time:
					tbl.AddFieldType(k, value.TimeType)
				case bool:
					tbl.AddFieldType(k, value.BoolType)
				case float32, float64:
					tbl.AddFieldType(k, value.NumberType)
				case string:
					valType := value.ValueTypeFromStringAll(val)
					if !exists {
						tbl.AddFieldType(k, valType)
						//fld := tbl.FieldMap[k]
						//u.Debugf("add field? %+v", fld)
						//u.Debugf("%s = %v   type: %T   vt:%s new? %v", k, val, val, valType, !exists)
					}
				case map[string]interface{}:
					tbl.AddFieldType(k, value.JsonType)
				default:
					u.Debugf("not implemented: %T", val)
				}
			}
		case *SqlDriverMessageMap:
			if needsCols {
				nameIndex = make(map[int]string, len(mt.ColIndex))
				for k2, ki := range mt.ColIndex {
					nameIndex[ki] = k2
				}
			}
			for i, v := range mt.Vals {

				k := nameIndex[i]
				// if k == "" {
				// 	for k2, ki := range mt.ColIndex {
				// 		if ki == i {
				// 			k = k2
				// 			break
				// 		}
				// 	}
				// }

				_, exists := tbl.FieldMap[k]

				//u.Debugf("i:%v k:%s  v: %T %v", i, k, v, v)
				switch val := v.(type) {
				case int, int64, int16, int32, uint16, uint64, uint32:
					tbl.AddFieldType(k, value.IntType)
				case time.Time, *time.Time:
					tbl.AddFieldType(k, value.TimeType)
				case bool:
					tbl.AddFieldType(k, value.BoolType)
				case float32, float64, json.Number:
					tbl.AddFieldType(k, value.NumberType)
				case string:
					valType := value.ValueTypeFromStringAll(val)
					if !exists {
						tbl.AddFieldType(k, valType)
						//fld := tbl.FieldMap[k]
						//u.Debugf("add field? %+v", fld)
						//u.Debugf("%s = %v   type: %T   vt:%s new? %v", k, val, val, valType, !exists)
					}
				case map[string]interface{}:
					tbl.AddFieldType(k, value.JsonType)
				case []interface{}:
					tbl.AddFieldType(k, value.JsonType)
				case nil:
					// hm.....
					tbl.AddFieldType(k, value.JsonType)
				default:
					tbl.AddFieldType(k, value.JsonType)
					u.LogThrottle(u.WARN, 10, "not implemented: k:%v  %T", k, val)
				}
			}
		default:
			u.Warnf("not implemented: %T", mt)
		}

		ct++
	}
	if needsCols {
		cols := make([]string, len(tbl.Fields))
		for i, f := range tbl.Fields {
			//u.Debugf("%+v", f)
			cols[i] = f.Name
		}
		tbl.SetColumns(cols)
	}

	//u.Debugf("%s: %v", tbl.Name, tbl.Columns())
	return nil
}
