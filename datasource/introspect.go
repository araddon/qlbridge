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
				if exists {
					//u.Warnf("skipping because exists %s.%s", tbl.Name, k)
					// The flaw here is we only look at one value per field(k)
					// We really should do deeper inspection at more than one value.
					continue
				}

				//u.Debugf("i:%v k:%s  v: %T %v", i, k, v, v)
				switch val := v.(type) {
				case int, int64, int16, int32, uint16, uint64, uint32:
					tbl.AddField(schema.NewFieldBase(k, value.IntType, 64, ""))
				case time.Time, *time.Time:
					tbl.AddField(schema.NewFieldBase(k, value.TimeType, 64, ""))
				case bool:
					tbl.AddField(schema.NewFieldBase(k, value.BoolType, 1, ""))
				case float32, float64:
					tbl.AddField(schema.NewFieldBase(k, value.NumberType, 64, ""))
				case string:
					valType := value.ValueTypeFromStringAll(val)
					switch valType {
					case value.NumberType, value.IntType, value.TimeType:
						tbl.AddField(schema.NewFieldBase(k, valType, 64, ""))
					case value.BoolType:
						tbl.AddField(schema.NewFieldBase(k, valType, 1, ""))
					case value.StringType:
						tbl.AddField(schema.NewFieldBase(k, valType, 255, ""))
					default:
						tbl.AddField(schema.NewFieldBase(k, valType, 2000, ""))
					}
				case map[string]interface{}:
					tbl.AddField(schema.NewFieldBase(k, value.JsonType, 2000, ""))
				case []interface{}:
					tbl.AddField(schema.NewFieldBase(k, value.JsonType, 2000, ""))
				default:
					u.Debugf("not implemented: %T", val)
					tbl.AddField(schema.NewFieldBase(k, value.JsonType, 2000, ""))
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
				if exists {
					//u.Warnf("skipping because exists %s.%s", tbl.Name, k)
					// The flaw here is we only look at one value per field(k)
					// We really should do deeper inspection at more than one value.
					continue
				}

				//u.Debugf("%p %s i:%v k:%s  v: %T %v", tbl, tbl.Name, i, k, v, v)
				switch val := v.(type) {
				case int, int64, int16, int32, uint16, uint64, uint32:
					tbl.AddField(schema.NewFieldBase(k, value.IntType, 64, ""))
				case time.Time, *time.Time:
					tbl.AddField(schema.NewFieldBase(k, value.TimeType, 64, ""))
				case bool:
					tbl.AddField(schema.NewFieldBase(k, value.BoolType, 1, ""))
				case float32, float64, json.Number:
					tbl.AddField(schema.NewFieldBase(k, value.NumberType, 64, ""))
				case string:
					valType := value.ValueTypeFromStringAll(val)
					switch valType {
					case value.NumberType, value.IntType, value.TimeType:
						tbl.AddField(schema.NewFieldBase(k, valType, 64, ""))
					case value.BoolType:
						tbl.AddField(schema.NewFieldBase(k, valType, 1, ""))
					case value.StringType:
						tbl.AddField(schema.NewFieldBase(k, valType, 255, ""))
					default:
						tbl.AddField(schema.NewFieldBase(k, valType, 2000, ""))
					}
				case map[string]interface{}:
					tbl.AddField(schema.NewFieldBase(k, value.JsonType, 2000, ""))
				case []interface{}:
					tbl.AddField(schema.NewFieldBase(k, value.JsonType, 2000, ""))
				default:
					tbl.AddField(schema.NewFieldBase(k, value.JsonType, 2000, ""))
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
