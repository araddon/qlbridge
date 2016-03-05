package datasource

import (
	"strconv"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	_               = u.EMPTY
	IntrospectCount = 20
)

func IntrospectSchema(s *schema.Schema, name string, iter schema.Iterator) error {

	tbl, err := s.Table(name)
	if err != nil {
		u.Errorf("Could not find table %q", name)
		return err
	}
	nameIndex := make(map[int]string, len(tbl.Columns()))
	for i, colName := range tbl.Columns() {
		nameIndex[i] = colName
	}
	//u.Infof("name index: %v", nameIndex)
	ct := 0
	for {
		msg := iter.Next()
		//u.Debugf("msg %#v", msg)
		if msg == nil || ct > IntrospectCount {
			break
		}
		switch mt := msg.Body().(type) {
		case *SqlDriverMessageMap:
			for i, v := range mt.Vals {

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
					valType := guessValueType(val)
					if !exists {
						tbl.AddFieldType(k, valType)
						//fld := tbl.FieldMap[k]
						//u.Debugf("add field? %+v", fld)
						//u.Debugf("%s = %v   type: %T   vt:%s new? %v", k, val, val, valType, !exists)
					}
				default:
					u.Warnf("not implemented: %T", val)
				}
			}
		default:
			u.Warnf("not implemented: %T", mt)
		}

		ct++
	}
	return nil
}

func guessValueType(val string) value.ValueType {
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return value.IntType
	} else if _, err := strconv.ParseBool(val); err == nil {
		return value.IntType
	} else if _, err := strconv.ParseFloat(val, 64); err == nil {
		return value.NumberType
	} else if _, err := dateparse.ParseAny(val); err == nil {
		return value.TimeType
	}
	return value.StringType
}
