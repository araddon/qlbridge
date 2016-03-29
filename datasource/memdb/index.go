package memdb

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/hashicorp/go-memdb"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY
	// Indexes
	_ memdb.Indexer = (*indexWrapper)(nil)
)

func makeId(dv driver.Value) uint64 {
	switch vt := dv.(type) {
	case int:
		return uint64(vt)
	case int64:
		return uint64(vt)
	case []byte:
		return siphash.Hash(456729, 1111581582, vt)
	case string:
		return siphash.Hash(456729, 1111581582, []byte(vt))
		//by := append(make([]byte,0,8), byte(r), byte(r>>8), byte(r>>16), byte(r>>24), byte(r>>32), byte(r>>40), byte(r>>48), byte(r>>56))
	case datasource.KeyCol:
		return makeId(vt.Val)
	case nil:
		return 0
	default:
		u.LogTracef(u.WARN, "no id conversion for type")
		u.Warnf("not implemented conversion: %T", dv)
	}
	return 0
}

// Wrap the index so we can operate on rows
type indexWrapper struct {
	t *schema.Table
	*schema.Index
}

func (s *indexWrapper) FromObject(obj interface{}) (bool, []byte, error) {

	//u.Debugf("from value? %v", obj)
	switch row := obj.(type) {
	case *datasource.SqlDriverMessage:
		if len(row.Vals) < 0 {
			return false, nil, u.LogErrorf("No values in row?")
		}
		//u.Debugf("nice")
		// Add the null character as a terminator
		val := fmt.Sprintf("%v", row.Vals[0])
		val += "\x00"
		return true, []byte(val), nil
	default:
		return false, nil, u.LogErrorf("Unrecognized type %T", obj)
	}
}

func (s *indexWrapper) FromArgs(args ...interface{}) ([]byte, error) {
	//u.Debugf("not really well implimented %v", args)
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg := fmt.Sprintf("%v", args[0])
	// Add the null character as a terminator
	arg += "\x00"
	return []byte(arg), nil
}

// func (s *indexWrapper) PrefixFromArgs(args ...interface{}) ([]byte, error) {
// 	val, err := s.FromArgs(args...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Strip the null terminator, the rest is a prefix
// 	n := len(val)
// 	if n > 0 {
// 		return val[:n-1], nil
// 	}
// 	return val, nil
// }

func makeMemDbSchema(m *MemDb) (*memdb.DBSchema, error) {

	sindexes := make(map[string]*memdb.IndexSchema)

	for _, idx := range m.indexes {
		sidx := &memdb.IndexSchema{
			Name:    idx.Name,
			Indexer: &indexWrapper{Index: idx},
		}
		if idx.PrimaryKey {
			sidx.Unique = true
		}
		u.Debugf("creating index %q %#v", idx.Name, idx)
		sindexes[idx.Name] = sidx
	}
	/*
		{
			"id": &memdb.IndexSchema{
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.StringFieldIndex{Field: "ID"},
			},
			"foo": &memdb.IndexSchema{
				Name:    "foo",
				Indexer: &memdb.StringFieldIndex{Field: "Foo"},
			},
		},
	*/
	s := memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			m.tbl.Name: &memdb.TableSchema{
				Name:    m.tbl.Name,
				Indexes: sindexes,
			},
		},
	}
	return &s, nil
}
