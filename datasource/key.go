package datasource

import (
	"database/sql/driver"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ schema.Key = (*KeyInt)(nil)
	_ schema.Key = (*KeyInt64)(nil)
	_ schema.Key = (*KeyCol)(nil)
)

// Variety of Key Types
type (
	KeyInt struct {
		Id int
	}
	KeyInt64 struct {
		Id int64
	}
	KeyCol struct {
		Name string
		Val  driver.Value
	}
)

func NewKeyInt(key int) KeyInt      { return KeyInt{key} }
func (m *KeyInt) Key() driver.Value { return driver.Value(m.Id) }

//func (m KeyInt) Less(than Item) bool { return m.Id < than.(KeyInt).Id }

func NewKeyInt64(key int64) KeyInt64  { return KeyInt64{key} }
func (m *KeyInt64) Key() driver.Value { return driver.Value(m.Id) }

func NewKeyCol(name string, val driver.Value) KeyCol { return KeyCol{name, val} }
func (m KeyCol) Key() driver.Value                   { return m.Val }

// Given a Where expression, lets try to create a key which
//  requires form    `idenity = "value"`
//
func KeyFromWhere(wh interface{}) schema.Key {
	switch n := wh.(type) {
	case *rel.SqlWhere:
		return KeyFromWhere(n.Expr)
	case *expr.BinaryNode:
		if len(n.Args) != 2 {
			u.Warnf("need more args? %#v", n.Args)
			return nil
		}
		in, ok := n.Args[0].(*expr.IdentityNode)
		if !ok {
			u.Warnf("not identity? %T", n.Args[0])
			return nil
		}
		// This only allows for    identity = value
		// NOT:      identity = expr(identity, arg)
		//
		switch valT := n.Args[1].(type) {
		case *expr.NumberNode:
			return NewKeyCol(in.Text, valT.Float64)
		case *expr.StringNode:
			return NewKeyCol(in.Text, valT.Text)
		//case *expr.FuncNode:
		default:
			u.Warnf("not supported arg? %#v", valT)
		}
	default:
		u.Warnf("not supported node type? %#v", n)
	}
	return nil
}
