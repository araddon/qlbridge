package datasource

import (
	"database/sql/driver"
)

// Key interface is the Unique Key identifying a row
type Key interface {
	Key() driver.Value
}

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
