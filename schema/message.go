package schema

import (
	"database/sql/driver"
)

type (
	// Message is an interface to describe a Row being processed by query engine/vm
	// or it is a message between distributed parts of the system.  It provides a
	// Id() method which can be used by consistent-hash algorithms for routing a message
	// consistently to different processes/servers.
	//
	// Body() returns interface allowing this to be generic structure for routing
	//
	// see  http://github.com/lytics/grid
	//
	Message interface {
		Id() uint64
		Body() interface{}
	}
	// MessageValues describes a message with array of driver.Value.
	MessageValues interface {
		Values() []driver.Value
	}
	// Iterator is simple iterator for paging through a datastore Message(rows)
	// to be used for scanning.  Building block for Tasks that process part of
	// a DAG of tasks to process data.
	Iterator interface {
		Next() Message
	}
	// Key interface is the Unique Key identifying a row.
	Key interface {
		Key() driver.Value
	}
)

// KeyUint implements Key interface and is simple uint64 key
type KeyUint struct {
	ID uint64
}

// NewKeyUint simple new uint64 key
func NewKeyUint(key uint64) *KeyUint {
	return &KeyUint{key}
}

// Key is key interface
func (m *KeyUint) Key() driver.Value {
	return driver.Value(m.ID)
}
