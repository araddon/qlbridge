package schema

import (
	"database/sql/driver"
)

type (
	// represents a message, the Id() method provides a consistent uint64 which
	// can be used by consistent-hash algorithms for topologies that split messages
	// up amongst multiple machines
	//
	// Body()  returns interface allowing this to be generic structure for routing
	//
	// see  "https://github.com/mdmarek/topo" AND http://github.com/lytics/grid
	//
	Message interface {
		Id() uint64
		Body() interface{}
	}

	// Key interface is the Unique Key identifying a row
	Key interface {
		Key() driver.Value
	}
)
