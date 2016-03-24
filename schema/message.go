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
	// simple iterator interface for paging through a datastore Messages/rows
	// - used for scanning
	// - for datasources that implement exec.Visitor() (ie, select) this
	//    represents the alreader filtered, calculated rows
	Iterator interface {
		Next() Message
	}
	// Key interface is the Unique Key identifying a row
	Key interface {
		Key() driver.Value
	}
)
