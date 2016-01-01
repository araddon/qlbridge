package schema

type (
	// This message interface is duplicated in datasource for now
	Message interface {
		Id() uint64
		Body() interface{}
	}
)
