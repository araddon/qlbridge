package schema

// A datasource is most likely a database, file, api, in-mem data etc
// some data source that can be used to drive statements
type DataSource interface {
	Tables() []string
	Open(source string) (SourceConn, error)
	Close() error
}

// A backend data source provider that also provides schema
type SchemaProvider interface {
	DataSource
	Table(table string) (*Table, error)
}

// DataSource Connection, only one guaranteed feature, although
//  should implement many more (scan, seek, etc)
type SourceConn interface {
	Close() error
}

// Interface for a data source connection exposing column positions for []driver.Value iteration
type SchemaColumns interface {
	Columns() []string
}
