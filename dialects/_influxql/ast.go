package influxql

type Ast struct {
	Comments string `json:",omitempty"` // Any comments
	Select   *SelectStmt
}

type SelectStmt struct {
	Columns []*Column // identify inputs and outputs, e.g. "SELECT fname, count(*) FROM ..."
	From    *From     // metric can be regex
	GroupBy []string  // group by
	Alias   string    // Unique identifier of this query for start/stop/mgmt purposes
	//Where     []*Expr          `json:",omitempty"` // Filtering conditions, "SELECT ... WHERE x>y ... "
}
type From struct {
	Value string
	Regex bool
}
type Column struct {
	Name string
}
