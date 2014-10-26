package qlparse

type Dialect struct {
	Name       string
	Statements []*Statement
}

type Statement struct {
	Keyword string
	Clauses []*Clause
}

type Clause struct {
	Keyword  string
	Optional bool
}

var sqlSelect *Statement = &Statement{
	Clauses: []*Clause{
		{Keyword: TokenFrom.String()},
	},
}

var SqlDialect *Dialect = &Dialect{
	Statements: []*Statement{sqlSelect},
}
