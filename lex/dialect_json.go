package lex

var jsonDialectStatement = []*Clause{
	{Token: TokenNil, Lexer: LexJson},
}

// JsonDialect, is a json lexer
//
//    ["hello","world"]
//    {"name":"bob","apples":["honeycrisp","fuji"]}
//
var JsonDialect *Dialect = &Dialect{
	Statements: []*Clause{
		&Clause{Token: TokenNil, Clauses: jsonDialectStatement},
	},
}
