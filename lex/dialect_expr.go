package lex

import ()

// Single Expression Statement of the following functional format
//   eq(tolower(item_name),"buy")
//
var ExpressionStatement = []*Clause{
	{Token: TokenIdentity, Lexer: LexExpressionOrIdentity},
}

// ExpressionDialect, is a Single Expression dialect, useful for parsing Single
// function
var ExpressionDialect *Dialect = &Dialect{
	Statements: []*Statement{
		&Statement{TokenNil, ExpressionStatement},
	},
}

// logical Expression Statement of the following functional format
//
//   5 > 4   => true
//   4 + 5   => 9
//   tolower(item) + 12 > 4
//
var LogicalEpressions = []*Clause{
	{Token: TokenNil, Lexer: LexLogical},
}

var LogicalExpressionDialect *Dialect = &Dialect{
	Statements: []*Statement{
		&Statement{TokenNil, LogicalEpressions},
	},
}
