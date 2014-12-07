package lex

import ()

var expressionStatement = []*Clause{
	{Token: TokenIdentity, Lexer: LexExpressionOrIdentity},
}

// ExpressionDialect, is a Single Expression dialect, useful for parsing Single
// function
//
//    eq(tolower(item_name),"buy")
var ExpressionDialect *Dialect = &Dialect{
	Statements: []*Statement{
		&Statement{TokenNil, expressionStatement},
	},
}

var logicalEpressions = []*Clause{
	{Token: TokenNil, Lexer: LexLogical},
}

// logical Expression Statement of the following functional format
//
//   5 > 4   => true
//   4 + 5   => 9
//   tolower(item) + 12 > 4
//
var LogicalExpressionDialect *Dialect = &Dialect{
	Statements: []*Statement{
		&Statement{TokenNil, logicalEpressions},
	},
}
