package qlparser

import ()

// Single Expression Statement
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
