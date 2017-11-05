package lex

var (
	expressionStatement = []*Clause{
		{Token: TokenIdentity, Lexer: LexExpressionOrIdentity},
	}

	// ExpressionDialect, is a Single Expression dialect, useful for parsing Single
	// function
	//
	//    eq(tolower(item_name),"buy")
	ExpressionDialect *Dialect = &Dialect{
		Statements: []*Clause{
			{Token: TokenNil, Clauses: expressionStatement},
		},
	}

	logicalEpressions = []*Clause{
		{Token: TokenNil, Lexer: LexLogical},
	}

	// logical Expression Statement of the following functional format
	//
	//   5 > 4   => true
	//   4 + 5   => 9
	//   tolower(item) + 12 > 4
	//   4 IN (4,5,6)
	//
	LogicalExpressionDialect *Dialect = &Dialect{
		Statements: []*Clause{
			{Token: TokenNil, Clauses: logicalEpressions},
		},
	}
)

// NewExpressionLexer creates a new lexer for the input string using Expression Dialect.
func NewExpressionLexer(input string) *Lexer {
	return NewLexer(input, ExpressionDialect)
}
