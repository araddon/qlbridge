package lex

import (
	"fmt"
	u "github.com/araddon/gou"
	"strings"
)

var _ = u.EMPTY

// Tokens ---------------------------------------------------------------------

// TokenType identifies the type of lexical tokens.
type TokenType uint16

type TokenInfo struct {
	T           TokenType
	Kw          string
	firstWord   string // in event multi-word (Group By) the first word for match
	HasSpaces   bool
	Description string
}

// token represents a text string returned from the lexer.
type Token struct {
	T   TokenType // type
	V   string    // value
	Pos int       // original byte value location
}

// convert to human readable string
func (t Token) String() string {
	return fmt.Sprintf(`Token{Type:"%v" Value:"%v"}`, t.T.String(), t.V)
}

/*

	// List of datatypes from MySql, implement them as tokens?   or leave as Identity during
	// DDL create/alter statements?
	BOOL	TINYINT
	BOOLEAN	TINYINT
	CHARACTER VARYING(M)	VARCHAR(M)
	FIXED	DECIMAL
	FLOAT4	FLOAT
	FLOAT8	DOUBLE
	INT1	TINYINT
	INT2	SMALLINT
	INT3	MEDIUMINT
	INT4	INT
	INT8	BIGINT
	LONG VARBINARY	MEDIUMBLOB
	LONG VARCHAR	MEDIUMTEXT
	LONG	MEDIUMTEXT
	MIDDLEINT	MEDIUMINT
	NUMERIC	DECIMAL

*/
const (
	// List of all TokenTypes
	TokenNil   TokenType = iota // not used
	TokenEOF                    // EOF
	TokenEOS                    // ;
	TokenError                  // error occurred; value is text of error
	TokenRaw                    // raw unlexed text string
	TokenComma                  // ,
	TokenStar                   // *

	// Comments
	TokenComment           // Comment value string
	TokenCommentML         // Comment MultiValue
	TokenCommentStart      // /*
	TokenCommentEnd        // */
	TokenCommentSlashes    // Single Line comment:   // hello
	TokenCommentSingleLine // Single Line comment:   -- hello
	TokenCommentHash       // Single Line comment:  # hello

	// Primitive literal data-types
	TokenBool
	TokenFloat
	TokenInteger
	TokenString
	TokenList
	TokenMap
	// Data Types
	TokenText
	TokenVarChar
	TokenBigInt

	// Logical Evaluation/expression inputs and operations
	TokenMinus            // -
	TokenPlus             // +
	TokenPlusPlus         // ++
	TokenPlusEquals       // +=
	TokenDivide           // /
	TokenMultiply         // *
	TokenModulus          // %
	TokenEqual            // =
	TokenEqualEqual       // ==
	TokenNE               // !=
	TokenGE               // >=
	TokenLE               // <=
	TokenGT               // >
	TokenLT               // <
	TokenIf               // IF
	TokenOr               // ||
	TokenAnd              // &&
	TokenBetween          // between
	TokenLogicOr          // OR
	TokenLogicAnd         // AND
	TokenIN               // IN
	TokenLike             // LIKE
	TokenNegate           // NOT
	TokenLeftParenthesis  // (
	TokenRightParenthesis // )
	TokenTrue             // True
	TokenFalse            // False

	// ql top-level keywords, these first keywords determine parser
	TokenInsert
	TokenUpdate
	TokenDelete
	TokenSelect
	TokenUpsert
	TokenAlter
	TokenCreate
	TokenSubscribe
	TokenFilter
	TokenDescribe
	TokenShow

	// Other QL Keywords, These are clause-level keywords that mark seperation between clauses
	TokenTable     // table
	TokenFrom      // from
	TokenWhere     // where
	TokenHaving    // having
	TokenGroupBy   // group by
	TokenBy        // by
	TokenAlias     // alias
	TokenWith      // with
	TokenValues    // values
	TokenInto      // into
	TokenLimit     // limit
	TokenOrderBy   // order by
	TokenInnerJoin // inner join
	TokenOuterJoin // outer join
	TokenLeftJoin  // left join
	TokenJoin      // Join
	TokenOn        // on
	TokenDistinct  // DISTINCT
	TokenAll       // all

	// ddl
	TokenChange       // change
	TokenAdd          // add
	TokenFirst        // first
	TokenAfter        // after
	TokenCharacterSet // character set

	// Other QL keywords
	TokenSet  // set
	TokenAs   // as
	TokenAsc  // ascending
	TokenDesc // descending

	// User defined function/expression
	TokenUdfExpr

	// Value Types
	TokenIdentity             // identity, either column, table name etc
	TokenValue                // 'some string' string or continous sequence of chars delimited by WHITE SPACE | ' | , | ( | )
	TokenValueWithSingleQuote // '' becomes ' inside the string, parser will need to replace the string
	TokenRegex                // regex
	TokenDuration             // 14d , 22w, 3y, 45ms, 45us, 24hr, 2h, 45m, 30s
	//TokenKey                  // key
	//TokenTag                  // tag
)

var (
	// Which Identity Characters are allowed?
	//    if we allow forward slashes then we can allow xpath esque notation
	// IDENTITY_CHARS = "_."
	IDENTITY_CHARS = "_./"
	// A much more lax identity char set rule
	IDENTITY_LAX_CHARS = "_./ "
	// sql variables start with @@ ??
	IDENTITY_SQL_CHARS = "@_./"

	// list of token-name
	TokenNameMap = map[TokenType]*TokenInfo{

		TokenEOF:   {Description: "EOF"},
		TokenEOS:   {Description: ";"},
		TokenError: {Description: "Error"},
		TokenRaw:   {Description: "unlexed text"},
		TokenComma: {Description: ","},

		// Comments
		TokenComment:           {Description: "Comment"},
		TokenCommentML:         {Description: "CommentMultiLine"},
		TokenCommentStart:      {Description: "/*"},
		TokenCommentEnd:        {Description: "*/"},
		TokenCommentHash:       {Description: "#"},
		TokenCommentSingleLine: {Description: "--"},
		TokenCommentSlashes:    {Description: "//"},

		// Primitive literals.
		TokenBool:    {Description: "Bool"},
		TokenFloat:   {Description: "Float"},
		TokenInteger: {Description: "Integer"},
		TokenString:  {Description: "String"},
		TokenList:    {Description: "List"},
		TokenMap:     {Description: "Map"},

		// Some other data Types
		TokenText:    {Description: "Text"},
		TokenVarChar: {Description: "varchar"},
		TokenBigInt:  {Description: "bigint"},

		// Logic, Expressions, Operators etc
		TokenStar:       {Kw: "*", Description: "*"},
		TokenMultiply:   {Kw: "*", Description: "Multiply"},
		TokenMinus:      {Kw: "-", Description: "-"},
		TokenPlus:       {Kw: "+", Description: "+"},
		TokenPlusPlus:   {Kw: "++", Description: "++"},
		TokenPlusEquals: {Kw: "+=", Description: "+="},
		TokenDivide:     {Kw: "/", Description: "Divide /"},
		TokenModulus:    {Kw: "%", Description: "Modulus %"},
		TokenEqual:      {Kw: "=", Description: "Equal"},
		TokenEqualEqual: {Kw: "==", Description: "=="},
		TokenNE:         {Kw: "!=", Description: "NE"},
		TokenGE:         {Kw: ">=", Description: "GE"},
		TokenLE:         {Kw: "<=", Description: "LE"},
		TokenGT:         {Kw: ">", Description: "GT"},
		TokenLT:         {Kw: "<", Description: "LT"},
		TokenIf:         {Kw: "if", Description: "IF"},
		TokenAnd:        {Kw: "&&", Description: "&&"},
		TokenOr:         {Kw: "||", Description: "||"},
		TokenLogicOr:    {Kw: "or", Description: "Or"},
		TokenLogicAnd:   {Kw: "and", Description: "And"},
		TokenIN:         {Kw: "in", Description: "IN"},
		TokenLike:       {Kw: "like", Description: "LIKE"},
		TokenNegate:     {Kw: "not", Description: "NOT"},
		TokenBetween:    {Kw: "between", Description: "between"},

		// Identity ish bools
		TokenTrue:  {Kw: "true", Description: "True"},
		TokenFalse: {Kw: "false", Description: "False"},

		// parens, both logical expression as well as functional
		TokenLeftParenthesis:  {Description: "("},
		TokenRightParenthesis: {Description: ")"},

		// Expression Identifier
		TokenUdfExpr: {Description: "expr"},

		// Initial Keywords, these are the most important QL Type words
		TokenInsert:    {Description: "insert"},
		TokenSelect:    {Description: "select"},
		TokenDelete:    {Description: "delete"},
		TokenUpdate:    {Description: "update"},
		TokenUpsert:    {Description: "upsert"},
		TokenAlter:     {Description: "alter"},
		TokenCreate:    {Description: "create"},
		TokenSubscribe: {Description: "subscribe"},
		TokenFilter:    {Description: "filter"},
		TokenDescribe:  {Description: "describe"},
		TokenShow:      {Description: "show"},

		// value types
		TokenIdentity:             {Description: "identity"},
		TokenValue:                {Description: "value"},
		TokenValueWithSingleQuote: {Description: "valueWithSingleQuote"},
		TokenRegex:                {Description: "regex"},
		TokenDuration:             {Description: "duration"},

		// Top Level ql clause keywords
		TokenTable:     {Description: "table"},
		TokenInto:      {Description: "into"},
		TokenBy:        {Description: "by"},
		TokenFrom:      {Description: "from"},
		TokenWhere:     {Description: "where"},
		TokenHaving:    {Description: "having"},
		TokenGroupBy:   {Description: "group by"},
		TokenAlias:     {Description: "alias"},
		TokenWith:      {Description: "with"},
		TokenValues:    {Description: "values"},
		TokenLimit:     {Description: "limit"},
		TokenOrderBy:   {Description: "order by"},
		TokenInnerJoin: {Description: "inner join"},
		TokenOuterJoin: {Description: "outer join"},
		TokenLeftJoin:  {Description: "left join"},
		TokenJoin:      {Description: "join"},
		TokenOn:        {Description: "on"},
		TokenDistinct:  {Description: "distinct"},
		TokenAll:       {Description: "all"},

		// ddl keywords
		TokenChange:       {Description: "change"},
		TokenCharacterSet: {Description: "character set"},
		TokenAdd:          {Description: "add"},
		TokenFirst:        {Description: "first"},
		TokenAfter:        {Description: "after"},

		// QL Keywords, all lower-case
		TokenSet:  {Description: "set"},
		TokenAs:   {Description: "as"},
		TokenAsc:  {Description: "asc"},
		TokenDesc: {Description: "desc"},
	}
)

func init() {
	LoadTokenInfo()
}

func LoadTokenInfo() {
	for tok, ti := range TokenNameMap {
		ti.T = tok
		if ti.Kw == "" {
			ti.Kw = ti.Description
		}
		if strings.Contains(ti.Kw, " ") {
			parts := strings.Split(ti.Kw, " ")
			ti.firstWord = parts[0]
			ti.HasSpaces = true
		}
	}

	SqlDialect.Init()
}

// convert to human readable string
func (typ TokenType) String() string {
	s, ok := TokenNameMap[typ]
	if ok {
		return s.Kw
	}
	return "not implemented"
}

// which keyword should we look for, either full keyword
// OR in case of spaces such as "group by" look for group
func (typ TokenType) MatchString() string {
	tokInfo, ok := TokenNameMap[typ]
	//u.Debugf("matchstring: '%v' '%v'  '%v'", tokInfo.T, tokInfo.Kw, tokInfo.Description)
	if ok {
		if tokInfo.HasSpaces {
			return tokInfo.firstWord
		}
		return tokInfo.Kw
	}
	return "not implemented"
}

// is this a word such as "Group by" with multiple words?
func (typ TokenType) MultiWord() bool {
	tokInfo, ok := TokenNameMap[typ]
	if ok {
		return tokInfo.HasSpaces
	}
	return false
}
