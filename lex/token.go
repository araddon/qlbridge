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
	T     TokenType // type
	V     string    // value
	Pos   int       // original byte value location
	Quote byte      // quote mark:    " ` [ '
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
	// List of all TokenTypes Note we do NOT use IOTA because it is evil
	//  if we change the position (ie, add a token not at end) it will cause any
	//  usage of tokens serialized on disk/database to be invalid

	// Basic grammar items
	TokenNil      TokenType = 0 // not used
	TokenEOF      TokenType = 1 // EOF
	TokenEOS      TokenType = 2 // ;
	TokenEofOrEos TokenType = 3 // End of file, OR ;
	TokenError    TokenType = 4 // error occurred; value is text of error
	TokenRaw      TokenType = 5 // raw unlexed text string

	// Comments
	TokenComment           TokenType = 10 // Comment value string
	TokenCommentML         TokenType = 11 // Comment MultiValue
	TokenCommentStart      TokenType = 12 // /*
	TokenCommentEnd        TokenType = 13 // */
	TokenCommentSlashes    TokenType = 14 // Single Line comment:   // hello
	TokenCommentSingleLine TokenType = 15 // Single Line comment:   -- hello
	TokenCommentHash       TokenType = 16 // Single Line comment:  # hello

	// Misc
	TokenComma        TokenType = 20 // ,
	TokenStar         TokenType = 21 // *
	TokenColon        TokenType = 22 // :
	TokenLeftBracket  TokenType = 23 // [
	TokenRightBracket TokenType = 24 // ]
	TokenLeftBrace    TokenType = 25 // {
	TokenRightBrace   TokenType = 26 // }

	// Logical Evaluation/expression inputs and operations
	TokenMinus            TokenType = 60 // -
	TokenPlus             TokenType = 61 // +
	TokenPlusPlus         TokenType = 62 // ++
	TokenPlusEquals       TokenType = 63 // +=
	TokenDivide           TokenType = 64 // /
	TokenMultiply         TokenType = 65 // *
	TokenModulus          TokenType = 66 // %
	TokenEqual            TokenType = 67 // =
	TokenEqualEqual       TokenType = 68 // ==
	TokenNE               TokenType = 69 // !=
	TokenGE               TokenType = 70 // >=
	TokenLE               TokenType = 71 // <=
	TokenGT               TokenType = 72 // >
	TokenLT               TokenType = 73 // <
	TokenIf               TokenType = 74 // IF
	TokenOr               TokenType = 75 // ||
	TokenAnd              TokenType = 76 // &&
	TokenBetween          TokenType = 77 // between
	TokenLogicOr          TokenType = 78 // OR
	TokenLogicAnd         TokenType = 79 // AND
	TokenIN               TokenType = 80 // IN
	TokenLike             TokenType = 81 // LIKE
	TokenNegate           TokenType = 82 // NOT
	TokenLeftParenthesis  TokenType = 83 // (
	TokenRightParenthesis TokenType = 84 // )
	TokenTrue             TokenType = 85 // True
	TokenFalse            TokenType = 86 // False
	TokenIs               TokenType = 87 // IS
	TokenNull             TokenType = 88 // NULL

	// ql top-level keywords, these first keywords determine parser
	TokenPrepare   TokenType = 100
	TokenInsert    TokenType = 101
	TokenUpdate    TokenType = 102
	TokenDelete    TokenType = 103
	TokenSelect    TokenType = 104
	TokenUpsert    TokenType = 105
	TokenAlter     TokenType = 106
	TokenCreate    TokenType = 107
	TokenSubscribe TokenType = 108
	TokenFilter    TokenType = 109
	TokenShow      TokenType = 110
	TokenDescribe  TokenType = 111 // We can also use TokenDesc
	TokenExplain   TokenType = 112 // another alias for desccribe

	// Other QL Keywords, These are clause-level keywords that mark seperation between clauses
	TokenTable    TokenType = 120 // table
	TokenFrom     TokenType = 121 // from
	TokenWhere    TokenType = 122 // where
	TokenHaving   TokenType = 123 // having
	TokenGroupBy  TokenType = 124 // group by
	TokenBy       TokenType = 125 // by
	TokenAlias    TokenType = 126 // alias
	TokenWith     TokenType = 127 // with
	TokenValues   TokenType = 128 // values
	TokenInto     TokenType = 129 // into
	TokenLimit    TokenType = 130 // limit
	TokenOrderBy  TokenType = 131 // order by
	TokenInner    TokenType = 132 // inner , ie of join
	TokenCross    TokenType = 133 // cross
	TokenOuter    TokenType = 134 // outer
	TokenLeft     TokenType = 135 // left
	TokenRight    TokenType = 136 // right
	TokenJoin     TokenType = 137 // Join
	TokenOn       TokenType = 140 // on
	TokenDistinct TokenType = 141 // DISTINCT
	TokenAll      TokenType = 142 // all
	TokenInclude  TokenType = 143 // INCLUDE
	TokenExists   TokenType = 144 // EXISTS

	// ddl
	TokenChange       TokenType = 151 // change
	TokenAdd          TokenType = 152 // add
	TokenFirst        TokenType = 153 // first
	TokenAfter        TokenType = 154 // after
	TokenCharacterSet TokenType = 155 // character set

	// Other QL keywords
	TokenSet  TokenType = 170 // set
	TokenAs   TokenType = 171 // as
	TokenAsc  TokenType = 172 // ascending
	TokenDesc TokenType = 173 // descending
	TokenUse  TokenType = 174 // use

	// User defined function/expression
	TokenUdfExpr TokenType = 180

	// Value Types
	TokenIdentity             TokenType = 190 // identity, either column, table name etc
	TokenValue                TokenType = 191 // 'some string' string or continous sequence of chars delimited by WHITE SPACE | ' | , | ( | )
	TokenValueWithSingleQuote TokenType = 192 // '' becomes ' inside the string, parser will need to replace the string
	TokenRegex                TokenType = 193 // regex
	TokenDuration             TokenType = 194 // 14d , 22w, 3y, 45ms, 45us, 24hr, 2h, 45m, 30s

	// Primitive literal data-types
	TokenDataType TokenType = 200 // A generic Identifier of DataTypes
	TokenBool     TokenType = 201
	TokenFloat    TokenType = 202
	TokenInteger  TokenType = 203
	TokenString   TokenType = 204
	TokenVarChar  TokenType = 205
	TokenBigInt   TokenType = 206
	TokenText     TokenType = 207

	// Composite Data Types
	TokenList TokenType = 250
	TokenMap  TokenType = 251
	TokenJson TokenType = 252
)

var (
	// Which Identity Characters are allowed?
	//    if we allow forward slashes then we can allow xpath esque notation
	// IDENTITY_CHARS = "_."
	IDENTITY_CHARS = "_./-"
	// A much more lax identity char set rule
	IDENTITY_LAX_CHARS = "_./- "
	// sql variables start with @@ ??
	IDENTITY_SQL_CHARS = "@_./-"

	// list of token-name
	TokenNameMap = map[TokenType]*TokenInfo{

		TokenEOF:      {Description: "EOF"},
		TokenEOS:      {Description: ";"},
		TokenEofOrEos: {Kw: "", Description: "; OR EOF"},
		TokenError:    {Description: "Error"},
		TokenRaw:      {Description: "unlexed text"},

		// Comments
		TokenComment:           {Description: "Comment"},
		TokenCommentML:         {Description: "CommentMultiLine"},
		TokenCommentStart:      {Description: "/*"},
		TokenCommentEnd:        {Description: "*/"},
		TokenCommentHash:       {Description: "#"},
		TokenCommentSingleLine: {Description: "--"},
		TokenCommentSlashes:    {Description: "//"},

		// Misc
		TokenComma:        {Description: ","},
		TokenStar:         {Kw: "*", Description: "*"},
		TokenColon:        {Kw: ":", Description: ":"},
		TokenLeftBracket:  {Kw: "[", Description: "["},
		TokenRightBracket: {Kw: "]", Description: "]"},
		TokenLeftBrace:    {Kw: "{", Description: "{"},
		TokenRightBrace:   {Kw: "}", Description: "}"},

		// Logic, Expressions, Operators etc
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
		TokenIs:         {Kw: "is", Description: "IS"},
		TokenNull:       {Kw: "null", Description: "NULL"},

		// Identity ish bools
		TokenTrue:  {Kw: "true", Description: "True"},
		TokenFalse: {Kw: "false", Description: "False"},

		// parens, both logical expression as well as functional
		TokenLeftParenthesis:  {Description: "("},
		TokenRightParenthesis: {Description: ")"},

		// Expression Identifier
		TokenUdfExpr: {Description: "expr"},

		// Initial Keywords, these are the most important QL Type words
		TokenPrepare:   {Description: "prepare"},
		TokenInsert:    {Description: "insert"},
		TokenSelect:    {Description: "select"},
		TokenDelete:    {Description: "delete"},
		TokenUpdate:    {Description: "update"},
		TokenUpsert:    {Description: "upsert"},
		TokenAlter:     {Description: "alter"},
		TokenCreate:    {Description: "create"},
		TokenSubscribe: {Description: "subscribe"},
		TokenFilter:    {Description: "filter"},
		TokenShow:      {Description: "show"},
		TokenDescribe:  {Description: "describe"},
		TokenExplain:   {Description: "explain"},

		// Top Level ql clause keywords
		TokenTable:   {Description: "table"},
		TokenInto:    {Description: "into"},
		TokenBy:      {Description: "by"},
		TokenFrom:    {Description: "from"},
		TokenWhere:   {Description: "where"},
		TokenHaving:  {Description: "having"},
		TokenGroupBy: {Description: "group by"},
		// Other Ql Keywords
		TokenAlias:    {Description: "alias"},
		TokenWith:     {Description: "with"},
		TokenValues:   {Description: "values"},
		TokenLimit:    {Description: "limit"},
		TokenOrderBy:  {Description: "order by"},
		TokenInner:    {Description: "inner"},
		TokenCross:    {Description: "cross"},
		TokenOuter:    {Description: "outer"},
		TokenLeft:     {Description: "left"},
		TokenRight:    {Description: "right"},
		TokenJoin:     {Description: "join"},
		TokenOn:       {Description: "on"},
		TokenDistinct: {Description: "distinct"},
		TokenAll:      {Description: "all"},
		TokenInclude:  {Description: "include"},
		TokenExists:   {Description: "exists"},

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
		TokenUse:  {Description: "use"},

		// value types
		TokenIdentity:             {Description: "identity"},
		TokenValue:                {Description: "value"},
		TokenValueWithSingleQuote: {Description: "valueWithSingleQuote"},
		TokenRegex:                {Description: "regex"},
		TokenDuration:             {Description: "duration"},

		// Primitive literals.
		TokenBool:    {Description: "Bool"},
		TokenFloat:   {Description: "Float"},
		TokenInteger: {Description: "Integer"},
		TokenString:  {Description: "String"},
		TokenText:    {Description: "Text"},
		TokenVarChar: {Description: "varchar"},
		TokenBigInt:  {Description: "bigint"},

		// Some other data Types
		TokenDataType: {Description: "datatype"}, // Generic DataType similar to "TokenIdentity" for unknown data types
		TokenList:     {Description: "List"},
		TokenMap:      {Description: "Map"},
		TokenJson:     {Description: "JSON"},
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
	FilterQLDialect.Init()
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
