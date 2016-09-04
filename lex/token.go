package lex

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
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
	T      TokenType // type
	V      string    // value
	Quote  byte      // quote mark:    " ` [ '
	Line   int       // Line #
	Column int       // Position in line
}

// convert to human readable string
func (t Token) String() string {
	return fmt.Sprintf(`Token{ %s Type:"%v" Line:%d Col:%d Q:%s}`,
		t.V, t.T.String(), t.Line, t.Column, string(t.Quote))
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
	TokenNewLine  TokenType = 6 // NewLine  = \n

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

	//  operand related tokens
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
	TokenContains         TokenType = 89 // CONTAINS
	TokenIntersects       TokenType = 90 // INTERSECTS

	// ql top-level keywords, these first keywords determine parser
	TokenPrepare   TokenType = 200
	TokenInsert    TokenType = 201
	TokenUpdate    TokenType = 202
	TokenDelete    TokenType = 203
	TokenSelect    TokenType = 204
	TokenUpsert    TokenType = 205
	TokenAlter     TokenType = 206
	TokenCreate    TokenType = 207
	TokenSubscribe TokenType = 208
	TokenFilter    TokenType = 209
	TokenShow      TokenType = 210
	TokenDescribe  TokenType = 211 // We can also use TokenDesc
	TokenExplain   TokenType = 212 // another alias for desccribe
	TokenReplace   TokenType = 213 // Insert/Replace are interchangeable on insert statements
	TokenRollback  TokenType = 214
	TokenCommit    TokenType = 215

	// Other QL Keywords, These are clause-level keywords that mark seperation between clauses
	TokenTable    TokenType = 301 // table
	TokenFrom     TokenType = 302 // from
	TokenWhere    TokenType = 303 // where
	TokenHaving   TokenType = 304 // having
	TokenGroupBy  TokenType = 305 // group by
	TokenBy       TokenType = 306 // by
	TokenAlias    TokenType = 307 // alias
	TokenWith     TokenType = 308 // with
	TokenValues   TokenType = 309 // values
	TokenInto     TokenType = 310 // into
	TokenLimit    TokenType = 311 // limit
	TokenOrderBy  TokenType = 312 // order by
	TokenInner    TokenType = 313 // inner , ie of join
	TokenCross    TokenType = 314 // cross
	TokenOuter    TokenType = 315 // outer
	TokenLeft     TokenType = 316 // left
	TokenRight    TokenType = 317 // right
	TokenJoin     TokenType = 318 // Join
	TokenOn       TokenType = 319 // on
	TokenDistinct TokenType = 320 // DISTINCT
	TokenAll      TokenType = 321 // all
	TokenInclude  TokenType = 322 // INCLUDE
	TokenExists   TokenType = 323 // EXISTS
	TokenOffset   TokenType = 324 // OFFSET
	TokenFull     TokenType = 325 // FULL
	TokenGlobal   TokenType = 326 // GLOBAL
	TokenSession  TokenType = 327 // SESSION
	TokenTables   TokenType = 328 // TABLES

	// ddl
	TokenChange       TokenType = 400 // change
	TokenAdd          TokenType = 401 // add
	TokenFirst        TokenType = 402 // first
	TokenAfter        TokenType = 403 // after
	TokenCharacterSet TokenType = 404 // character set

	// Other QL keywords
	TokenSet  TokenType = 500 // set
	TokenAs   TokenType = 501 // as
	TokenAsc  TokenType = 502 // ascending
	TokenDesc TokenType = 503 // descending
	TokenUse  TokenType = 504 // use

	// User defined function/expression
	TokenUdfExpr TokenType = 550

	// Value Types
	TokenIdentity             TokenType = 600 // identity, either column, table name etc
	TokenValue                TokenType = 601 // 'some string' string or continous sequence of chars delimited by WHITE SPACE | ' | , | ( | )
	TokenValueWithSingleQuote TokenType = 602 // '' becomes ' inside the string, parser will need to replace the string
	TokenRegex                TokenType = 603 // regex
	TokenDuration             TokenType = 604 // 14d , 22w, 3y, 45ms, 45us, 24hr, 2h, 45m, 30s

	// Scalar literal data-types
	TokenDataType TokenType = 1000 // A generic Identifier of DataTypes
	TokenBool     TokenType = 1001
	TokenFloat    TokenType = 1002
	TokenInteger  TokenType = 1003
	TokenString   TokenType = 1004
	TokenVarChar  TokenType = 1005
	TokenBigInt   TokenType = 1006
	TokenText     TokenType = 1007
	TokenJson     TokenType = 1008

	// Composite Data Types
	TokenList TokenType = 1050
	TokenMap  TokenType = 1051
)

var (
	// Which Identity Characters are allowed for UNESCAPED identities
	IDENTITY_CHARS = "_.-/"
	// A much more lax identity char set rule  that allows spaces
	IDENTITY_LAX_CHARS = "_./- "
	// sql variables start with @@ ??
	IDENTITY_SQL_CHARS = "@_.-"

	// list of token-name
	TokenNameMap = map[TokenType]*TokenInfo{

		TokenEOF:      {Description: "EOF"},
		TokenEOS:      {Description: ";"},
		TokenEofOrEos: {Kw: "", Description: "; OR EOF"},
		TokenError:    {Description: "Error"},
		TokenRaw:      {Description: "unlexed text"},
		TokenNewLine:  {Description: "New Line"},

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
		TokenContains:   {Kw: "contains", Description: "contains"},
		TokenIntersects: {Kw: "intersects", Description: "intersects"},

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
		TokenReplace:   {Description: "replace"},
		TokenRollback:  {Description: "rollback"},
		TokenCommit:    {Description: "commit"},

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
		TokenOffset:   {Description: "offset"},
		TokenFull:     {Description: "full"},
		TokenGlobal:   {Description: "global"},
		TokenSession:  {Description: "session"},
		TokenTables:   {Description: "tables"},

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

		// scalar literals.
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

	TokenToOp = make(map[string]TokenType)
)

func init() {
	LoadTokenInfo()
	SqlDialect.Init()
	FilterQLDialect.Init()
	JsonDialect.Init()
}

func LoadTokenInfo() {
	for tok, ti := range TokenNameMap {
		ti.T = tok
		if ti.Kw == "" {
			ti.Kw = ti.Description
		}
		TokenToOp[ti.Kw] = tok
		if strings.Contains(ti.Kw, " ") {
			parts := strings.Split(ti.Kw, " ")
			ti.firstWord = parts[0]
			ti.HasSpaces = true
		}
	}
}

func TokenFromOp(op string) Token {
	tt, ok := TokenToOp[op]
	if ok {
		return Token{T: tt, V: op}
	}
	return Token{T: TokenNil, V: "nil"}
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
