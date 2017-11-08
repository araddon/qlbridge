package lex

import (
	"fmt"
	"strings"
)

// TokenType identifies the type of lexical tokens.
type TokenType uint16

// TokenInfo provides metadata about tokens
type TokenInfo struct {
	T           TokenType
	Kw          string
	firstWord   string // in event multi-word (Group By) the first word for match
	HasSpaces   bool
	Description string
}

// Token represents a text string returned from the lexer.
type Token struct {
	T      TokenType // type
	V      string    // value
	Quote  byte      // quote mark:    " ` [ '
	Line   int       // Line #
	Column int       // Position in line
	Pos    int       // Absolute position
}

// convert to human readable string
func (t Token) String() string {
	return fmt.Sprintf(`Token{ %s Type:"%v" Line:%d Col:%d Q:%s Pos:%d}`,
		t.V, t.T.String(), t.Line, t.Column, string(t.Quote), t.Pos)
}
func (t Token) Err(l *Lexer) error { return t.ErrMsg(l, "") }
func (t Token) ErrMsg(l *Lexer, msg string) error {
	return l.ErrMsg(t, msg)
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
	TokenDrop      TokenType = 208
	TokenSubscribe TokenType = 209
	TokenFilter    TokenType = 210
	TokenShow      TokenType = 211
	TokenDescribe  TokenType = 212 // We can also use TokenDesc
	TokenExplain   TokenType = 213 // another alias for desccribe
	TokenReplace   TokenType = 214 // Insert/Replace are interchangeable on insert statements
	TokenRollback  TokenType = 215
	TokenCommit    TokenType = 216

	// Other QL Keywords, These are clause-level keywords that mark separation between clauses
	TokenFrom     TokenType = 300 // from
	TokenWhere    TokenType = 301 // where
	TokenHaving   TokenType = 302 // having
	TokenGroupBy  TokenType = 303 // group by
	TokenBy       TokenType = 304 // by
	TokenAlias    TokenType = 305 // alias
	TokenWith     TokenType = 306 // with
	TokenValues   TokenType = 307 // values
	TokenInto     TokenType = 308 // into
	TokenLimit    TokenType = 309 // limit
	TokenOrderBy  TokenType = 310 // order by
	TokenInner    TokenType = 311 // inner , ie of join
	TokenCross    TokenType = 312 // cross
	TokenOuter    TokenType = 313 // outer
	TokenLeft     TokenType = 314 // left
	TokenRight    TokenType = 315 // right
	TokenJoin     TokenType = 316 // Join
	TokenOn       TokenType = 317 // on
	TokenDistinct TokenType = 318 // DISTINCT
	TokenAll      TokenType = 319 // all
	TokenInclude  TokenType = 320 // INCLUDE
	TokenExists   TokenType = 321 // EXISTS
	TokenOffset   TokenType = 322 // OFFSET
	TokenFull     TokenType = 323 // FULL
	TokenGlobal   TokenType = 324 // GLOBAL
	TokenSession  TokenType = 325 // SESSION
	TokenTables   TokenType = 326 // TABLES

	// ddl major words
	TokenSchema         TokenType = 400 // SCHEMA
	TokenDatabase       TokenType = 401 // DATABASE
	TokenTable          TokenType = 402 // TABLE
	TokenSource         TokenType = 403 // SOURCE
	TokenView           TokenType = 404 // VIEW
	TokenContinuousView TokenType = 405 // CONTINUOUSVIEW
	TokenTemp           TokenType = 406 // TEMP or TEMPORARY

	// ddl other
	TokenChange       TokenType = 410 // change
	TokenAdd          TokenType = 411 // add
	TokenFirst        TokenType = 412 // first
	TokenAfter        TokenType = 413 // after
	TokenCharacterSet TokenType = 414 // character set
	TokenDefault      TokenType = 415 // default
	TokenUnique       TokenType = 416 // unique
	TokenKey          TokenType = 417 // key
	TokenPrimary      TokenType = 418 // primary
	TokenConstraint   TokenType = 419 // constraint
	TokenForeign      TokenType = 420 // foreign
	TokenReferences   TokenType = 421 // references
	TokenEngine       TokenType = 422 // engine

	// Other QL keywords
	TokenSet  TokenType = 500 // set
	TokenAs   TokenType = 501 // as
	TokenAsc  TokenType = 502 // ascending
	TokenDesc TokenType = 503 // descending
	TokenUse  TokenType = 504 // use

	// User defined function/expression
	TokenUdfExpr TokenType = 550

	// Value Types
	TokenIdentity     TokenType = 600 // identity, either column, table name etc
	TokenValue        TokenType = 601 // 'some string' string or continuous sequence of chars delimited by WHITE SPACE | ' | , | ( | )
	TokenValueEscaped TokenType = 602 // '' becomes ' inside the string, parser will need to replace the string
	TokenRegex        TokenType = 603 // regex
	TokenDuration     TokenType = 604 // 14d , 22w, 3y, 45ms, 45us, 24hr, 2h, 45m, 30s

	// Data Type Definitions
	TokenTypeDef     TokenType = 999
	TokenTypeBool    TokenType = 998
	TokenTypeFloat   TokenType = 997
	TokenTypeInteger TokenType = 996
	TokenTypeString  TokenType = 995
	TokenTypeVarChar TokenType = 994
	TokenTypeChar    TokenType = 993
	TokenTypeBigInt  TokenType = 992
	TokenTypeTime    TokenType = 991
	TokenTypeText    TokenType = 990
	TokenTypeJson    TokenType = 989

	// Value types
	TokenValueType TokenType = 1000 // A generic Identifier of value type
	TokenBool      TokenType = 1001
	TokenFloat     TokenType = 1002
	TokenInteger   TokenType = 1003
	TokenString    TokenType = 1004
	TokenTime      TokenType = 1005

	// Composite Data Types
	TokenJson TokenType = 1010
	TokenList TokenType = 1011
	TokenMap  TokenType = 1012
)

var (
	// IDENTITY_CHARS Which Identity Characters are allowed for UNESCAPED identities
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
		TokenDrop:      {Description: "drop"},
		TokenSubscribe: {Description: "subscribe"},
		TokenFilter:    {Description: "filter"},
		TokenShow:      {Description: "show"},
		TokenDescribe:  {Description: "describe"},
		TokenExplain:   {Description: "explain"},
		TokenReplace:   {Description: "replace"},
		TokenRollback:  {Description: "rollback"},
		TokenCommit:    {Description: "commit"},

		// Top Level dml ql clause keywords
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
		TokenSchema:         {Description: "schema"},
		TokenDatabase:       {Description: "database"},
		TokenTable:          {Description: "table"},
		TokenSource:         {Description: "source"},
		TokenView:           {Description: "view"},
		TokenContinuousView: {Description: "continuousview"},
		TokenTemp:           {Description: "temp"},
		// ddl other
		TokenChange:       {Description: "change"},
		TokenCharacterSet: {Description: "character set"},
		TokenAdd:          {Description: "add"},
		TokenFirst:        {Description: "first"},
		TokenAfter:        {Description: "after"},
		TokenDefault:      {Description: "default"},
		TokenUnique:       {Description: "unique"},
		TokenKey:          {Description: "key"},
		TokenPrimary:      {Description: "primary"},
		TokenConstraint:   {Description: "constraint"},
		TokenForeign:      {Description: "foreign"},
		TokenReferences:   {Description: "references"},
		TokenEngine:       {Description: "engine"},

		// QL Keywords, all lower-case
		TokenSet:  {Description: "set"},
		TokenAs:   {Description: "as"},
		TokenAsc:  {Description: "asc"},
		TokenDesc: {Description: "desc"},
		TokenUse:  {Description: "use"},

		// special value types
		TokenIdentity:     {Description: "identity"},
		TokenValue:        {Description: "value"},
		TokenValueEscaped: {Description: "value-escaped"},
		TokenRegex:        {Description: "regex"},
		TokenDuration:     {Description: "duration"},

		// Data TYPES:  ie type system
		TokenTypeDef:     {Description: "TypeDef"}, // Generic DataType
		TokenTypeBool:    {Description: "BoolType"},
		TokenTypeFloat:   {Description: "FloatType"},
		TokenTypeInteger: {Description: "IntegerType"},
		TokenTypeString:  {Description: "StringType"},
		TokenTypeVarChar: {Description: "VarCharType"},
		TokenTypeChar:    {Description: "CharType"},
		TokenTypeBigInt:  {Description: "BigIntType"},
		TokenTypeTime:    {Description: "TimeType"},
		TokenTypeText:    {Description: "TextType"},
		TokenTypeJson:    {Description: "JsonType"},

		// VALUE TYPES:  ie literal values
		TokenBool:    {Description: "BoolVal"},
		TokenFloat:   {Description: "FloatVal"},
		TokenInteger: {Description: "IntegerVal"},
		TokenString:  {Description: "StringVal"},
		TokenTime:    {Description: "TimeVal"},

		// Some other value Types
		TokenValueType: {Description: "Value"}, // Generic DataType just stores in a value.Value
		TokenList:      {Description: "List"},
		TokenMap:       {Description: "Map"},
		TokenJson:      {Description: "JSON"},
	}

	TokenToOp = make(map[string]TokenType)
)

func init() {
	LoadTokenInfo()
	SqlDialect.Init()
	FilterQLDialect.Init()
	JsonDialect.Init()
}

// LoadTokenInfo load the token info into global map
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

// TokenFromOp get token from operation string
func TokenFromOp(op string) Token {
	tt, ok := TokenToOp[op]
	if ok {
		return Token{T: tt, V: op}
	}
	return Token{T: TokenNil}
}

// String convert to human readable string
func (typ TokenType) String() string {
	s, ok := TokenNameMap[typ]
	if ok {
		return s.Kw
	}
	return "not implemented"
}

// MatchString which keyword should we look for, either full keyword
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

// MultiWord is this a word such as "Group by" with multiple words?
func (typ TokenType) MultiWord() bool {
	tokInfo, ok := TokenNameMap[typ]
	if ok {
		return tokInfo.HasSpaces
	}
	return false
}
