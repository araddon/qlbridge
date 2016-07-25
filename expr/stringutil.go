package expr

import (
	"bytes"
	"io"
	"strings"
	"unicode"
)

// LeftRight Return left, right values if is of form `table.column` or `schema`.`table`
// also return true/false for if it even has left/right
func LeftRight(val string) (string, string, bool) {
	vals := strings.SplitN(val, ".", 2)
	var left, right string
	if len(vals) == 1 {
		right = val
	} else {
		left = identTrim(vals[0])
		right = vals[1]
	}
	right = identTrim(right)
	return left, right, left != ""
}

func identTrim(ident string) string {
	if len(ident) > 0 {
		if ident[0] == '`' || ident[0] == '[' {
			ident = ident[1:]
		}
		if len(ident) > 0 {
			if ident[len(ident)-1] == '`' || ident[len(ident)-1] == ']' {
				ident = ident[0 : len(ident)-1]
			}
		}
	}
	return ident
}

// IdentityMaybeEscape Quote an identity/literal
// if need be (has illegal characters or spaces)
func IdentityMaybeEscapeBuf(buf *bytes.Buffer, quote byte, ident string) {
	IdentityMaybeQuoteStrictBuf(buf, quote, ident)
}

// IdentityMaybeQuoteStrict Quote an identity if need be (has illegal characters or spaces)
//  First character MUST be alpha (not numeric or any other character)
func IdentityMaybeQuoteStrictBuf(buf *bytes.Buffer, quote byte, ident string) {

	needsQuote := false
	if len(ident) > 0 && !unicode.IsLetter(rune(ident[0])) {
		needsQuote = true
	} else {
		for _, r := range ident {
			if !lex.IsIdentifierRune(r) {
				needsQuote = true
				break
			} else if r == quote {
				needsQuote = true
				break
			}
		}
	}

	if needsQuote {
		buf.WriteByte(quote)
		escapeQuote(&buf, quote, ident)
		buf.WriteByte(quote)
	} else {
		io.WriteString(&buf, ident)
	}
}

// IdentityMaybeQuoteStrict Quote an identity if need be (has illegal characters or spaces)
//  First character MUST be alpha (not numeric or any other character)
func IdentityMaybeQuoteStrict(quote byte, ident string) string {
	var buf bytes.Buffer
	IdentityMaybeQuoteStrictBuf(&buf, quote, ident)
	return buf.String()
}

func escapeQuote(buf *bytes.Buffer, quote rune, val string) {
	last := 0
	for i, r := range val {
		if r == quote {
			io.WriteString(&buf, val[last:i])
			io.WriteString(&buf, string(quote+quote))
			last = i + 1
		}
	}
	io.WriteString(&buf, val[last:])
}

// LiteralQuoteEscape escape string that may need characters escaped
//
//  LiteralQuoteEscape("'","item's") => 'item''s'
//  LiteralQuoteEscape(`"`,"item's") => "item's"
//  LiteralQuoteEscape(`"`,`item"s`) => "item""s"
//
func LiteralQuoteEscape(quote rune, ident string) string {
	var buf bytes.Buffer
	LiteralQuoteEscapeBuf(&buf, quote, ident)
	return buf.String()
}

// LiteralQuoteEscapeBuf escape string that may need characters escaped
//
//  LiteralQuoteEscapeBuf("'","item's") => 'item''s'
//  LiteralQuoteEscapeBuf(`"`,"item's") => "item's"
//  LiteralQuoteEscapeBuf(`"`,`item"s`) => "item""s"
//
func LiteralQuoteEscapeBuf(buf *bytes.buffer, quote rune, ident string) {
	buf.WriteByte(quote)
	escapeQuote(&buf, quote, ident)
	buf.WriteByte(quote)
}

// StringEscape escape string that may need characters escaped
//
//  StringEscape("'","item's") => "item''s"
//
func StringEscape(quote rune, ident string) string {
	var buf bytes.Buffer
	escapeQuote(&buf, quote, ident)
	return buf.String()
}

// A break, is some character such as comma, ;, whitespace
func isBreak(r rune) bool {
	switch r {
	case ',', ';', ' ', '\n', '\t':
		return true
	}
	return false
}
