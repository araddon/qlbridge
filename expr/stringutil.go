package expr

import (
	"bytes"
	"io"
	"strings"
	"unicode"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/lex"
)

var _ = u.EMPTY

// LeftRight Return left, right values if is of form `table.column` or `schema`.`table`
// also return true/false for if it even has left/right
func LeftRight(val string) (string, string, bool) {
	if len(val) < 2 {
		return "", val, false
	}
	switch by := val[0]; by {
	case '`':
		vals := strings.Split(val, "`.`")
		if len(vals) == 1 {
			return "", IdentityTrim(val), false
		} else if len(vals) == 2 {
			return IdentityTrim(vals[0]), IdentityTrim(vals[1]), true
		}
		// wat, no idea what this is
		return "", val, false
	case '[':
		vals := strings.Split(val, "].[")
		if len(vals) == 1 {
			return "", IdentityTrim(val), false
		} else if len(vals) == 2 {
			return IdentityTrim(vals[0]), IdentityTrim(vals[1]), true
		}
		// wat, no idea what this is
		return "", val, false
	default:
		vals := strings.SplitN(val, ".", 2)
		if len(vals) == 1 {
			return "", val, false
		} else if len(vals) == 2 {
			return IdentityTrim(vals[0]), IdentityTrim(vals[1]), true
		}
	}

	return "", val, false
}

// IdentityTrim trims the leading/trailing identity quote marks  ` or []
func IdentityTrim(ident string) string {
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

// IdentityMaybeQuote
func IdentityMaybeQuote(quote byte, ident string) string {
	buf := bytes.Buffer{}
	IdentityMaybeQuoteStrictBuf(&buf, quote, ident)
	return buf.String()
}

// IdentityMaybeEscape Quote an identity/literal
// if need be (has illegal characters or spaces)
func IdentityMaybeEscapeBuf(buf *bytes.Buffer, quote byte, ident string) {
	IdentityMaybeQuoteStrictBuf(buf, quote, ident)
}

// IdentityMaybeQuoteStrict Quote an identity if need be (has illegal characters or spaces)
//  First character MUST be alpha (not numeric or any other character)
func IdentityMaybeQuoteStrictBuf(buf *bytes.Buffer, quote byte, ident string) {

	if len(ident) == 0 {
		return
	}
	needsQuote := false
	quoter := rune(quote)
	if len(ident) > 1 {
		if ident[0] == quote && ident[len(ident)-1] == quote {
			// Already escaped??
			io.WriteString(buf, ident)
			return
		}
	}
	if ident[0] == '@' {
		needsQuote = false
	} else if len(ident) > 0 && !unicode.IsLetter(rune(ident[0])) {
		needsQuote = true
	} else {
		for _, r := range ident {
			if !lex.IsIdentifierRune(r) {
				needsQuote = true
				break
			} else if r == quoter {
				needsQuote = true
				break
			} else if r == '.' {
				needsQuote = true
				break
			}
		}
	}

	if needsQuote {
		buf.WriteByte(quote)
		escapeQuote(buf, quoter, ident)
		buf.WriteByte(quote)
	} else {
		io.WriteString(buf, ident)
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
			io.WriteString(buf, val[last:i])
			io.WriteString(buf, string(quote))
			io.WriteString(buf, string(quote))
			last = i + 1
		}
	}
	io.WriteString(buf, val[last:])
}

// LiteralQuoteEscape escape string that may need characters escaped
//
//  LiteralQuoteEscape("'","item's") => 'item''s'
//  LiteralQuoteEscape(`"`,"item's") => "item's"
//  LiteralQuoteEscape(`"`,`item"s`) => "item""s"
//
func LiteralQuoteEscape(quote rune, literal string) string {
	if len(literal) > 1 {
		quoteb := byte(quote)
		if literal[0] == quoteb && literal[len(literal)-1] == quoteb {
			// Already escaped??
			return literal
		}
	}
	var buf bytes.Buffer
	LiteralQuoteEscapeBuf(&buf, quote, literal)
	return buf.String()
}

// LiteralQuoteEscapeBuf escape string that may need characters escaped
//
//  LiteralQuoteEscapeBuf("'","item's") => 'item''s'
//  LiteralQuoteEscapeBuf(`"`,"item's") => "item's"
//  LiteralQuoteEscapeBuf(`"`,`item"s`) => "item""s"
//
func LiteralQuoteEscapeBuf(buf *bytes.Buffer, quote rune, literal string) {
	if len(literal) > 1 {
		quoteb := byte(quote)
		if literal[0] == quoteb && literal[len(literal)-1] == quoteb {
			// Already escaped??
			io.WriteString(buf, literal)
			return
		}
	}
	buf.WriteByte(byte(quote))
	escapeQuote(buf, quote, literal)
	buf.WriteByte(byte(quote))
}

// StringEscape escape string that may need characters escaped
//
//  StringEscape("'","item's") => "item''s"
//
func StringEscape(quote rune, literal string) string {
	var buf bytes.Buffer
	escapeQuote(&buf, quote, literal)
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
