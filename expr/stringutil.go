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
	if len(val) < 2 {
		return "", val, false
	}
	switch by := val[0]; by {
	case '`':
		vals := strings.Split(val, "`.`")
		if len(vals) == 1 {
			return "", identTrim(val), false
		} else if len(vals) == 2 {
			return identTrim(vals[0]), identTrim(vals[1]), true
		}
		// wat, no idea what this is
		return "", val, false
	case '[':
		vals := strings.Split(val, "].[")
		if len(vals) == 1 {
			return "", identTrim(val), false
		} else if len(vals) == 2 {
			return identTrim(vals[0]), identTrim(vals[1]), true
		}
		// wat, no idea what this is
		return "", val, false
	default:
		vals := strings.SplitN(val, ".", 2)
		if len(vals) == 1 {
			return "", val, false
		} else if len(vals) == 2 {
			return identTrim(vals[0]), identTrim(vals[1]), true
		}
	}

	return "", val, false
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

// IdentityMaybeQuote Quote an identity if need be (has illegal characters or spaces)
func IdentityMaybeQuote(quote byte, ident string) string {
	var buf bytes.Buffer
	//last := 0
	needsQuote := false
	//quoteRune := rune(quote)
	for _, r := range ident {
		if r == ' ' {
			needsQuote = true
			break
		} else if r == ',' {
			needsQuote = true
			break
		}
	}
	if needsQuote {
		buf.WriteByte(quote)
	}
	io.WriteString(&buf, ident)
	// for i, r := range ident {
	// 	if r == quoteRune {
	// 		io.WriteString(&buf, ident[last:i])
	// 		//io.WriteString(&buf, `''`)
	// 		last = i + 1
	// 	}
	// }
	// io.WriteString(&buf, ident[last:])
	if needsQuote {
		buf.WriteByte(quote)
	}
	return buf.String()
}

// IdentityMaybeQuoteStrict Quote an identity if need be (has illegal characters or spaces)
//  First character MUST be alpha (not numeric or any other character)
func IdentityMaybeQuoteStrict(quote byte, ident string) string {
	var buf bytes.Buffer
	//last := 0
	needsQuote := false
	//quoteRune := rune(quote)
	if len(ident) > 0 && !unicode.IsLetter(rune(ident[0])) {
		needsQuote = true
	} else {
		for _, r := range ident {
			if r == ' ' {
				needsQuote = true
				break
			} else if r == ',' {
				needsQuote = true
				break
			}
		}
	}

	if needsQuote {
		buf.WriteByte(quote)
	}
	io.WriteString(&buf, ident)
	// for i, r := range ident {
	// 	if r == quoteRune {
	// 		io.WriteString(&buf, ident[last:i])
	// 		//io.WriteString(&buf, `''`)
	// 		last = i + 1
	// 	}
	// }
	// io.WriteString(&buf, ident[last:])
	if needsQuote {
		buf.WriteByte(quote)
	}
	return buf.String()
}

// IdentityEscape escape string identity that may use quote
//  mark used in identities:
// IdentityEscape("'","item's") => "item''s"
func IdentityEscape(quote rune, ident string) string {
	var buf bytes.Buffer
	last := 0
	for i, r := range ident {
		if r == quote {
			io.WriteString(&buf, ident[last:i])
			io.WriteString(&buf, string(quote+quote))
			last = i + 1
		}
	}
	io.WriteString(&buf, ident[last:])
	return buf.String()
}
