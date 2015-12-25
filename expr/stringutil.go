package expr

import (
	"bytes"
	"io"
	"strings"
)

// Return left, right values if is of form   `table.column` or `schema`.`table`
// also return true/false for if it even has left/right
func LeftRight(val string) (string, string, bool) {
	vals := strings.SplitN(val, ".", 2)
	var left, right string
	if len(vals) == 1 {
		right = val
	} else {
		left = vals[0]
		right = vals[1]
	}
	left = identTrim(left)
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

// Quote an identity if need be (has illegal characters or spaces)
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

func IdentityEscape(quote rune, ident string) string {
	var buf bytes.Buffer
	last := 0
	for i, r := range ident {
		if r == quote {
			io.WriteString(&buf, ident[last:i])
			io.WriteString(&buf, `''`)
			last = i + 1
		}
	}
	io.WriteString(&buf, ident[last:])
	return buf.String()
}
