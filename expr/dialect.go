package expr

import (
	"bytes"
	"io"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/value"
)

type (
	// DialectWriters allow different dialects to have different escape characters
	// - postgres:  literal-escape = ', identity = "
	// - mysql:     literal-escape = ", identity = `
	// - cql:       literal-escape = ', identity = `
	// - bigquery:  literal-escape = ", identity = []
	DialectWriter interface {
		io.Writer
		Len() int
		WriteLiteral(string)
		WriteIdentity(string)
		WriteIdentityQuote(string, byte)
		WriteNumber(string)
		WriteNull()
		WriteValue(v value.Value)
		String() string
	}
	// Default Dialect writer uses mysql escaping rules literals=", identity=`
	defaultDialect struct {
		bytes.Buffer
		Null          string
		LiteralQuote  byte
		IdentityQuote byte
	}
	// finterprinter, ie ? substitution
	fingerprintDialect struct {
		DialectWriter
		replace string
	}
	// Keyword writer
	keywordDialect struct {
		*defaultDialect
		kw map[string]struct{}
	}
)

// NewDialectWriter creates a writer that is custom literal and identity
// escape characters
func NewDialectWriter(l, i byte) DialectWriter {
	return &defaultDialect{LiteralQuote: l, IdentityQuote: i}
}

// NewDefaultWriter uses mysql escaping rules literals=", identity=`
func NewDefaultWriter() DialectWriter {
	return &defaultDialect{LiteralQuote: '"', IdentityQuote: '`', Null: "NULL"}
}

// WriteLiteral writes literal with escapes if needed
func (w *defaultDialect) WriteLiteral(l string) {
	if len(l) == 1 && l == "*" {
		w.WriteByte('*')
		return
	}
	LiteralQuoteEscapeBuf(&w.Buffer, rune(w.LiteralQuote), l)
}

// WriteIdentity writes identity with escaping if needed
func (w *defaultDialect) WriteIdentity(i string) {
	if len(i) == 1 && i == "*" {
		w.WriteByte('*')
		return
	}
	IdentityMaybeEscapeBuf(&w.Buffer, w.IdentityQuote, i)
}

// WriteIdentityQuote write out an identity using given quote character
func (w *defaultDialect) WriteIdentityQuote(i string, quote byte) {
	if len(i) == 1 && i == "*" {
		w.WriteByte('*')
		return
	}
	LiteralQuoteEscapeBuf(&w.Buffer, rune(w.IdentityQuote), i)
}
func (w *defaultDialect) WriteNumber(n string) {
	io.WriteString(w, n)
}
func (w *defaultDialect) WriteNull() {
	io.WriteString(w, w.Null)
}
func (w *defaultDialect) WriteValue(v value.Value) {
	switch vt := v.(type) {
	case value.StringValue:
		w.WriteLiteral(vt.Val())
	case value.IntValue:
		w.WriteNumber(vt.ToString())
	case value.NumberValue:
		w.WriteNumber(vt.ToString())
	case value.BoolValue:
		io.WriteString(w, vt.ToString())
	case nil, value.NilValue:
		w.WriteNull()
	case value.TimeValue:
		w.WriteLiteral(vt.Val().String())
	case value.Slice:
		// If you don't want json, then over-ride this WriteValue
		by, err := vt.MarshalJSON()
		if err == nil {
			w.Write(by)
		} else {
			u.Debugf("could not convert %v", err)
			w.Write([]byte("[]"))
		}
	case value.Map:
		// If you don't want json, then over-ride this WriteValue
		by, err := vt.MarshalJSON()
		if err == nil {
			w.Write(by)
		} else {
			u.Debugf("could not convert %v", err)
			w.Write([]byte("null"))
		}
	default:
		io.WriteString(w, vt.ToString())
	}
}
func NewKeywordDialect(kw []string) DialectWriter {
	m := make(map[string]struct{}, len(kw))
	for _, w := range kw {
		m[w] = struct{}{}
	}
	return &keywordDialect{
		&defaultDialect{LiteralQuote: '"', IdentityQuote: '`', Null: "NULL"},
		m,
	}
}
func (w *keywordDialect) WriteIdentity(id string) {
	_, isKeyword := w.kw[strings.ToLower(id)]
	if isKeyword {
		io.WriteString(w, LiteralQuoteEscape(rune(w.IdentityQuote), id))
		return
	}
	w.defaultDialect.WriteIdentity(id)
}

func NewFingerPrinter() DialectWriter {
	return &fingerprintDialect{NewDefaultWriter(), "?"}
}
func NewFingerPrintWriter(replace string, w DialectWriter) DialectWriter {
	return &fingerprintDialect{w, replace}
}
func (w *fingerprintDialect) WriteLiteral(l string) {
	io.WriteString(w.DialectWriter, w.replace)
}
func (w *fingerprintDialect) WriteNumber(n string) {
	io.WriteString(w.DialectWriter, w.replace)
}
func (w *fingerprintDialect) WriteIdentity(id string) {
	w.DialectWriter.WriteIdentity(strings.ToLower(id))
}
