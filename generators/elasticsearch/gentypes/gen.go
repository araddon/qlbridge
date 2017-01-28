package gentypes

import (
	"fmt"
	"strconv"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

type (
	// FieldMapper translates a given column name to a field-type
	FieldMapper interface {
		Column(col string) value.ValueType
		// Map a FilterStatement column to an Elasticsearch field or false if the field
		// doesn't exist.
		Map(qlcol string) (*FieldType, bool)
	}
	// FilterValidate interface Will walk a filter statement validating columns, types
	// against underlying Schema.
	FilterValidate func(fs *rel.FilterStatement) error
	// FieldType Describes a field's usage within Elasticsearch
	// - is it nested? which changes query semantics
	// - prefix for nested object values
	FieldType struct {
		Field    string // Field Name
		Prefix   string // .f, .b, .i, .v for nested object types
		Path     string // mapstr_fieldname ,etc, prefixed
		Type     value.ValueType
		TypeName string
	}
	// Payload is the top Level Request to Elasticsearch
	Payload struct {
		Size   *int                   `json:"size,omitempty"`
		Filter interface{}            `json:"filter,omitempty"`
		Fields []string               `json:"fields,omitempty"`
		Sort   []map[string]SortOrder `json:"sort,omitempty"`
	}
	// SortOder of the es query request
	SortOrder struct {
		Order string `json:"order"`
	}
)

// Numeric returns true if field type has numeric values.
func (f *FieldType) Numeric() bool {
	if f.Type == value.NumberType || f.Type == value.IntType {
		return true
	}

	// If a nested field with numeric values it's numeric
	if f.Nested() {
		switch f.Type {
		case value.MapIntType, value.MapNumberType:
			return true
		}
	}

	// Nothing else is numeric
	return false
}
func (f *FieldType) Nested() bool { return f.Path != "" }
func (f *FieldType) String() string {
	return fmt.Sprintf("<ft path=%q field=%q prefix=%q type=%q >", f.Path, f.Field, f.Prefix, f.Type.String())
}
func (f *FieldType) PathAndPrefix(val string) string {
	if f.Type == value.MapValueType {
		_, pfx := ValueAndPrefix(val)
		return f.Path + "." + pfx
	}
	return f.Path + "." + f.Prefix
}
func (f *FieldType) PrefixAndValue(val interface{}) (string, interface{}) {
	if f.Type == value.MapValueType {
		val, pfx := ValueAndPrefix(val)
		return f.Path + "." + pfx, val
	}
	return f.Path + "." + f.Prefix, val
}

func (p *Payload) SortAsc(field string) {
	p.Sort = append(p.Sort, map[string]SortOrder{field: SortOrder{"asc"}})
}

func (p *Payload) SortDesc(field string) {
	p.Sort = append(p.Sort, map[string]SortOrder{field: SortOrder{"desc"}})
}

// For Fields declared as map[string]type  (type  = int, string, time, bool, value)
// in lql, we need to determine which nested key/value combo to search for
func ValueAndPrefix(val interface{}) (interface{}, string) {

	switch vt := val.(type) {
	case string:
		// Most values come through as strings
		if v, err := strconv.ParseInt(vt, 10, 64); err == nil {
			return v, "i"
		} else if v, err := strconv.ParseBool(vt); err == nil {
			return v, "b"
		} else if v, err := strconv.ParseFloat(vt, 64); err == nil {
			return v, "f"
		} else if v, err := dateparse.ParseAny(vt); err == nil {
			return v, "t"
		}
	case time.Time:
		return vt, "t"
	case int:
		return vt, "i"
	case int32:
		return vt, "i"
	case int64:
		return vt, "i"
	case bool:
		return vt, "b"
	case float64:
		return vt, "f"
	case float32:
		return vt, "f"
	}

	// Default to strings
	return val, "s"
}
