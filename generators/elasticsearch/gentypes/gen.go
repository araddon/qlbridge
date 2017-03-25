package gentypes

import (
	"fmt"
	"strconv"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY
	// lets make sure this local interface SchemaColumns
	// matches the SourceTableColumn from schema
	_ schema.SourceTableColumn = (*fsc)(nil)
	_ SchemaColumns            = (*fsc)(nil)
)

type (
	// FilterValidate interface Will walk a filter statement validating columns, types
	// against underlying Schema.
	FilterValidate func(fs *rel.FilterStatement) error

	// SchemaColumns provides info on fields/columns to help the generator
	// understand how to map Columns to Underlying es fields
	SchemaColumns interface {
		// Underlying data type of column
		Column(col string) (value.ValueType, bool)
		// ColumnInfo of a FilterStatement column explains this column
		// and how to map to Elasticsearch field or false if the field
		// doesn't exist.
		ColumnInfo(col string) (*FieldType, bool)
	}
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

	fsc struct{}
)

func (m *fsc) Column(col string) (value.ValueType, bool) { return value.UnknownType, true }
func (m *fsc) ColumnInfo(col string) (*FieldType, bool)  { return nil, true }

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
