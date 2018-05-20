package sqlite

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (

// normal tables
//defaultSchemaTables = []string{"tables", "databases", "columns", "global_variables", "session_variables","functions", "procedures", "engines", "status", "indexes"}
)

func init() {
	datasource.DialectWriterCols = append(datasource.DialectWriterCols, "sqlite")
	datasource.DialectWriters = append(datasource.DialectWriters, &sqliteWriter{})
}

type sqliteWriter struct {
}

func (m *sqliteWriter) Dialect() string {
	return "sqlite"
}
func (m *sqliteWriter) FieldType(t value.ValueType) string {
	return ValueString(t)
}

// Table Implement Dialect Specific Writers
// ie, mysql, postgres, cassandra all have different dialects
// so the Create statements are quite different

// Table output a CREATE TABLE statement using mysql dialect.
func (m *sqliteWriter) Table(tbl *schema.Table) string {
	return TableToString(tbl)
}

// TableToString Table output a CREATE TABLE statement using mysql dialect.
func TableToString(tbl *schema.Table) string {

	w := &bytes.Buffer{}
	//u.Infof("%s tbl=%p fields? %#v fields?%v", tbl.Name, tbl, tbl.FieldMap, len(tbl.Fields))
	fmt.Fprintf(w, "CREATE TABLE `%s` (", tbl.Name)
	for i, fld := range tbl.Fields {
		if i != 0 {
			w.WriteByte(',')
		}
		fmt.Fprint(w, "\n    ")
		WriteField(w, fld)
	}
	fmt.Fprint(w, "\n);")
	//tblStr := fmt.Sprintf("CREATE TABLE `%s` (\n\n);", tbl.Name, strings.Join(cols, ","))
	//return tblStr, nil
	return w.String()
}

// WriteField write a schema.Field as string output for sqlite create statement
//
// https://www.sqlite.org/datatype3.html
func WriteField(w *bytes.Buffer, fld *schema.Field) {
	fmt.Fprintf(w, "`%s` ", fld.Name)
	/*
		NULL. The value is a NULL value.
		INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
		REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
		TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
		BLOB. The value is a blob of data, stored exactly as it was input.
	*/
	//deflen := fld.Length
	switch fld.ValueType() {
	case value.BoolType:
		fmt.Fprint(w, "INTEGER")
	case value.IntType:
		fmt.Fprint(w, "INTEGER")
	case value.StringType:
		fmt.Fprintf(w, "text")
	case value.NumberType:
		fmt.Fprint(w, "REAL")
	case value.TimeType:
		fmt.Fprint(w, "text")
	case value.JsonType:
		fmt.Fprintf(w, "text")
	default:
		fmt.Fprint(w, "text")
	}
	if len(fld.Description) > 0 {
		fmt.Fprintf(w, " COMMENT %q", fld.Description)
	}
}

// TypeFromString given a string, return data type
func TypeFromString(t string) value.ValueType {
	switch strings.ToLower(t) {
	case "integer":
		// This isn't necessarily true, as integer could be bool
		return value.IntType
	case "real":
		return value.NumberType
	default:
		return value.StringType
	}
}

// ValueString convert a value.ValueType into a sqlite type descriptor
func ValueString(t value.ValueType) string {
	switch t {
	case value.NilType:
		return "text"
	case value.ErrorType:
		return "text"
	case value.UnknownType:
		return "text"
	case value.ValueInterfaceType:
		return "text"
	case value.NumberType:
		return "real"
	case value.IntType:
		return "integer"
	case value.BoolType:
		return "integer"
	case value.TimeType:
		return "text"
	case value.ByteSliceType:
		return "text"
	case value.StringType:
		return "text"
	case value.StringsType:
		return "text"
	case value.MapValueType:
		return "text"
	case value.MapIntType:
		return "text"
	case value.MapStringType:
		return "text"
	case value.MapNumberType:
		return "text"
	case value.MapBoolType:
		return "text"
	case value.SliceValueType:
		return "text"
	case value.StructType:
		return "text"
	case value.JsonType:
		return "text"
	default:
		return "text"
	}
}
