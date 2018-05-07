package sqlite

import (
	"bytes"
	"fmt"

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
func WriteField(w *bytes.Buffer, fld *schema.Field) {
	fmt.Fprintf(w, "`%s` ", fld.Name)
	//deflen := fld.Length
	switch fld.ValueType() {
	case value.BoolType:
		fmt.Fprint(w, "tinyint(1) DEFAULT NULL")
	case value.IntType:
		fmt.Fprint(w, "bigint DEFAULT NULL")
	case value.StringType:
		fmt.Fprintf(w, "text DEFAULT NULL")
	case value.NumberType:
		fmt.Fprint(w, "float DEFAULT NULL")
	case value.TimeType:
		fmt.Fprint(w, "datetime DEFAULT NULL")
	case value.JsonType:
		fmt.Fprintf(w, "text")
	default:
		fmt.Fprint(w, "text DEFAULT NULL")
	}
	if len(fld.Description) > 0 {
		fmt.Fprintf(w, " COMMENT %q", fld.Description)
	}
}
func ValueString(t value.ValueType) string {
	switch t {
	case value.NilType:
		return "NULL"
	case value.ErrorType:
		return "text"
	case value.UnknownType:
		return "text"
	case value.ValueInterfaceType:
		return "text"
	case value.NumberType:
		return "float"
	case value.IntType:
		return "long"
	case value.BoolType:
		return "boolean"
	case value.TimeType:
		return "datetime"
	case value.ByteSliceType:
		return "text"
	case value.StringType:
		return "varchar(255)"
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
		return "json"
	default:
		return "text"
	}
}
