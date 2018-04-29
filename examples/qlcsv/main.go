package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"net/mail"
	"strings"

	// Side-Effect Import the qlbridge sql driver
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/value"
)

var (
	sqlText          string
	flagCsvDelimiter = ","
	logging          = "info"
)

func init() {

	flag.StringVar(&logging, "logging", "info", "logging [ debug,info ]")
	flag.StringVar(&sqlText, "sql", "", "QL ish query multi-node such as [select user_id, yy(reg_date) from stdio];")
	flag.StringVar(&flagCsvDelimiter, "delimiter", ",", "delimiter:   default = comma [t,|]")
	flag.Parse()

	u.SetupLogging(logging)
	u.SetColorOutput()
}

func main() {

	if sqlText == "" {
		u.Errorf("You must provide a valid select query in argument:    --sql=\"select ...\"")
		return
	}

	// load all of our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", &EmailIsValid{})

	// We are registering the "csv" datasource, to show that
	// the backend/sources can be easily created/added.  This csv
	// reader is an example datasource that is very, very simple.
	exit := make(chan bool)
	src, _ := datasource.NewCsvSource("stdin", 0, bytes.NewReader([]byte("##")), exit)
	schema.RegisterSourceAsSchema("example_csv", src)

	db, err := sql.Open("qlbridge", "example_csv")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(sqlText)
	if err != nil {
		u.Errorf("could not execute query: %v", err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	fmt.Printf("\n\nScanning through CSV: (%v)\n\n", strings.Join(cols, ","))
	for rows.Next() {
		rows.Scan(readCols...)
		fmt.Println(strings.Join(writeCols, ", "))
	}
	fmt.Println("")
}

// Example of a custom Function, that we are adding into the Expression VM
//
//         select
//              user_id AS theuserid, email, item_count * 2, reg_date
//         FROM stdio
//         WHERE email_is_valid(email)
type EmailIsValid struct{}

func (m *EmailIsValid) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailIsValid(arg) but got %s", n)
	}
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		if args[0] == nil || args[0].Err() || args[0].Nil() {
			return value.BoolValueFalse, true
		}
		if _, err := mail.ParseAddress(args[0].ToString()); err == nil {
			return value.BoolValueTrue, true
		}

		return value.BoolValueFalse, true
	}, nil
}
func (m *EmailIsValid) Type() value.ValueType { return value.BoolType }
