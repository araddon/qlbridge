package main

import (
	"flag"
	"fmt"
	"net/mail"
	"strings"

	"database/sql"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	_ "github.com/araddon/qlbridge/qlbdriver"
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
	//u.SetColorIfTerminal()
	u.SetColorOutput()
}

func main() {

	// load all of our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// We are registering the "csv" datasource, to show that
	// the backend/sources can be easily created/added
	datasource.Register("csv", &datasource.CsvDataSource{})

	db, err := sql.Open("qlbridge", "csv:///dev/stdin")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(sqlText)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}
	fmt.Println("\n\nScanning through CSV:  \n")
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
func EmailIsValid(ctx expr.EvalContext, email value.Value) (value.BoolValue, bool) {
	emailstr, ok := value.ToString(email.Rv())
	if !ok || emailstr == "" {
		return value.BoolValueFalse, true
	}
	if _, err := mail.ParseAddress(emailstr); err == nil {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}
