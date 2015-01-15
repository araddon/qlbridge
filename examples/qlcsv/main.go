package main

import (
	"flag"
	"fmt"
	"net/mail"
	"net/url"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/builtins"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
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

	/*
		TODO:
			- allow a custom-context, load in somehow
			- db driver

	*/

	// load our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	datasource.Register("csv", &datasource.CsvDataSource{})

	// parse our sql statement
	exprVm, err := vm.NewSqlVm(sqlText)
	if err != nil {
		u.Errorf("Error: %v", err)
		return
	}

	// Create a csv data source from stdin
	csvIn, err := datasource.Open("csv", "/dev/stdin")
	if err != nil {
		u.Errorf("Error: %v", err)
		return
	}
	iter := csvIn.CreateIterator(nil)

	for msg := iter.Next(); msg != nil; msg = iter.Next() {
		uv := msg.Body().(url.Values)
		readContext := datasource.NewContextUrlValues(uv)
		// use our custom write context for example purposes
		writeContext := NewContext()
		err := exprVm.Execute(writeContext, readContext)
		if err != nil && err == vm.SqlEvalError {
			u.Errorf("error on execute: ", err)
		} else if len(writeContext.All()) > 0 {
			u.Info(printall(writeContext.All()))
		} else {
			u.Debugf("Filtered out row:  %v", uv)
		}
	}

}

// Example of a custom Function, that we are adding into the Expression VM
//
//         select
//              user_id AS theuserid, email, item_count * 2, reg_date
//         FROM stdio
//         WHERE email_is_valid(email)
func EmailIsValid(e *vm.State, email value.Value) (value.BoolValue, bool) {
	emailstr, ok := value.ToString(email.Rv())
	if !ok || emailstr == "" {
		return value.BoolValueFalse, true
	}
	if _, err := mail.ParseAddress(emailstr); err == nil {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}

// Write context for vm engine to write data
// somewhat the equivalent of a "recordset"
type OurContext struct {
	data map[string]value.Value
}

func NewContext() OurContext {
	return OurContext{data: make(map[string]value.Value)}
}

func (m OurContext) All() map[string]value.Value {
	return m.data
}

func (m OurContext) Get(key string) (value.Value, bool) {
	return m.data[key], true
}
func (m OurContext) Delete(row map[string]value.Value) error {
	return fmt.Errorf("not implemented")
}
func (m OurContext) Put(col expr.SchemaInfo, rctx datasource.ContextReader, v value.Value) error {
	m.data[col.Key()] = v
	return nil
}

func printall(all map[string]value.Value) string {
	allStr := make([]string, 0)
	for name, val := range all {
		allStr = append(allStr, fmt.Sprintf("%s:%v", name, val.Value()))
	}
	return strings.Join(allStr, ", ")
}
