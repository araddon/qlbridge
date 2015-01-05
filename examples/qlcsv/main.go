package main

import (
	"flag"
	"fmt"
	"net/mail"
	"net/url"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/ast"
	"github.com/araddon/qlbridge/builtins"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	exprText         string
	sqlText          string
	flagCsvDelimiter = ","
	logging          = "info"
)

func init() {

	flag.StringVar(&logging, "logging", "info", "logging [ debug,info ]")
	flag.StringVar(&exprText, "expr", "", "Single Expression Statement [ 4 * toint(item_count) ]")
	flag.StringVar(&sqlText, "sql", "", "QL ish query multi-node such as [select user_id, yy(reg_date) from stdio];")
	flag.StringVar(&flagCsvDelimiter, "delimiter", ",", "delimiter:   default = comma [t,|]")
	flag.Parse()

	u.SetupLogging(logging)
	//u.SetColorIfTerminal()
	u.SetColorOutput()
	builtins.LoadAllBuiltins()
}

func main() {

	//quit := make(chan bool)
	msgChan := make(chan url.Values, 100)

	// Add a custom function to the VM to make available to SQL language
	ast.FuncAdd("email_is_valid", EmailIsValid)

	// We have two different Expression Engines to demo here, called by
	// using one of two different Flag's
	//   --sql="select ...."
	//   --expr="item + 4"
	switch {
	case sqlText != "":
		sqlEvaluation(msgChan)
		// case exprText != "":
		// 	singleExprEvaluation(msgChan)
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

// Write context for vm engine to store data
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
func (m OurContext) Put(col ast.SchemaInfo, rctx vm.ContextReader, v value.Value) error {
	m.data[col.Key()] = v
	return nil
}

// This is the evaluation engine for SQL
func sqlEvaluation(msgChan chan url.Values) {

	//quit := make(<-chan bool)
	csvIn, err := datasource.Open("csv", "/dev/stdin")

	exprVm, err := vm.NewSqlVm(sqlText)
	if err != nil {
		u.Errorf("Error: %v", err)
		return
	}
	iter := csvIn.CreateIterator(nil)
	for msg := iter.Next(); msg != nil; msg = iter.Next() {
		uv := msg.Body().(url.Values)
		readContext := vm.NewContextUrlValues(uv)
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

func printall(all map[string]value.Value) string {
	allStr := make([]string, 0)
	for name, val := range all {
		allStr = append(allStr, fmt.Sprintf("%s:%v", name, val.Value()))
	}
	return strings.Join(allStr, ", ")
}

// Simple simple expression
func singleExprEvaluation(msgChan chan url.Values) {

	// go ahead and use built in context
	writeContext := vm.NewContextSimple()

	exprVm, err := vm.NewVm(exprText)
	if err != nil {
		u.Errorf("Error: %v", err)
		return
	}
	for msg := range msgChan {
		readContext := vm.NewContextUrlValues(msg)
		err := exprVm.Execute(writeContext, readContext)
		if err != nil {
			u.Errorf("error on execute: ", err)
		} else {
			val, _ := writeContext.Get("")
			u.Info(val.Value())
		}
	}
}
