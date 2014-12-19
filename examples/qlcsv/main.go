package main

import (
	"flag"
	"fmt"
	"net/mail"
	"net/url"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/builtins"
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

	msgChan := make(chan url.Values, 100)
	quit := make(chan bool)
	go CsvProducer(msgChan, quit)

	// Add a custom function to the VM to make available to SQL language
	vm.FuncAdd("email_is_valid", EmailIsValid)

	// We have two different Expression Engines to demo here, called by
	// using one of two different Flag's
	//   --sql="select ...."
	//   --expr="item + 4"
	switch {
	case sqlText != "":
		go sqlEvaluation(msgChan)
	case exprText != "":
		go singleExprEvaluation(msgChan)
	}

	<-quit
}

// Example of a custom Function, that we are adding into the Expression VM
//
//         select
//              user_id AS theuserid, email, item_count * 2, reg_date
//         FROM stdio
//         WHERE email_is_valid(email)
func EmailIsValid(e *vm.State, email vm.Value) (vm.BoolValue, bool) {
	emailstr, ok := vm.ToString(email.Rv())
	if !ok || emailstr == "" {
		return vm.BoolValueFalse, true
	}
	if _, err := mail.ParseAddress(emailstr); err == nil {
		return vm.BoolValueTrue, true
	}

	return vm.BoolValueFalse, true
}

// Write context for vm engine to store data
type OurContext struct {
	data map[string]vm.Value
}

func NewContext() OurContext {
	return OurContext{data: make(map[string]vm.Value)}
}

func (m OurContext) All() map[string]vm.Value {
	return m.data
}

func (m OurContext) Get(key string) (vm.Value, bool) {
	return m.data[key], true
}

func (m OurContext) Put(col vm.SchemaInfo, rctx vm.ContextReader, v vm.Value) error {
	m.data[col.Key()] = v
	return nil
}

// This is the evaluation engine for SQL
func sqlEvaluation(msgChan chan url.Values) {

	exprVm, err := vm.NewSqlVm(sqlText)
	if err != nil {
		u.Errorf("Error: %v", err)
		return
	}
	for msg := range msgChan {
		readContext := vm.NewContextUrlValues(msg)
		// use our custom write context for example purposes
		writeContext := NewContext()
		err := exprVm.Execute(writeContext, readContext)
		if err != nil && err == vm.SqlEvalError {
			u.Errorf("error on execute: ", err)
		} else if len(writeContext.All()) > 0 {
			u.Info(printall(writeContext.All()))
		} else {
			u.Debugf("Filtered out row:  %v", msg)
		}
	}
}

func printall(all map[string]vm.Value) string {
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
