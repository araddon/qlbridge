package main

import (
	"flag"
	"fmt"
	"net/url"
	"strings"

	u "github.com/araddon/gou"
	vm "github.com/araddon/qlparser/vm"
)

var (
	exprText  string
	sqlText   string
	delimiter = ","
	logging   = "info"
)

func main() {

	flag.StringVar(&logging, "logging", "info", "logging [ debug,info ]")
	flag.StringVar(&exprText, "expr", "", "Single Expression Statement [ 4 * toint(item_count) ]")
	flag.StringVar(&sqlText, "sql", "", "QL ish query multi-node such as [select user_id, yy(reg_date) from stdio];")
	flag.StringVar(&delimiter, "delimiter", ",", "delimiter:   default = comma [t,|]")
	flag.Parse()

	u.SetupLogging(logging)
	//u.SetColorIfTerminal()
	u.SetColorOutput()

	msgChan := make(chan url.Values, 100)
	quit := make(chan bool)
	go CsvProducer(msgChan, quit)

	switch {
	case sqlText != "":
		go sqlEvaluation(msgChan)
	case exprText != "":
		go singleExprEvaluation(msgChan)
	}

	<-quit
}

func singleExprEvaluation(msgChan chan url.Values) {

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
			u.Info(writeContext.Get("").Value())
		}
	}
}
func sqlEvaluation(msgChan chan url.Values) {

	writeContext := vm.NewContextSimple()

	exprVm, err := vm.NewSqlVm(sqlText)
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
			u.Info(printall(writeContext.All()))
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
