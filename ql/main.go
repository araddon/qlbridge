package main

import (
	"encoding/csv"
	"flag"
	"io"
	"net/url"
	"os"
	"strings"

	u "github.com/araddon/gou"
	vm "github.com/araddon/qlparser/vm"
)

var (
	EnforceRowLength bool = true
	rejected         int64
	query            string
	delimiter        = ","
)

func main() {
	u.SetupLogging("info")
	u.SetColorIfTerminal()
	flag.StringVar(&query, "q", "", "Query text")
	flag.StringVar(&delimiter, "delimiter", ",", "delimiter:   default = comma [t,|]")
	flag.Parse()

	exprVm, err := vm.NewVm(query)
	if err != nil {
		exprVm, err = vm.NewSqlVm(query)
		if err != nil {
			u.Errorf("Error: %v", err)
			return
		}
	}

	msgChan := make(chan url.Values, 100)
	quit := make(chan bool)
	go CsvProducer(msgChan, quit)
	go func() {
		for msg := range msgChan {
			results, err := exprVm.Execute(vm.NewContextUrlValues(msg))
			if err != nil {
				u.Errorf("error on execute: ", err)
			} else {
				u.Info(results.Value())
			}
		}
	}()
	<-quit
}

func CsvProducer(msgChan chan url.Values, quit chan bool) {
	defer func() {
		quit <- true
	}()
	csvr := csv.NewReader(os.Stdin)
	csvr.TrailingComma = true // allow empty fields
	if delimiter == "|" {
		csvr.Comma = '|'
	} else if delimiter == "\t" || delimiter == "t" {
		csvr.Comma = '\t'
	}
	headers, err := csvr.Read()
	if err != nil {
		panic(err.Error())
	}
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				return
			} else if EnforceRowLength && strings.Contains(err.Error(), "wrong number of fields in line") {
				rejected++
				continue
			} else {
				if EnforceRowLength {
					u.Warnf("err: %v   %v", err, row)
					return
				}
			}
		}

		v := make(url.Values)

		// If values exist for desired indexes, set them.
		for idx, fieldName := range headers {
			if idx <= len(row)-1 {
				v.Set(fieldName, strings.TrimSpace(row[idx]))
			}
		}

		msgChan <- v

	}
}
