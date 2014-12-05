package main

import (
	"encoding/csv"
	"io"
	"net/url"
	"os"
	"strings"
)

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
			}
			continue
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
