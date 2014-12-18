

Example App: Reading Csv via Stdio, and evaluating in QL VM
------------------------------------------------------------------

This is an example app to read CSV from cli, and ouput after processing
through a xQL expression evaluation VM which has a custom Func
supplied into the VM eval engine  `email_is_valid`

```sh

go build 

# SQL:   select cols including expression
./qlcsv -sql '
    select 
        user_id, email, item_count * 2, yy(reg_date) > 10 
    FROM stdio' < users.csv

# SQL: where guard
./qlcsv -sql 'select 
    user_id AS theuserid, email, item_count * 2, reg_date FROM stdio 
  WHERE yy(reg_date) > 10' < users.csv

# SQL: add a custom function - email_is_valid
./qlcsv -sql 'select 
         user_id AS theuserid, email, item_count * 2, reg_date 
     FROM stdio 
     WHERE email_is_valid(email)" < users.csv

````
TODO:
* Aggregator Context's (GROUP BY, DISTINCT) and aggregate ops (COUNT, SUM, etc)
* ORDER BY
* LIMIT
* support go/database/sql/driver in

```go

func main() {

	flag.StringVar(&sqlText, "sql", "", "QL ish query multi-node such as [select user_id, yy(reg_date) from stdio];")
	flag.Parse()

	// Add a custom function to the VM to make available to SQL language
	vm.FuncAdd("email_is_valid", EmailIsValid)

	msgChan := make(chan url.Values, 100)
	quit := make(chan bool)
	go CsvProducer(msgChan, quit)

	go sqlEvaluation(msgChan)

	<-quit
}

func EmailIsValid(e *vm.State, email vm.Value) (vm.BoolValue, bool) {
	emailstr, _ := vm.ToString(email.Rv())
	if _, err := mail.ParseAddress(emailstr); err == nil {
		return vm.BoolValueTrue, true
	}

	return vm.BoolValueFalse, true
}


func sqlEvaluation(msgChan chan url.Values) {

	// A write context holds the results of evaluation
	// could be a storage layer, in this case a simple map
	writeContext := vm.NewContextSimple()

	// our parsed sql, and runtime to evaluate sql
	exprVm, err := vm.NewSqlVm(sqlText)
	if err != nil {
		return
	}
	for msg := range msgChan {
		readContext := vm.NewContextUrlValues(msg)
		err := exprVm.Execute(writeContext, readContext)
		if err != nil {
			fmt.Println(err)
			return
		} 
		fmt.Println(printall(writeContext.All()))
	}
}

func CsvProducer(msgChan chan url.Values, quit chan bool) {
	defer func() {
		quit <- true
	}()
	csvr := csv.NewReader(os.Stdin)
	headers, _ := csvr.Read()
	for {
		row, err := csvr.Read()
		if err != nil && err == io.EOF {
			return
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


```


