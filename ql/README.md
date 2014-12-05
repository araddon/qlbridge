

Example Toy csv reader
--------------------------

This is an example app to read CSV from cli, and ouput after processing
through a xQL expression

```sh

go build 

# simple expression, single field
./ql -expr "user_id" < users.csv
./ql -expr "item_count * 2" < users.csv
./ql -expr "yy(reg_date)" < users.csv

# SQL:   select cols including expression
./ql -sql "select user_id, email, item_count * 2, yy(reg_date) > 10 FROM stdio" < users.csv

./ql -sql "select user_id AS theuserid, email, item_count * 2, yy(reg_date) > 10 FROM stdio" < users.csv

# SQL: where guard
./ql -sql "select user_id AS theuserid, email, item_count * 2, reg_date FROM stdio WHERE yy(reg_date) > 10" < users.csv

# SQL: add a custom function - email_is_valid
./ql -sql "select user_id AS theuserid, email, item_count * 2, reg_date FROM stdio WHERE email_is_valid(email)" < users.csv

````
TODO:
* Aggregator Context's (GROUP BY, DISTINCT)


### Example VM Runtime for Reading a Csv via Stdio, and evaluating


See example in [ql](https://github.com/araddon/qlparser/tree/master/ql)
folder for a CSV reader, parser, evaluation engine.


```go

func main() {

	flag.StringVar(&sqlText, "sql", "", "QL ish query multi-node such as [select user_id, yy(reg_date) from stdio];")
	flag.Parse()

	msgChan := make(chan url.Values, 100)
	quit := make(chan bool)
	go CsvProducer(msgChan, quit)

	go singleExprEvaluation(msgChan)

	<-quit
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
		fmt.Printall(printall(writeContext.All()))
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


```sh

# where guard
./ql -sql 'select sum(item_count) AS cts FROM stdio WHERE interests = "running"' < users.csv

./ql -sql "select sum(item_count) AS cts FROM stdio" < users.csv

./ql -sql "select count(user_id) AS ct_users FROM stdio GROUP BY user_id" < users.csv


````