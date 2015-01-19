

Example App: Reading Csv via Stdio, and evaluating in QL VM
------------------------------------------------------------------

This is an example app to read a CSV file, and ouput query results after processing
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
     WHERE email_is_valid(email)' < users.csv

````
TODO:
* Aggregates/collector's (GROUP BY, DISTINCT) and aggregate ops (COUNT, SUM, etc)
* ORDER BY
* LIMIT
* ~~support go/database/sql/driver~~
* ~~pluggable datasource~~

```go

func main() {

	// load all of our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// We are registering the "csv" datasource 
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

	for rows.Next() {
		rows.Scan(readCols...)
		fmt.Println(strings.Join(writeCols, ", "))
	}
}

// Example of a custom Function, that we are adding into the Expression VM
//
//         select
//              user_id AS theuserid, email, item_count * 2, reg_date
//         FROM stdio
//         WHERE email_is_valid(email)
func EmailIsValid(ctx vm.EvalContext, email value.Value) (value.BoolValue, bool) {
	emailstr, ok := value.ToString(email.Rv())
	if !ok || emailstr == "" {
		return value.BoolValueFalse, true
	}
	if _, err := mail.ParseAddress(emailstr); err == nil {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}


```


