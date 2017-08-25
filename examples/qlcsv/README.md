

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
    FROM stdin' < users.csv

# SQL: where guard
./qlcsv -sql 'select 
    user_id AS theuserid, email, item_count * 2, reg_date FROM stdin 
  WHERE yy(reg_date) > 10' < users.csv

# SQL: add a custom function - email_is_valid
./qlcsv -sql 'select 
         user_id AS theuserid, email, item_count * 2, reg_date 
     FROM stdin 
     WHERE email_is_valid(email)' < users.csv

./qlcsv -sql 'select count(*) as user_ct FROM stdin' < users.csv

````


```go

func main() {

	if sqlText == "" {
		u.Errorf("You must provide a valid select query in argument:    --sql=\"select ...\"")
		return
	}

	// load all of our built-in functions
	builtins.LoadAllBuiltins()

	// Add a custom function to the VM to make available to SQL language
	expr.FuncAdd("email_is_valid", EmailIsValid)

	// Our file source of csv's is stdin
	stdIn, err := os.Open("/dev/stdin")
	if err != nil {
		u.Errorf("could not open stdin? %v", err)
		return
	}

	// We are registering the "csv" datasource, to show that
	// the backend/sources can be easily created/added.  This csv
	// reader is an example datasource that is very, very simple.
	exit := make(chan bool)
	src, _ := datasource.NewCsvSource("stdin", 0, stdIn, exit)
	datasource.Register("csv", src)

	db, err := sql.Open("qlbridge", "csv:///dev/stdin")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(sqlText)
	if err != nil {
		u.Errorf("could not execute query: %v", err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}
	fmt.Printf("\n\nScanning through CSV: (%v)\n\n", strings.Join(cols, ","))
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
	if _, err := mail.ParseAddress(email.ToString()); err == nil {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}


```


