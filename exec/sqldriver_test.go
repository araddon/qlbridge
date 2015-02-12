package exec

import (
	"testing"
	"time"

	"database/sql"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource/mockcsv"
	"github.com/bmizerany/assert"
)

func init() {

	RegisterSqlDriver()

	// Load in a "csv file" into our mock data store
	mockcsv.MockData["users"] = `user_id,email,interests,reg_date,referral_count
9Ip1aKbeZe2njCDM,"aaron@email.com","fishing","2012-10-17T17:29:39.738Z",22
hT2impsOPUREcVPc,"bob@email.com","swimming","2009-12-11T19:53:31.547Z",12
hT2impsabc345c,"not_an_email","swimming","2009-12-11T19:53:31.547Z",12`

	mockcsv.MockData["orders"] = `user_id,item_id,price,order_date,item_count
9Ip1aKbeZe2njCDM,1,22.50,"2012-10-24T17:29:39.738Z",82
9Ip1aKbeZe2njCDM,1,22.50,"2012-10-24T17:29:39.738Z",82
`

}

type user struct {
	Id         string
	Email      string
	interests  []string
	RegDate    time.Time
	ItemCount  int
	RegAfter10 bool
}

func TestSqlCsvDriver1(t *testing.T) {

	sqlText := `
		select 
	        user_id, email, referral_count * 2, yy(reg_date) > 10 AS reg_date_gt10
	    FROM users
	    WHERE 
	    	yy(reg_date) > ? 
    `
	db, err := sql.Open("qlbridge", "mockcsv")
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, db != nil, "has conn: %v", db)

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	rows, err := db.Query(sqlText, 10)
	assert.Tf(t, err == nil, "no error: %v", err)
	defer rows.Close()
	assert.Tf(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 4, "4 cols: %v", cols)
	users := make([]user, 0)
	for rows.Next() {
		var ur user
		err = rows.Scan(&ur.Id, &ur.Email, &ur.ItemCount, &ur.RegAfter10)
		assert.Tf(t, err == nil, "no error: %v", err)
		u.Debugf("user=%+v", ur)
		users = append(users, ur)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(users) == 1, "has 1 user row: %+v", users)

	u1 := users[0]
	assert.T(t, u1.Email == "aaron@email.com")
	assert.Tf(t, u1.RegAfter10 == true, "true")
}

func TestSqlCsvDriverJoin(t *testing.T) {
	// not yet implemented
	return
	sqlText := `
		SELECT 
			t1.name, t2.salary
		FROM employee AS t1 
		INNER JOIN info AS t2 
		ON t1.name = t2.name;
    `
	db, err := sql.Open("qlbridge", "mockcsv")
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, db != nil, "has conn: %v", db)

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	rows, err := db.Query(sqlText)
	assert.Tf(t, err == nil, "no error: %v", err)
	defer rows.Close()
	assert.Tf(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 4, "4 cols: %v", cols)
	users := make([]user, 0)
	for rows.Next() {
		var ur user
		err = rows.Scan(&ur.Id, &ur.Email, &ur.ItemCount, &ur.RegAfter10)
		assert.Tf(t, err == nil, "no error: %v", err)
		u.Debugf("user=%+v", ur)
		users = append(users, ur)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(users) == 1, "has 1 user row: %+v", users)

	u1 := users[0]
	assert.T(t, u1.Email == "aaron@email.com")
	assert.Tf(t, u1.RegAfter10 == true, "true")
}
