package exec

import (
	"database/sql"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
	//"github.com/araddon/qlbridge/datasource/mockcsv"
)

func init() {

	RegisterSqlDriver()

	LoadTestDataOnce()

	rtConf.DisableRecover = true
	_ = u.EMPTY
}

type user struct {
	Id           string
	Email        string
	interests    []string
	RegDate      time.Time
	ItemCount    int
	RegYearMonth int
}

type userorder struct {
	UserId    string
	Email     string
	ItemId    string
	Price     float64
	OrderDate datasource.TimeValue
}

func TestSqlCsvDriverSimple(t *testing.T) {

	sqlText := `
		select 
	        user_id, email, referral_count * 2, yymm(reg_date)
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
		err = rows.Scan(&ur.Id, &ur.Email, &ur.ItemCount, &ur.RegYearMonth)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("user=%+v", ur)
		users = append(users, ur)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(users) == 1, "has 1 user row: %+v", users)

	u1 := users[0]
	assert.T(t, u1.Email == "aaron@email.com")
	assert.T(t, u1.RegYearMonth == 1210)
	assert.T(t, u1.Id == "9Ip1aKbeZe2njCDM")
}

func TestSqlCsvDriverJoin(t *testing.T) {
	//  - No sort (overall), or where, full scans
	sqlText := `
		SELECT 
			u.user_id, u.email, o.item_id, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id;
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
	assert.Tf(t, len(cols) == 5, "5 cols: %v", cols)
	userOrders := make([]userorder, 0)
	for rows.Next() {
		var uo userorder
		err = rows.Scan(&uo.UserId, &uo.Email, &uo.ItemId, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 := userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)

	/*
	   - Where Statement (rewrite query)


	*/
}
