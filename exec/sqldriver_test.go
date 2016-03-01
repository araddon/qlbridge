package exec

import (
	"database/sql"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/datasource"
)

func init() {

	RegisterSqlDriver()

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
	RegDate   datasource.TimeValue
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
		u.Debugf("user=%+v", ur)
		users = append(users, ur)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(users) == 1, "has 1 user row: %+v", users)

	u1 := users[0]
	assert.T(t, u1.Email == "aaron@email.com")
	assert.T(t, u1.RegYearMonth == 1210)
	assert.T(t, u1.Id == "9Ip1aKbeZe2njCDM")
}

func TestSqlCsvDriverJoinSimple(t *testing.T) {

	// No sort, or where, full scans

	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
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
	assert.Tf(t, len(cols) == 6, "6 cols: %v", cols)
	userOrders := make([]userorder, 0)
	for rows.Next() {
		var uo userorder
		err = rows.Scan(&uo.UserId, &uo.ItemId, &uo.RegDate, &uo.Email, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 := userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)
}

func TestSqlCsvDriverJoinWithWhere1(t *testing.T) {

	// Where Statement on join on column (o.item_count) that isn't in query
	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id
		WHERE o.item_count > 10;
	`
	db, err := sql.Open("qlbridge", "mockcsv")
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, db != nil, "has conn: %v", db)

	defer func() {
		u.Debugf("db.Close()")
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
	assert.Tf(t, len(cols) == 6, "6 cols: %v", cols)
	userOrders := make([]userorder, 0)
	for rows.Next() {
		var uo userorder
		err = rows.Scan(&uo.UserId, &uo.ItemId, &uo.RegDate, &uo.Email, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 := userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)
}

func TestSqlCsvDriverJoinWithWhere2(t *testing.T) {

	// Where Statement on join on column (o.item_count) that isn't in query
	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id
		WHERE o.price > 10;
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
	assert.Tf(t, len(cols) == 6, "6 cols: %v", cols)
	userOrders := make([]userorder, 0)
	for rows.Next() {
		var uo userorder
		err = rows.Scan(&uo.UserId, &uo.ItemId, &uo.RegDate, &uo.Email, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 := userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)
}

func TestSqlCsvDriverSubQuery(t *testing.T) {
	// Sub-Query
	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN (
				SELECT price, order_date, user_id from ORDERS
				WHERE user_id IS NOT NULL AND price > 10
			) AS o 
			ON u.user_id = o.user_id
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
	assert.Tf(t, rows != nil, "has results: %v", rows)

	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 6, "6 cols: %v", cols)
	userOrders := make([]userorder, 0)
	for rows.Next() {
		var uo userorder
		err = rows.Scan(&uo.UserId, &uo.ItemId, &uo.RegDate, &uo.Email, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 := userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)

	rows.Close()

	// Really want to test the order_date date conversion detection

	// sqlText := `
	// 	SELECT
	// 		u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
	// 	FROM users AS u
	// 	INNER JOIN (
	// 			SELECT price, order_date, user_id from ORDERS
	// 			WHERE user_id IS NOT NULL AND order_date > "2014/01/01"
	// 		) AS o
	// 		ON u.user_id = o.user_id
	// 	WHERE o.price > 10;
	// `

	// 	s = `SELECT  aa.*,
	// 			        bb.meal
	// 			FROM table1 aa
	// 				INNER JOIN table2 bb
	// 				    ON aa.tableseat = bb.tableseat AND
	// 				        aa.weddingtable = bb.weddingtable
	// 				INNER JOIN
	// 				(
	// 					SELECT  a.tableSeat
	// 					FROM    table1 a
	// 					        INNER JOIN table2 b
	// 					            ON a.tableseat = b.tableseat AND
	// 					                a.weddingtable = b.weddingtable
	// 					WHERE b.meal IN ('chicken', 'steak')
	// 					GROUP by a.tableSeat
	// 					HAVING COUNT(DISTINCT b.Meal) = 2
	// 				) c ON aa.tableseat = c.tableSeat
	// `
}

func TestSqlDbConnFailure(t *testing.T) {
	// Where Statement on join on column (o.item_count) that isn't in query
	sqlText := `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id
		WHERE o.price > 10;
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
	assert.Tf(t, rows != nil, "has results: %v", rows)

	cols, err := rows.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 6, "6 cols: %v", cols)
	userOrders := make([]userorder, 0)
	for rows.Next() {
		var uo userorder
		err = rows.Scan(&uo.UserId, &uo.ItemId, &uo.RegDate, &uo.Email, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 := userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)

	rows.Close()
	u.Debug("end 1\n\n\n")
	//return

	// Return same query, was failing for some reason?
	sqlText = `
		SELECT 
			u.user_id, o.item_id, u.reg_date, u.email, o.price, o.order_date
		FROM users AS u 
		INNER JOIN orders AS o 
			ON u.user_id = o.user_id
		WHERE o.price > 10;
	`

	rows2, err := db.Query(sqlText)
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, rows2 != nil, "has results: %v", rows2)

	cols, err = rows2.Columns()
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, len(cols) == 6, "6 cols: %v", cols)
	userOrders = make([]userorder, 0)
	for rows2.Next() {
		var uo userorder
		err = rows2.Scan(&uo.UserId, &uo.ItemId, &uo.RegDate, &uo.Email, &uo.Price, &uo.OrderDate)
		assert.Tf(t, err == nil, "no error: %v", err)
		//u.Debugf("userorder=%+v", uo)
		userOrders = append(userOrders, uo)
	}
	assert.Tf(t, rows2.Err() == nil, "no error: %v", err)
	assert.Tf(t, len(userOrders) == 2, "want 2 userOrders row: %+v", userOrders)

	uo1 = userOrders[0]
	assert.Tf(t, uo1.Email == "aaron@email.com", "%#v", uo1)
	assert.Tf(t, uo1.Price == 22.5, "? %#v", uo1)
	rows2.Close()
}
