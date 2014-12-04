

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

# select cols including expression
./ql -sql "select user_id, email, item_count * 2, yy(reg_date) > 10 FROM stdio" < users.csv

./ql -sql "select user_id AS theuserid, email, item_count * 2, yy(reg_date) > 10 FROM stdio" < users.csv

# where guard
./ql -sql 'select sum(item_count) AS cts FROM stdio WHERE interests = "running"' < users.csv

./ql -sql "select sum(item_count) AS cts FROM stdio" < users.csv

./ql -sql "select count(user_id) AS ct_users FROM stdio GROUP BY user_id" < users.csv

````
