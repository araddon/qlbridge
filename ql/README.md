

Example Toy csv reader
--------------------------

This is an example app to read CSV from cli, and ouput after processing
through a xQL expression

```sh

go build 


./ql -q "user_id" < users.csv

# select cols including expression
./ql -q "select user_id, email, item_count, yy() > 10 FROM stdio" < users.csv

# where guard
./ql -q 'select sum(item_count) AS cts FROM stdio WHERE interests = "running"' < users.csv

./ql -q "select sum(item_count) AS cts FROM stdio" < users.csv

./ql -q "select count(user_id) AS ct_users FROM stdio GROUP BY user_id" < users.csv

````
