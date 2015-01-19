/*
Package driver registers a QL Bridge sql/driver named "qlbridge"

See also [0], [1] and [2].

Usage

	package main

	import (
		"database/sql"
		_ "github.com/araddon/qlbridge/qlbdriver"
	)

	func main() {

		db, err := sql.Open("qlbridge", "csv:///dev/stdin")
		if err != nil {
			log.Fatal(err)
		}

		// Use db here

	}

This package exports nothing.

Links

Referenced from above:

  [0]: http://godoc.org/github.com/cznic/ql
  [1]: http://golang.org/pkg/database/sql/
  [2]: http://golang.org/pkg/database/sql/driver
*/
package qlbdriver

import "github.com/araddon/qlbridge/exec"

func init() {
	exec.RegisterSqlDriver()
}
