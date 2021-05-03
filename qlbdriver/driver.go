/*
Package driver registers a QL Bridge sql/driver named "qlbridge"

Usage

	package main

	import (
		"database/sql"
		_ "github.com/lytics/qlbridge/qlbdriver"
	)

	func main() {

		db, err := sql.Open("qlbridge", "csv:///dev/stdin")
		if err != nil {
			log.Fatal(err)
		}

		// Use db here

	}

*/
package qlbdriver

import "github.com/lytics/qlbridge/exec"

func init() {
	exec.RegisterSqlDriver()
}
