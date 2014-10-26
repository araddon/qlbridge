package qlparse

import (
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"testing"
)

func TestParser(t *testing.T) {
	r, err := Parse(`SELECT name, age FROM user`)

	assert.Tf(t, err == nil, "nil err: %v", err)
	assert.Tf(t, r != nil, "Not nil: %v", r)
	sql, ok := r.(*SqlRequest)
	u.Info(sql.Columns)
	assert.Tf(t, ok, "ok: %v", ok)
	assert.Tf(t, sql != nil, "Not nil: %v", sql)
	assert.Tf(t, len(sql.Columns.Cols) == 2, "cols=2?: %v", len(sql.Columns.Cols))
	col1 := sql.Columns.Cols[0]
	assert.Tf(t, col1.As == "name", "name: %v", col1.As)
	col2 := sql.Columns.Cols[1]
	assert.Tf(t, col2.As == "age", "age: %v", col2.As)

	assert.Tf(t, sql.FromTable == "user", "expected table 'user' but was: %v", sql.FromTable)
	// r, err := Parse(`SELECT LOWER(Name) FROM Product`)
	// assert.Tf(t, err == nil, "nil err: %v", err)
	// assert.Tf(t, r != nil, "Not nil: %v", r)
}

func TestParserForEs(t *testing.T) {
	r, err := Parse(`SELECT author.name, author.age 
		FROM 'github/user' 
		WHERE repository.language = 'java';
	`)

	assert.Tf(t, err == nil, "nil err: %v", err)
	assert.Tf(t, r != nil, "Not nil: %v", r)
	sql, ok := r.(*SqlRequest)
	u.Info(sql.Columns)
	assert.Tf(t, ok, "ok: %v", ok)
	assert.Tf(t, sql != nil, "Not nil: %v", sql)
	assert.Tf(t, len(sql.Columns.Cols) == 2, "cols=2?: %v", len(sql.Columns.Cols))
	col1 := sql.Columns.Cols[0]
	assert.Tf(t, col1.As == "author.name", "author.name: %v", col1.As)
	col2 := sql.Columns.Cols[1]
	assert.Tf(t, col2.As == "author.age", "author.age: %v", col2.As)

	assert.Tf(t, sql.FromTable == "github/user", "expected table 'github/user' but was: %v", sql.FromTable)
	assert.T(t, len(sql.Where) == 1)
	//assert.T(t, sql.Where["repository.language"] == "java")
	// r, err := Parse(`SELECT LOWER(Name) FROM Product`)
	// assert.Tf(t, err == nil, "nil err: %v", err)
	// assert.Tf(t, r != nil, "Not nil: %v", r)
}
