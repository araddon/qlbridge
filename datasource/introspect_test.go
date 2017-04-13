package datasource_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

func init() {
	testutil.Setup()
	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()
}

func TestIntrospectedCsvSchema(t *testing.T) {
	sch := td.MockSchema

	tableName := "users"
	csvSrc, err := sch.Open(tableName)
	assert.True(t, err == nil, "should not have error: %v", err)
	scanner, ok := csvSrc.(schema.ConnScanner)
	assert.True(t, ok)

	err = datasource.IntrospectSchema(sch, tableName, scanner)
	assert.True(t, err == nil, "should not have error: %v", err)
	tbl, err := sch.Table("users")
	assert.True(t, err == nil, "should not have error: %v", err)
	assert.True(t, tbl.Name == "users", "wanted users got %s", tbl.Name)
	assert.True(t, len(tbl.Fields) == 5, "want 5 cols got %v", len(tbl.Fields))

	refCt := tbl.FieldMap["referral_count"]
	assert.True(t, refCt.Type == value.IntType, "wanted int got %s", refCt.Type)

	userId := tbl.FieldMap["user_id"]
	assert.True(t, userId.Type == value.StringType, "wanted string got %s", userId.Type)
}
