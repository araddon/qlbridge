package datasource_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/testutil"
	"github.com/araddon/qlbridge/value"
)

func TestMain(m *testing.M) {
	testutil.Setup() // will call flag.Parse()

	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()

	// Now run the actual Tests
	os.Exit(m.Run())
}

func TestIntrospectedCsvSchema(t *testing.T) {
	sch := td.MockSchema

	tableName := "users"
	csvSrc, err := sch.OpenConn(tableName)
	assert.Equal(t, nil, err)
	scanner, ok := csvSrc.(schema.ConnScanner)
	assert.True(t, ok)

	err = datasource.IntrospectSchema(sch, tableName, scanner)
	assert.Equal(t, nil, err)
	tbl, err := sch.Table("users")
	assert.Equal(t, nil, err)
	assert.Equal(t, "users", tbl.Name)
	assert.Equal(t, 6, len(tbl.Fields))

	refCt := tbl.FieldMap["referral_count"]
	assert.Equal(t, int(value.IntType), int(refCt.Type), "wanted int got %s", refCt.Type)

	userId := tbl.FieldMap["user_id"]
	assert.Equal(t, int(value.StringType), int(userId.Type), "wanted string got %s", userId.Type)

	jd := tbl.FieldMap["json_data"]
	assert.Equal(t, int(value.JsonType), int(jd.Type), "wanted json got %s", jd.Type)
}
