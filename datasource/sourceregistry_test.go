package datasource_test

import (
	"database/sql/driver"
	"flag"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/schema"
)

func init() {
	flag.Parse()
	if testing.Verbose() {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
}

func TestSourceRegistry(t *testing.T) {

	created, _ := dateparse.ParseAny("2015/07/04")

	inrow := []driver.Value{122, "bob", "bob@email.com", created.In(time.UTC).Add(time.Hour * -24), []string{"not_admin"}}

	db, err := memdb.NewMemDbData("memdb_users", [][]driver.Value{inrow}, []string{"user_id", "name", "email", "created", "roles"})
	assert.Equal(t, nil, err)

	c, err := db.Open("memdb_users")
	assert.Equal(t, nil, err)
	dc, ok := c.(schema.ConnAll)
	assert.True(t, ok)

	_, err = dc.Put(nil, &datasource.KeyInt{Id: 123}, []driver.Value{123, "aaron", "email@email.com", created.In(time.UTC), []string{"admin"}})
	assert.Equal(t, nil, err)

	// We need to register our DataSource provider here
	datasource.Register("memdb_reg_test", db)
}
